use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use anyhow::{Result, Error};
use serde::{Serialize};
use tokio::time;
use tokio::time::Instant;
use warp::Filter;

pub struct StatsServer {
  port: u16,
  stats: Arc<RwLock<HashMap<String, (CommittedStats, CurrentlyCollectingStats)>>>
}

pub enum StatsUpdate {
  AddBytesReceived(usize),
  UpdateRTT(u32),
  AddDroppedPackets(u32)
}

#[derive(Debug)]
struct CommittedStats {
  bitrate: u64,
  rtt: u32,
  dropped_pkts: u64,
  last_update: Instant
}

#[derive(Debug)]
struct CurrentlyCollectingStats {
  bytes_received: AtomicU64,
  rtt_rolling_avg: AtomicU32,
  dropped_pkts: AtomicU64
}

#[derive(Serialize)]
struct StatsReply {
  push: PushStatsReply
}
#[derive(Serialize)]
struct PushStatsReply {
  status: String,
  bitrate: u64,
  rtt: u32,
  dropped_pkts: u64
}

impl StatsServer {
  pub async fn new(port: u16) -> Self {
    StatsServer{port, stats: Arc::new(RwLock::new(HashMap::new()))}
  }

  pub fn push_stats(&self, srt_stream_id: Option<String>, stats_update: StatsUpdate) {
    let stream_id = srt_stream_id.unwrap_or(String::new());
    if let Some((_, current)) = self.stats.read().expect("stats lock should not be poisoned").get(&stream_id) {
      match stats_update {
        StatsUpdate::AddBytesReceived(num_bytes) => {
          let _ = current.bytes_received.fetch_add(num_bytes as u64, Ordering::Release);
        }
        StatsUpdate::UpdateRTT(rtt) => {
          let _ = current.rtt_rolling_avg.fetch_update(Ordering::Acquire, Ordering::Acquire, |old| Some((old as f32 * 0.95 + rtt as f32 * 0.05).round() as u32));
        }
        StatsUpdate::AddDroppedPackets(num_dropped) => {
          let _ = current.dropped_pkts.fetch_add(u64::from(num_dropped), Ordering::Release);
        }
      }
      return;
    }

    let new_stats = (CommittedStats{
      bitrate: 0,
      rtt: 0,
      dropped_pkts: 0,
      last_update: Instant::now(),
    }, match stats_update {
      StatsUpdate::AddBytesReceived(num_bytes) => {
        CurrentlyCollectingStats{
          bytes_received: AtomicU64::new(num_bytes as u64),
          rtt_rolling_avg: AtomicU32::new(0),
          dropped_pkts: AtomicU64::new(0),
        }
      }
      StatsUpdate::UpdateRTT(rtt) => {
        CurrentlyCollectingStats{
          bytes_received: AtomicU64::new(0),
          rtt_rolling_avg: AtomicU32::new(rtt),
          dropped_pkts: AtomicU64::new(0),
        }
      }
      StatsUpdate::AddDroppedPackets(num_dropped_pkts) => {
        CurrentlyCollectingStats{
          bytes_received: AtomicU64::new(0),
          rtt_rolling_avg: AtomicU32::new(0),
          dropped_pkts: AtomicU64::new(u64::from(num_dropped_pkts)),
        }
      }
    });
    self.stats.write().expect("stats lock should not be poisoned").insert(stream_id, new_stats);
  }

  pub async fn listen(&self) -> Result<(), Error> {
    self.start_timer_thread();

    let status = warp::path::end().map(|| r#"{"status":"ok"}"#);


    let stats_single = {
      let stats = self.stats.clone();
      warp::path!("stats").map(move || {
        println!("{:?}", stats.read().expect(""));
        if stats.read().expect("stats lock should not be poisoned").len() > 1 {
          r#"{"status":"error", "message": "more than 1 group registered, please use /stats/[srt_stream_id]"}"#.to_string()
        } else if let Some(st) = stats.read().expect("stats lock should not be poisoned").values().next() {
          let reply = StatsReply{
            push: PushStatsReply {
              status: "connected".to_string(),
              bitrate: st.0.bitrate,
              rtt: st.0.rtt,
              dropped_pkts: st.0.dropped_pkts,
            },
          };
          serde_json::to_string(&reply).unwrap_or(r#"{"status":"error", "message": "error creating response object"}"#.to_string())
        } else {
          r#"{"status":"error", "message": "no group connected"}"#.to_string()
        }
      })
    };

    let stats_by_id = {
      let stats = self.stats.clone();
      warp::path!("stats" / String).map(move |sid| {
        if let Some(st) = stats.read().expect("stats lock should not be poisoned").get(&sid) {
          let reply = StatsReply{
            push: PushStatsReply {
              status: "connected".to_string(),
              bitrate: st.0.bitrate,
              rtt: st.0.rtt,
              dropped_pkts: st.0.dropped_pkts,
            },
          };
          serde_json::to_string(&reply).unwrap_or(r#"{"status":"error", "message": "error creating response object"}"#.to_string())
        } else {
          r#"{"status":"error", "message": "group not connected"}"#.to_string()
        }
      })
    };

    let handlers = status.or(stats_single).or(stats_by_id);
    warp::serve(handlers)
      .run(([0, 0, 0, 0], self.port))
      .await;

    Ok(())
  }

  fn start_timer_thread(&self) {
    let stats = self.stats.clone();
    tokio::spawn(async move {
      let mut interval = time::interval(Duration::from_secs(1));

      loop {
        interval.tick().await;

        let mut to_remove = Vec::new();
        let mut stats_write = stats.write().expect("stats lock should not be poisoned");

        for (stream_id, (committed, collecting)) in stats_write.iter_mut() {
          committed.bitrate = collecting.bytes_received.swap(0, Ordering::Acquire)/125;
          committed.rtt = collecting.rtt_rolling_avg.load(Ordering::Acquire);
          committed.dropped_pkts = collecting.dropped_pkts.load(Ordering::Acquire);
          if committed.bitrate > 0 {
            committed.last_update = Instant::now()
          }
          if committed.last_update < Instant::now() - Duration::from_secs(5) {
            to_remove.push(stream_id.clone())
          }
        }

        for stream_id in to_remove {
          stats_write.remove(&stream_id);
        }
      }
    });
  }
}