use std::mem;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, atomic, RwLock};
use std::sync::atomic::AtomicU32;
use std::time::Duration;
use tokio::net::UdpSocket;
use anyhow::{Error, Result};
use crate::net::{packets, srt};
use crate::net::packets::{SRTLA_ID_LEN, SrtlaPacketType, SrtlaParserMode};
use crate::net;
use log::{info, warn};
use rand::RngCore;
use tokio::time;
use tokio::time::Instant;
use crate::net::srt::SrtConnection;

const SRTLA_RECV_ACK_AMOUNT: usize = 10;

pub struct SrtlaReceiver {
  socket: Arc<UdpSocket>,
  connections: Arc<RwLock<HashMap<SocketAddr, [u8; SRTLA_ID_LEN]>>>,
  groups: Arc<RwLock<HashMap<[u8; SRTLA_ID_LEN], SrtlaGroup>>>,
  srt_connection: SrtConnection,
}

#[derive(Debug)]
pub struct SrtlaGroup {
  connections: Vec<SrtlaConnection>,
  last_address: SocketAddr,
  srt_socket: Option<Arc<UdpSocket>>,
  bytes_received: AtomicU32,
  rtt: AtomicU32,
  estimated_link_capacity: AtomicU32
}

#[derive(Debug, Clone)]
pub struct SrtlaConnection {
  address: SocketAddr,
  last_received: Instant,
  received_srt_packets: Vec<u32>
}


impl SrtlaReceiver {
  pub async fn new(port: u16, srt_connection: SrtConnection) -> Result<SrtlaReceiver, Error<>> {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
    let socket = Arc::new(UdpSocket::bind(addr).await?);

    Ok(SrtlaReceiver{
      socket,
      connections: Arc::new(RwLock::new(HashMap::new())),
      groups: Arc::new(RwLock::new(HashMap::new())),
      srt_connection
    })
  }

  pub async fn listen(&self) {
    let mut buf: [u8; net::MTU] = [0; net::MTU];
    self.spawn_housekeeping_task();
    loop {
      let rec_res = self.socket.recv_from(&mut buf).await;
      let Ok((num_bytes, source)) = rec_res else {
        warn!("Error reading from socket: {:?}", rec_res);
        continue;
      };
      let timestamp = Instant::now();

      let pt = packets::get_srtla_packet_type(&buf[0..num_bytes], SrtlaParserMode::Recv);
      let Ok(packet_type) = pt else {
        warn!("Error parsing incoming packet: {:?}", pt);
        continue;
      };

      let bytes = buf[0..num_bytes].to_owned();
      self.handle_packet(packet_type, bytes, source, timestamp);
    }
  }

  fn spawn_housekeeping_task(&self) {
    let groups = self.groups.clone();
    let connections = self.connections.clone();

    tokio::spawn(async move {
      let mut interval = time::interval(Duration::from_secs(5));

      loop {
        interval.tick().await;

        let mut removed_connections = Vec::new();
        let mut removed_groups = Vec::new();

        for (group_id, group) in groups.write().expect("groups lock should not be poisoned").iter_mut() {
          group.connections.retain(|c| {
            if c.last_received > (Instant::now() - Duration::from_secs(10)) {
              true
            } else {
              removed_connections.push(c.address);
              false
            }
          });
          if group.connections.is_empty() {
            removed_groups.push(*group_id)
          }
        }

        {
          let mut group_write = groups.write().expect("groups lock should not be poisoned");
          for removed_group_id in removed_groups {
            group_write.remove(&removed_group_id);
          }
        }

        {
          let mut conn_write = connections.write().expect("connection lock should not be poisoned");
          for removed_con in removed_connections {
            conn_write.remove(&removed_con);
          }
        }
      }
    });
  }

  fn handle_packet(&self, packet_type: SrtlaPacketType, data: Vec<u8>, source: SocketAddr, timestamp: Instant) {
    let socket = self.socket.clone();
    let connections = self.connections.clone();
    let groups = self.groups.clone();
    let srt_addr = self.srt_connection.remote_addr;

    // spawn small task to handle packet
    tokio::spawn(async move {
      if packet_type == SrtlaPacketType::Reg1 {
        if connections.read().expect("connection lock should not be poisoned").contains_key(&source) {
          socket.send_to(&packets::new_srtla_reg_err(), source).await?;
          return Ok(());
        }
        let view = packets::srtla_reg_1::View::new(data.as_slice());
        let client_srtla_id = view.srtla_id();
        let mut generated_id: [u8; SRTLA_ID_LEN] = [0; SRTLA_ID_LEN];
        generated_id[..SRTLA_ID_LEN/2].copy_from_slice(&client_srtla_id[..SRTLA_ID_LEN/2]);

        // Prevent duplicate IDs
        loop {
          let mut server_gen_id_part: [u8; SRTLA_ID_LEN/2] = [0; SRTLA_ID_LEN/2];
          rand::thread_rng().fill_bytes(&mut server_gen_id_part);
          generated_id[SRTLA_ID_LEN/2..].copy_from_slice(&server_gen_id_part);

          if !groups.read().expect("connection lock should not be poisoned").contains_key(&generated_id) {
            break;
          }
        }

        let new_connection = SrtlaConnection{
          address: source,
          last_received: timestamp,
          received_srt_packets: Vec::new()
        };
        let new_group = SrtlaGroup{
          connections: vec![new_connection],
          last_address: source,
          srt_socket: None,
          bytes_received: AtomicU32::new(0),
          rtt: AtomicU32::new(0),
          estimated_link_capacity: AtomicU32::new(0)
        };
        groups.write().expect("group lock should not be poisoned").insert(generated_id, new_group);
        connections.write().expect("connection lock should not be poisoned").insert(source, generated_id);

        let reg2 = packets::new_srtla_reg2(&generated_id);
        socket.send_to(&reg2, source).await?;
      }

      if packet_type == SrtlaPacketType::Reg2 {
        let view = packets::srtla_reg_2::View::new(data.as_slice());
        let req_srtla_id = view.srtla_id();

        if groups.read().expect("group lock should not be poisoned").get(req_srtla_id).is_none() {
          info!("Connection {:?} tried registering for nonexistent group.", source);
          socket.send_to(&packets::new_srtla_reg_ngp(), source).await?;
          return Ok(());
        };

        // Do logic in a different scope than sending the response, to drop the lock as fast as possible
        let early_response = match connections.read().expect("connection lock should not be poisoned").get(&source) {
          None => None,
          Some(arr) => {
            if compare_bytes(req_srtla_id, arr) != Ordering::Equal {
              info!("Already registered connection {:?} tried registering for differing group.", source);
              Some(SrtlaPacketType::RegErr)
            } else {
              Some(SrtlaPacketType::Reg3)
            }
          }
        };
        match early_response {
          Some(SrtlaPacketType::Reg3) => {
            socket.send_to(&packets::new_srtla_reg3(), source).await?;
            info!("Connection {:?} registered in group", source);
            return Ok(());
          },
          Some(SrtlaPacketType::RegErr) => {
            socket.send_to(&packets::new_srtla_reg_err(), source).await?;
            return Ok(());
          },
          _ => {}
        };


        let new_connection = SrtlaConnection{
          address: source,
          last_received: timestamp,
          received_srt_packets: Vec::new()
        };
        if let Some(group) = groups.write().expect("group lock should not be poisoned").get_mut(req_srtla_id) {
          group.last_address = source;
          group.connections.push(new_connection);
        };
        connections.write().expect("connection lock should not be poisoned").insert(source, *req_srtla_id);
        socket.send_to(&packets::new_srtla_reg3(), source).await?;
        info!("Connection {:?} registered in group", source);
      }


      if packet_type == SrtlaPacketType::Keepalive {
        let Some(srtla_id) = connections.read().expect("connection lock should not be poisoned").get(&source).copied() else {
          info!("Received Keepalive from unknown connection at {:?}, ignoring.", source);
          return Ok(());
        };

        if let Some(group) = groups.write().expect("group lock should not be poisoned").get_mut(&srtla_id) {
          group.connections.iter_mut()
            .filter(|c| c.address.eq(&source))
            .for_each(|c| c.last_received = timestamp);
        };

        socket.send_to(&packets::new_srtla_keepalive(), source).await?;
      }


      if packet_type == SrtlaPacketType::Srt {
        let Some(srtla_id) = connections.read().expect("connection lock should not be poisoned").get(&source).copied() else {
          info!("Received SRT packet from unknown connection at {:?}, ignoring.", source);
          return Ok(());
        };

        let packet_view = srt::srt_data::View::new(&data);
        let seq_num = packet_view.seq_num().read();

        let (srtla_ack_channel_send, mut srtla_ack_channel_recv) = tokio::sync::oneshot::channel();

        if let Some(group) = groups.write().expect("group lock should not be poisoned").get_mut(&srtla_id) {
          group.last_address = source;
          if let Some(c) = group.connections.iter_mut().find(|c| c.address.eq(&source)) {
            c.last_received = timestamp;
            if seq_num & (1 << 31) == 0 {
              c.received_srt_packets.push(seq_num);
              if c.received_srt_packets.len() == SRTLA_RECV_ACK_AMOUNT {
                let old_packets = mem::take(&mut c.received_srt_packets);
                srtla_ack_channel_send.send(old_packets).expect("oneshot channel should not be closed");
              }
            }
          }
        };

        if let Ok(seq_ids) = srtla_ack_channel_recv.try_recv() {
          let srtla_ack_packet = packets::new_srtla_ack(seq_ids);
          socket.send_to(srtla_ack_packet.as_slice(), source).await?;
        };

        let may_socket = match groups.read().expect("group lock should not be poisoned").get(&srtla_id) {
          None => None,
          Some(group) => { group.srt_socket.clone() }
        };

        if let Some(srt_sock) = may_socket {
          srt_sock.send(data.as_slice()).await?;
        } else {
          let new_sock = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
          new_sock.connect(srt_addr).await?;
          {
            // reader thread
            let srtla_sock = socket.clone();
            let new_sock = new_sock.clone();
            let connections = connections.clone();
            let groups = groups.clone();

            tokio::spawn(async move {
              srt_connection_loop(srtla_sock, new_sock, connections, groups, srtla_id).await;
            });
          }
          new_sock.send(data.as_slice()).await?;
          if let Some(group) = groups.write().expect("group lock should not be poisoned").get_mut(&srtla_id) {
            group.srt_socket = Some(new_sock)
          }
        }
      }

      Ok::<(), Error>(())
    });
  }

}

async fn srt_connection_loop(
  srtla_sock: Arc<UdpSocket>,
  srt_sock: Arc<UdpSocket>,
  connections: Arc<RwLock<HashMap<SocketAddr, [u8; SRTLA_ID_LEN]>>>,
  groups: Arc<RwLock<HashMap<[u8; SRTLA_ID_LEN], SrtlaGroup>>>,
  group_id: [u8; SRTLA_ID_LEN]
) {
  let mut buf: [u8; net::MTU] = [0; net::MTU];
  loop {
    let recv_res = time::timeout(Duration::from_secs(5), srt_sock.recv(&mut buf)).await;
    let Ok(Ok(num_bytes)) = recv_res else {
      break;
    };

    let data = &buf[0..num_bytes];

    let send_to: Vec<SocketAddr>;

    if let Some(group) = groups.read().expect("group lock should not be poisoned").get(&group_id) {
      if srt::is_srt_ack(data) {
        let srt_ack_data = srt::get_srt_ack_data(data);
        group.bytes_received.store(srt_ack_data.receiving_rate, atomic::Ordering::Release);
        group.rtt.store(srt_ack_data.rtt, atomic::Ordering::Release);
        group.estimated_link_capacity.store(srt_ack_data.estimated_link_capacity, atomic::Ordering::Release);
        send_to = group.connections.iter().map(|c| c.address).collect()
      } else {
        send_to = vec![group.last_address];
      }
    } else {
      break;
    }

    for addr in send_to {
      let _ = srtla_sock.send_to(data, addr).await;
    }
  }

  info!("SRT Socket disconnected, removing respective group");
  let mut csa: Vec<SocketAddr> = Vec::new();
  if let Some(group) = groups.write().expect("group lock should not be poisoned").remove(&group_id) {
    csa = group.connections.iter().map(|c| c.address).collect();
  }

  let mut connections_write = connections.write().expect("connections log should not be poisoned");
  for sa in csa {
    connections_write.remove(&sa);
  }
}

pub fn compare_bytes(a: &[u8], b: &[u8]) -> Ordering {
  a.iter()
    .zip(b)
    .map(|(x, y)| x.cmp(y))
    .find(|&ord| ord != Ordering::Equal)
    .unwrap_or(a.len().cmp(&b.len()))
}