extern crate srtla;

use std::sync::Arc;
use anyhow::{Context, Error, Result};
use clap::{Arg, command, value_parser, ValueHint};
use url::Url;
use srtla::net::srt::SrtConnection;

#[tokio::main]
async fn main() -> Result<()> {
  env_logger::init();

  let matches = command!()
    .arg(
      Arg::new("port")
          .required(true)
          .value_parser(value_parser!(u16).range(1..65535))
    )
    .arg(
      Arg::new("push")
          .required(true)
          .value_parser(value_parser!(Url)).value_hint(ValueHint::Url)
    )
    .arg(
      Arg::new("with-stats")
          .long("with-stats")
          .required(false)
          .help("port for the HTTP stats server")
          .value_parser(value_parser!(u16).range(1..65535))
    )
    .get_matches();

  let port: u16 = *matches.get_one::<u16>("port").expect("port is required and should be parsed");
  let push_url: &Url = matches.get_one::<Url>("push").expect("push url is required and should be parsed");
  if (push_url.is_special() && push_url.scheme() != "srt") || push_url.cannot_be_a_base() {
    return Err(Error::msg("Invalid URL scheme for SRT Push URL"))
  }

  let stats_port = matches.get_one::<u16>("with-stats");

  let srt_connection = SrtConnection::new(push_url).context("Could not resolve SRT connection URL")?;
  srt_connection.probe().await.context("Could not connect to SRT server")?;

  let stat_server = if let Some(p) = stats_port {
    let stat_server = Arc::new(srtla::http::stats_server::StatsServer::new(*p).await);
    {
      let stat_server = stat_server.clone();
      tokio::spawn(async move {
        stat_server.listen().await.expect("Could not start HTTP Stat Server");
      });
    }
    Some(stat_server)
  } else {
    None
  };

  let receiver = srtla::net::receiver::SrtlaReceiver::new(port, srt_connection, stat_server).await?;

  receiver.listen().await;
  Ok(())
}