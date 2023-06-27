use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;
use anyhow::{Result, Error};
use binary_layout::prelude::*;
use tokio::net::UdpSocket;
use tokio::time;
use url::Url;

define_layout!(srt_data, BigEndian, {
  seq_num: u32,
  rest: [u8],
});

define_layout!(srt_ack, BigEndian, {
  ignored: [u8; 20],
  rtt: u32,
  ignored_2: [u8; 12],
  estimated_link_capacity: u32,
  receiving_rate: u32,
});

const SRT_TYPE_HANDSHAKE: u16 = 0x8000;
const SRT_TYPE_ACK: u16 = 0x8002;

pub struct SrtConnection {
  pub remote_addr: SocketAddr
}

impl SrtConnection {

  pub fn new(url: &Url) -> Result<Self, Error> {
    let addr_str = format!("{}:{}", url.host_str().ok_or(Error::msg("URL missing host"))?, url.port().unwrap_or(9710));
    let addr = addr_str.to_socket_addrs()?.next().ok_or(Error::msg("Error resolving SRT url to socket address"))?;
    Ok(SrtConnection{
      remote_addr: addr,
    })
  }

  pub async fn probe(&self) -> Result<(), Error> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.send_to(&new_srt_handshake(4, 2, 1), self.remote_addr).await?;

    let mut buf: [u8; crate::net::MTU] = [0; crate::net::MTU];

    match time::timeout(Duration::from_secs(1), socket.recv(&mut buf)).await? {
      Ok(64) => { Ok(()) }
      Ok(_) => { Err(Error::msg("Response has unexpected size")) }
      Err(err) => { Err(Error::from(err)) }
    }
  }

}

pub fn is_srt_ack(data: &[u8]) -> bool {
  if data.len() < 2 {
    return false
  }
  u16::from_be_bytes([data[0], data[1]]) == SRT_TYPE_ACK
}

pub struct SrtAckData {
  pub receiving_rate: u32,
  pub rtt: u32,
  pub estimated_link_capacity: u32
}

pub fn get_srt_ack_data(data: &[u8]) -> SrtAckData {
  let view = srt_ack::View::new(data);
  return SrtAckData{
    receiving_rate: view.receiving_rate().read(),
    rtt: view.rtt().read(),
    estimated_link_capacity: view.estimated_link_capacity().read()
  }
}

fn new_srt_handshake(version: u32, ext_field: u16, handshake_type: u32) -> [u8; 64] {
  let vec: Vec<u8> = SRT_TYPE_HANDSHAKE.to_be_bytes().iter()
    .copied()
    .chain(std::iter::repeat(0x00).take(14))
    .chain(version.to_be_bytes())
    .chain(std::iter::repeat(0x00).take(2))
    .chain(ext_field.to_be_bytes())
    .chain(std::iter::repeat(0x00).take(12))
    .chain(handshake_type.to_be_bytes())
    .chain(std::iter::repeat(0x00).take(24))
    .collect();
  vec.try_into().expect("conversion to known-size array should succeed")
}