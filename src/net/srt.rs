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

const SRT_TYPE_HANDSHAKE: u16 = 0x8000;
const SRT_TYPE_ACK: u16 = 0x8002;
const SRT_TYPE_NAK: u16 = 0x8003;

const SRT_EXTENSION_TYPE_SID: u16 = 5;

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
  if data.len() <= 2 {
    return false
  }
  u16::from_be_bytes([data[0], data[1]]) == SRT_TYPE_ACK
}

pub fn is_srt_nak(data: &[u8]) -> bool {
  if data.len() <= 2 {
    return false
  }
  u16::from_be_bytes([data[0], data[1]]) == SRT_TYPE_NAK
}

pub fn nak_count_missing(data: &[u8]) -> u32 {
  let mut missing_count = 0;
  let mut current_row = 4;

  loop {
    if current_row >= data.len()/4 {
      break
    }
    if data[current_row*4] & 0b10000000 == 0 {
      // single nak
      missing_count += 1;
      current_row += 1;
    } else {
      let from_seq = u32::from_be_bytes([data[current_row*4] & 0b01111111, data[current_row*4+1], data[current_row*4+2], data[current_row*4+3]]);
      current_row += 1;
      let to_seq = u32::from_be_bytes([data[current_row*4], data[current_row*4+1], data[current_row*4+2], data[current_row*4+3]]);
      missing_count += to_seq - from_seq;
      current_row += 1;
    }
  }
  missing_count
}

pub fn is_srt_handshake(data: &[u8]) -> bool {
  if data.len() < 2 {
    return false
  }
  u16::from_be_bytes([data[0], data[1]]) == SRT_TYPE_HANDSHAKE
}

pub fn is_srt_data(data: &[u8]) -> bool {
  data[0] & 0b10000000 == 0
}

pub fn extract_srt_stream_id(data: &[u8]) -> Result<String, Error> {
  if data.len() < 68 {
    return Err(Error::msg("Handshake packet does not contain extensions"));
  }
  let mut current_extension_idx = 64;

  loop {
    let extension_type = u16::from_be_bytes([data[current_extension_idx], data[current_extension_idx+1]]);
    let extension_length = usize::from(u16::from_be_bytes([data[current_extension_idx+2], data[current_extension_idx+3]]))*4;

    if extension_type == SRT_EXTENSION_TYPE_SID {
      if data.len() < current_extension_idx + 4 + extension_length {
        return Err(Error::msg("invalid extension length"))
      }
      let mut utf_data = data[current_extension_idx+4..current_extension_idx+4+extension_length].to_owned();
      reverse_blocks_of_four(&mut utf_data);

      return Ok(String::from_utf8_lossy(&utf_data).trim_matches(char::from(0)).to_string())
    } else if data.len() >= current_extension_idx + 4 + extension_length + 4 {
      current_extension_idx = current_extension_idx + 4 + extension_length;
    } else {
      break;
    }
  }
  Err(Error::msg("Handshake packet does not contain SID extension"))
}

fn reverse_blocks_of_four(vec: &mut Vec<u8>) {
  let mut index = 0;

  while index + 4 <= vec.len() {
    vec[index..index + 4].reverse();
    index += 4;
  }
}

pub fn get_srt_ack_rtt(data: &[u8]) -> u32 {
  u32::from_be_bytes([data[20], data[21], data[22], data[23]])
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