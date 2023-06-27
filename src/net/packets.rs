use std::fmt::Debug;
use anyhow::{Result, Error};
use binary_layout::prelude::*;

pub const SRTLA_ID_LEN: usize = 256;
const SRT_MIN_LEN: usize = 16;
const SRTLA_TYPE_KEEPALIVE_LEN: usize = 2;

const SRTLA_TYPE_REG1_LEN: usize = 2 + SRTLA_ID_LEN;
const SRTLA_TYPE_REG2_LEN: usize = 2 + SRTLA_ID_LEN;
// const SRTLA_TYPE_REG3_LEN: usize = 2;

const SRTLA_TYPE_KEEPALIVE: u16 = 0x9000;
const SRTLA_TYPE_ACK: u16 = 0x9100;
const SRTLA_TYPE_REG1: u16 = 0x9200;
const SRTLA_TYPE_REG2: u16 = 0x9201;
const SRTLA_TYPE_REG3: u16 = 0x9202;
const SRTLA_TYPE_REG_ERR: u16 = 0x9210;
const SRTLA_TYPE_REG_NGP: u16 = 0x9211;
// const SRTLA_TYPE_REG_NAK: u16 = 0x9212;

#[derive(Debug, PartialEq)]
pub enum SrtlaParserMode {
  Recv,
  Send
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum SrtlaPacketType {
  Keepalive,
  Ack,
  Reg1,
  Reg2,
  Reg3,
  RegErr,
  RegNgp,
  RegNak,
  Srt
}

define_layout!(srtla_reg_1, BigEndian, {
  header: u16,
  srtla_id: [u8; SRTLA_ID_LEN]
});

define_layout!(srtla_reg_2, BigEndian, {
  header: u16,
  srtla_id: [u8; SRTLA_ID_LEN]
});


pub fn get_srtla_packet_type(bytes: &[u8], parser_mode: SrtlaParserMode) -> Result<SrtlaPacketType, Error> {
  if bytes.len() < 2 {
    return Err(Error::msg("not enough bytes"));
  }
  let header = u16::from_be_bytes([bytes[0], bytes[1]]);

  match parser_mode {
    SrtlaParserMode::Recv => {
      if bytes.len() == SRTLA_TYPE_KEEPALIVE_LEN && header == SRTLA_TYPE_KEEPALIVE {
        return Ok(SrtlaPacketType::Keepalive);
      } else if bytes.len() == SRTLA_TYPE_REG1_LEN && header == SRTLA_TYPE_REG1 {
        return Ok(SrtlaPacketType::Reg1);
      } else if bytes.len() == SRTLA_TYPE_REG2_LEN && header == SRTLA_TYPE_REG2 {
        return Ok(SrtlaPacketType::Reg2);
      } else if bytes.len() >= SRT_MIN_LEN {
        return Ok(SrtlaPacketType::Srt);
      }
    }
    SrtlaParserMode::Send => {
      unimplemented!()
    }
  }
  Err(Error::msg("unexpected packet"))
}

pub fn new_srtla_keepalive() -> [u8; 2] {
  SRTLA_TYPE_KEEPALIVE.to_be_bytes()
}

pub fn new_srtla_reg2(srtla_id: &[u8; SRTLA_ID_LEN]) -> [u8; 2+SRTLA_ID_LEN] {
  let vec: Vec<u8> = SRTLA_TYPE_REG2.to_be_bytes().iter()
    .chain(srtla_id.iter())
    .copied()
    .collect();
  vec.try_into().expect("conversion to known-size array should succeed")
}

pub fn new_srtla_reg3() -> [u8; 2] {
  SRTLA_TYPE_REG3.to_be_bytes()
}

pub fn new_srtla_reg_err() -> [u8; 2] {
  SRTLA_TYPE_REG_ERR.to_be_bytes()
}

pub fn new_srtla_reg_ngp() -> [u8; 2] {
  SRTLA_TYPE_REG_NGP.to_be_bytes()
}

pub fn new_srtla_ack(seq_ids: Vec<u32>) -> Vec<u8> {
  u32::from(SRTLA_TYPE_ACK).to_be_bytes().iter()
    .copied()
    .chain(seq_ids.iter().flat_map(|num| num.to_be_bytes()))
    .collect()
}