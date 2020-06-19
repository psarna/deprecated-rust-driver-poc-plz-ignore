use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::prelude::*;
use serde::{Serialize, Deserialize};
use bincode::{config};
use std::env;
use std::convert::TryInto;
use std::str::from_utf8;

/*
  The CQL binary protocol is a frame based protocol. Frames are defined as:

      0         8        16        24        32         40
      +---------+---------+---------+---------+---------+
      | version |  flags  |      stream       | opcode  |
      +---------+---------+---------+---------+---------+
      |                length                 |
      +---------+---------+---------+---------+
      |                                       |
      .            ...  body ...              .
      .                                       .
      .                                       .
      +----------------------------------------

  The protocol is big-endian (network byte order).

  Each frame contains a fixed size header (9 bytes) followed by a variable size
  body. The header is described in Section 2. The content of the body depends
  on the header opcode value (the body can in particular be empty for some
  opcode values). The list of allowed opcode is defined Section 2.4 and the
  details of each corresponding message is described Section 4.

  The protocol distinguishes 2 types of frames: requests and responses. Requests
  are those frame sent by the clients to the server, response are the ones sent
  by the server. Note however that the protocol supports server pushes (events)
  so responses does not necessarily come right after a client request.

  Note to client implementors: clients library should always assume that the
  body of a given frame may contain more data than what is described in this
  document. It will however always be safe to ignore the remaining of the frame
  body in such cases. The reason is that this may allow to sometimes extend the
  protocol with optional features without needing to change the protocol
  version.
*/

#[derive(Debug, PartialEq)]
pub enum Opcode {
    Error,
    Startup,
    Ready,
    Authenticate,
    Options,
    Supported,
    Query,
    Result,
    Prepare,
    Execute,
    Register,
    Event,
    Batch,
    AuthChallenge,
    AuthResponse,
    AuthSuccess,
}

impl Into<u8> for Opcode {
    fn into(self) -> u8 {
        match self {
            Opcode::Error => 0x00,
            Opcode::Startup => 0x01,
            Opcode::Ready => 0x02,
            Opcode::Authenticate => 0x03,
            Opcode::Options => 0x05,
            Opcode::Supported => 0x06,
            Opcode::Query => 0x07,
            Opcode::Result => 0x08,
            Opcode::Prepare => 0x09,
            Opcode::Execute => 0x0A,
            Opcode::Register => 0x0B,
            Opcode::Event => 0x0C,
            Opcode::Batch => 0x0D,
            Opcode::AuthChallenge => 0x0E,
            Opcode::AuthResponse => 0x0F,
            Opcode::AuthSuccess => 0x10,
        }
    }
}

impl From<u8> for Opcode {
    fn from(b: u8) -> Opcode {
        match b {
            0x00 => Opcode::Error,
            0x01 => Opcode::Startup,
            0x02 => Opcode::Ready,
            0x03 => Opcode::Authenticate,
            0x05 => Opcode::Options,
            0x06 => Opcode::Supported,
            0x07 => Opcode::Query,
            0x08 => Opcode::Result,
            0x09 => Opcode::Prepare,
            0x0A => Opcode::Execute,
            0x0B => Opcode::Register,
            0x0C => Opcode::Event,
            0x0D => Opcode::Batch,
            0x0E => Opcode::AuthChallenge,
            0x0F => Opcode::AuthResponse,
            0x10 => Opcode::AuthSuccess,
            _ => unreachable!(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Header {
    version: u8,
    flags: u8,
    stream: u16,
    opcode: u8,
    length: u32
}

async fn init_connection(stream: &mut TcpStream) {
    let mut bincode = config();
    bincode.big_endian();

    let header = Header {
        version: 0x04,
        flags: 0x00,
        stream: 0x0007,
        opcode: Opcode::Startup.into(),
        length: 0
    };
    let request = bincode.serialize(&header).unwrap();
    println!("{:?}", request);
    let sent = stream.write(&request).await.unwrap();
    println!("Sent {:?}", sent);

    let mut header_buf: [u8; 9] = [0; 9];
    stream.read_exact(&mut header_buf).await.unwrap();
    let len: u32 = bincode.deserialize(&header_buf[5..9]).unwrap();
    println!("len {}, {:?}", len, header_buf);
    let mut payload: Vec<u8> = vec![0; len.try_into().unwrap()];
    stream.read_exact(&mut payload).await.unwrap();
    println!("Payload: {:?}", from_utf8(&payload).unwrap());
}

async fn 

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect(); 
    let addr = SocketAddr::from(([127, 0, 0, 1], args[1].parse().unwrap()));
    let mut stream = TcpStream::connect(addr).await.unwrap();

    init_connection(&mut stream).await;
}
