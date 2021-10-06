//! # FIX crate
//!
//! `fix_crate` contains a FIX server and FIX client

use std::collections::HashMap;
// use std::error::Error;
use std::str;
use std::fmt;
use std::io::{Cursor, Write};

use chrono::Utc;

#[derive(Debug, Clone, PartialEq)]
pub enum FixErrorKind {
  Parse,
  MissingField,
  InvalidFormat,
  UnexpectedMessage,
}

#[derive(Debug, Clone)]
pub struct FixError {
  pub kind: FixErrorKind,
  pub field: i32,
}

impl fmt::Display for FixError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self.kind {
      FixErrorKind::Parse =>
        write!(f, "Parse error"),
      FixErrorKind::MissingField =>
        write!(f, "Message is missing field #{}", self.field),
      FixErrorKind::InvalidFormat =>
        write!(f, "Message field #{} has an invalid format", self.field),
      FixErrorKind::UnexpectedMessage =>
        write!(f, "Message kind is unexpected"),
    }
  }
}

fn get_or_fail(m: &HashMap<i32, &str>, field : i32) -> Result<String, FixError> {
  m.get(&field).map(|s| s.to_string()).ok_or(FixError{ kind: FixErrorKind::MissingField, field: field})
}

#[derive(Debug)]
pub struct NewOrder {
    pub symbol: String,
    pub clordid: String,
    pub price: i32,
    pub qty: i32,
    pub side: char,
}
impl NewOrder {
  pub fn new(m: &HashMap<i32, &str>) -> Result<NewOrder, FixError> {
    let symbol = get_or_fail(m, 55)?;
    let clordid = get_or_fail(m, 11)?;
    let price  = get_or_fail(m, 44)?;
    let price : f64 = price.parse().unwrap();
    let price = price * 10000.0;
    let price = price as i32;
    let side = get_or_fail(m, 54)?;
    let side = if "1" == side { 'B' } else { 'S' };
    let qty = get_or_fail(m, 38)?;
    let qty : i32 = qty.parse().unwrap();
    return Ok(NewOrder{symbol, clordid, price, qty, side})
  }
}
#[derive(Debug)]
pub struct CancelOrder {
    pub clordid: String,
}
impl CancelOrder {
  fn new(m : &HashMap<i32, &str>) -> Result<CancelOrder, FixError> {
    let clordid = get_or_fail(m, 11)?;
    return Ok(CancelOrder{clordid});
  }
}
#[derive(Debug)]
pub struct NewOrderAck {
    symbol: Option<String>,
    clordid: String,
}
impl NewOrderAck {
  fn new(m: &HashMap<i32, &str>) -> Result<NewOrderAck, FixError> {
    let clordid = get_or_fail(m, 11)?;
    return Ok(NewOrderAck{symbol: m.get(&55).map(|s| s.to_string()), clordid: clordid});
  }
  pub fn serialize(sendercompid: &str, targetcompid: &str, seqno: u32, clordid: &str, orderid: &str, symbol: &str, price: i32, qty: i32, side: char) -> Vec<u8> {
    let price = (price as f64) / 10000.0;
    let price = format!("{:.4}", price);
    let qty = qty.to_string();
    let side = side.to_string();
    let fields : HashMap<i32, &str> = vec![(11, clordid), (37, orderid), (20, "0"), (39, "0"), (150, "0"), (55, symbol), (44, &price), (38, &qty), (54, &side)].into_iter().collect();
    serialize("8", sendercompid, targetcompid, seqno, &fields)
  }
}
#[derive(Debug)]
pub struct CancelOrderAck {
    pub symbol: String,
    pub clorid: u64,
}
#[derive(Debug)]
pub struct Fill {
    pub symbol: String,
    pub clorid: String,
    pub exec_price: i32,
    pub exec_qty: i32,
    pub side: char,
    pub aggr_ind: char,
}
impl Fill {
  pub fn new(m: &HashMap<i32, &str>) -> Result<Fill, FixError> {
    let symbol = get_or_fail(m, 55)?;
    let clordid = get_or_fail(m, 11)?;
    let exec_price = get_or_fail(m, 31)?;
    let exec_price : f64 = exec_price.parse().map_err(|_| FixError{kind: FixErrorKind::InvalidFormat, field: 31})?;
    let exec_price = (exec_price * 10000.0) as i32;
    let exec_qty = m.get(&32).ok_or(FixError{kind:FixErrorKind::MissingField, field:32})?;
    let exec_qty : i32 = exec_qty.parse().map_err(|_| FixError{ kind: FixErrorKind::InvalidFormat, field: 32})?;
    let side = m.get(&54).ok_or(FixError{kind:FixErrorKind::MissingField, field:54})?;
    let side = if &"1" == side { 'B' } else { 'S' };
    return Ok(Fill{symbol: symbol, clorid: clordid, exec_price: exec_price, exec_qty: exec_qty, side: side, aggr_ind: 'A'});
  }
}
#[derive(Debug)]
pub struct Login {
  pub sendercompid: String,
  pub targetcompid: String,
  pub seqno: u32,
}
impl Login {
  pub fn new(msg: &HashMap<i32, &str>) -> Self {
    Self {
      sendercompid: msg.get(&49).unwrap().to_string(),
      targetcompid: msg.get(&56).unwrap().to_string(),
      seqno: msg.get(&34).unwrap().parse().unwrap(),
    }
  }
  pub fn serialize(sendercompid: &str, targetcompid: &str, seqno: u32) -> Vec<u8> {
    serialize("A", sendercompid, targetcompid, seqno, &HashMap::new())
  }
}
#[derive(Debug)]
pub struct Logout;
#[derive(Debug)]
pub struct Heartbeat;
impl Heartbeat {
  pub fn serialize(sendercompid: &str, targetcompid: &str, seqno: u32) -> Vec<u8> {
    serialize("0", sendercompid, targetcompid, seqno, &HashMap::new())
  }
}

fn serialize_body<'a>(msg: &HashMap<i32, &str>, buf: &'a mut [u8]) -> &'a [u8]{
  let mut cursor = Cursor::new(buf);
  for (k,v) in msg.iter()
    .filter(|&(k,_)| !vec![8,9,35,34].contains(k)) {
      write!(cursor, "{}={}\x01", k,v).expect("can't write!()");
  }
  let len = cursor.position() as usize;
  return &cursor.into_inner()[..len];
}

fn serialize_head<'a>(msg_type: &str, sendercompid: &str, targetcompid: &str, seqno: u32, body: &[u8], buf: &'a mut [u8]) -> &'a [u8] {
  let timestamp_format = "YYYYMMDD-HH:MM:SS.sss";
  let mut cursor = Cursor::new(buf);
  let msg_len = 4 + msg_type.len()
              + 4 + sendercompid.len() 
              + 4 + targetcompid.len()
              + 4 + timestamp_format.len()
              + 4 + 7 // seqno
              + body.len();
  let dt = Utc::now();
  let dtstr = dt.format("%Y%m%d-%T%.3f");
  write!(cursor, "8=FIX4.2\x019={}\x0135={}\x0152={}\x0149={}\x0156={}\x0134={:07}\x01", msg_len, msg_type, dtstr, sendercompid, targetcompid, seqno).unwrap();
  let len = cursor.position() as usize;
  return &cursor.into_inner()[..len];
}

fn serialize<'a>(msg_type: &str, sendercompid: &str, targetcompid: &str, seqno: u32, msg: &HashMap<i32, &str>) -> Vec<u8> {
  let mut body_buf = [0 as u8; 1024];
  let mut head_buf = [0 as u8; 1024];
  let body = serialize_body(&msg, &mut body_buf[..]);
  let head = serialize_head(msg_type, sendercompid, targetcompid, seqno, body, &mut head_buf[..]);
  let mut tail_buf = [0 as u8; 8];
  write!(&mut tail_buf[..], "10={:03}\x01", get_checksum(body, head)).unwrap();
  [head, body, &tail_buf[..7]].concat()
}

fn get_checksum(header: &[u8], body: &[u8]) -> u8 {
  let mut checksum : usize = 0;
  // TODO sfortas vectorize
  for byte in header {
    checksum += *byte as usize;
  }
  for byte in body {
    checksum += *byte as usize;
  }
  return (checksum & 0xff) as u8;
}

#[derive(Debug)]
pub enum Message {
  Login(Login),
  Heartbeat(Heartbeat),
  New(NewOrder),
  Cancel(CancelOrder),
  NewAck(NewOrderAck),
  CancelAck(CancelOrderAck),
  Fill(Fill),
  Logout(Logout),
}

static FIX_SEPARATOR : &str = "\x01"; 

/// Parses a FIX string into a hashmap<fieldno, value>.
///
/// # Example
///
/// ```
/// use fix::to_fix_hash;
/// let fix_string = "8=FIX4.2\x0135=A\x0134=1234\x0149=FOOBAR\x0156=BAZQUX\x0110=000\x01";
/// let fix_msg = to_fix_hash(&fix_string);
/// assert_eq!(fix_msg.get(&35), Some(&"A"));
/// ```
pub fn to_fix_hash(string: &str) -> HashMap<i32, &str> {
    string.split(FIX_SEPARATOR)
        .filter(|s| s.len() > 0)
        .map(|s| s.split_at(s.find("=").unwrap()))
        .map(|(key, val)| (key.parse().unwrap(), &val[1..]))
        .collect()
    // let mut map = HashMap::new();
    // let sp = s.split("|");
    // for (i, tok) in sp.enumerate() {
    //     println!("{}: {}", i, tok);
    //     if tok.len() == 0 {
    //         continue;
    //     }
    //     let kv : Vec<&str> = tok.split("=").collect();
    //     println!("kv.len() = {}", kv.len());
    //     for item in &kv {
    //         println!("item: {}", item);
    //     }
    //     // assert_eq!(kv.len(), 2);
    //     map.insert(kv[0].parse().expect("not an integer"), kv[1]);
    // }
    // return map;
}

/// Parses a FIX string into a Result<fix::Message, fix::FixError>
///
/// # Example
///
/// ```
/// use fix::parse;
/// use fix::Message;
/// let fix_string = "8=FIX4.2\x0135=A\x0134=1234\x0149=BAZQUX\x0156=FOOBAR\x0110=000\x01";
/// assert!(matches!(parse(&fix_string).unwrap(), Message::Login{..}));
/// ```
pub fn parse(fixstr: &str ) -> Result<Message, FixError>  {
  let hash = to_fix_hash(fixstr); // HashMap<i32, &str>
  if let Some(&msg_type) = hash.get(&35) {
    if msg_type == "A" {
      return Ok(Message::Login(Login::new(&hash)));
    } else if msg_type == "5" {
      return Ok(Message::Logout(Logout{}));
    } else if msg_type == "0" {
      return Ok(Message::Heartbeat(Heartbeat{}));
    } else if msg_type == "D" {
      let obj = NewOrder::new(&hash)?;
      return Ok(Message::New(obj));
    } else if msg_type == "F" {
      println!("Cancel {:?}!", hash);
      let obj = CancelOrder::new(&hash)?;
      return Ok(Message::Cancel(obj));
    } else if msg_type == "8" {
      // return Err(FixError{kind:FixErrorKind::Parse, field:0});
      if let Some(&ord_status) = hash.get(&150) {
        if ord_status == "0" {
          let obj = NewOrderAck::new(&hash)?;
          return Ok(Message::NewAck(obj));
        } else if ord_status == "1" || ord_status == "2" {
          let obj = Fill::new(&hash)?;
          return Ok(Message::Fill(obj));
        // } else if ord_status == "4" || ord_status == "C" {
        //   // canceled
        // } else {
        }
      } else {
        return Err(FixError{kind: FixErrorKind::MissingField, field:150});
      }
      return Err(FixError{kind: FixErrorKind::MissingField, field:150});
    } else {
      return Err(FixError{kind: FixErrorKind::UnexpectedMessage, field:0});
    }
  } else {
    Err(FixError{kind: FixErrorKind::MissingField, field:35})
  }
}

#[test]
fn test_to_fix_hash() {
    let data = b"8=FIX4.2\x0135=D\x0155=AAPL\x0139=100\x0111=CLORDID1\x0144=134.56\x0159=SENDER\x0110=101\x01";
    let data = str::from_utf8(data).unwrap();
    let fix = to_fix_hash(&data);
    for (key, value) in &fix {
        println!("{}: \"{}\"", key, value);
    }
    println!("{:?}", fix);
    assert_eq!(fix.get(&8), Some(&"FIX4.2"));
}

#[test]
fn test_parse_new_order() {
    let data = b"8=FIX4.2\x0135=D\x0155=AAPL\x0139=100\x0111=CLORDID1\x0154=2\x0144=134.56\x0138=600\x0159=SENDER\x0110=101\x01";
    let data = str::from_utf8(data).unwrap();
    let out = parse(data);
    println!("{:?}", out);
    assert!(out.is_ok());
    let msg = out.unwrap();
    // assert!(matches!(msg, Message::New{..}));
    match msg {
      Message::New(no) => {
        assert_eq!(no.qty, 600);
        assert_eq!(no.price, 1345600);
        assert_eq!(no.clordid, "CLORDID1");
      },
      _ => panic!("expected New"),
    }
}

#[test]
fn test_parse_cancel() {
    let data = b"8=FIX4.2\x0135=F\x0155=AAPL\x0139=100\x0111=CLORDID1\x0144=134.56\x0159=SENDER\x0110=101\x01";
    let data = str::from_utf8(data).unwrap();
    let out = parse(data);
    assert!(out.is_ok());
    println!("{:?}", out);
    let is_cancel = |m| {
      match m {
        Message::Cancel{..} => true,
        _ => false,
      }
    };
    assert!(is_cancel(out.unwrap()));
}

#[test]
fn test_parse_ack() {
    let data = b"8=FIX4.2\x0135=8\x0155=AAPL\x01150=0\x0111=CLORDID1\x0144=134.56\x0159=SENDER\x0110=101\x01";
    let data = str::from_utf8(data).unwrap();
    let out = parse(data);
    assert!(out.is_ok());
    println!("{:?}", out);
    assert!(matches!(out.unwrap(), Message::NewAck{..}));
}

#[test]
fn test_parse_fill() {
    let data = b"8=FIX4.2\x0135=8\x0155=AAPL\x01150=1\x0111=CLORDID1\x0131=134.55\x0132=300\x0154=1\x0144=134.56\x0159=SENDER\x0110=101\x01";
    let data = str::from_utf8(data).unwrap();
    let out = parse(data);
    println!("{:?}", out);
    assert!(out.is_ok());
    assert!(matches!(out.unwrap(), Message::Fill{..}));
}

#[test]
fn test_parse_fill_fail() {
    let data = b"8=FIX4.2\x0135=8\x0155=AAPL\x01150=1\x0111=CLORDID1\x0131=134.55\x0132=ABCD\x0154=1\x0144=134.56\x0159=SENDER\x0110=101\x01";
    let data = str::from_utf8(data).unwrap();
    let out = parse(data);
    println!("{:?}", out);
    assert!(out.is_err());
    let err = out.err().unwrap();
    assert_eq!(err.kind, FixErrorKind::InvalidFormat);
}

#[test]
fn test_parse_fail() {
  // no symbol
  let data = b"8=FIX4.2\x0135=D\x0139=100\x0111=CLORDID1\x0144=134.56\x0159=SENDER\x0110=101\x01";
  let data = str::from_utf8(data).unwrap();
  let out = parse(data);
  assert!(out.is_err());
  println!("{}", out.err().expect(""));
}

#[test]
fn test_atoi() {
    let num = b"12345";
    let the_string = str::from_utf8(num).expect("not utf-8");
    let the_number : i32 = the_string.parse().expect("not a number");
    assert_eq!(the_number, 12345);
}

#[test]
fn test_serialize_body() {
  let msg : HashMap<i32, &str> = vec![(8,"FIX4.2"),(9,"1234"),(52,"BAH"),(54,"QUX"),(99,"FOOBAR")].into_iter().collect();
  let mut body_buf = [0 as u8; 1024];
  let body = serialize_body(&msg, &mut body_buf[..]);
  let body_str =str::from_utf8(&body).unwrap();
  assert!(body_str.contains("52=BAH\x01") &&
          body_str.contains("54=QUX\x01") &&
          body_str.contains("99=FOOBAR\x01"));
}

#[test]
fn test_serialize() {
  let msg : HashMap<i32, &str> = vec![(8,"FIX4.2"),(9,"1234"),(52,"BAH"),(54,"QUX"),(99,"FOOBAR")].into_iter().collect();
  // assert_eq!(serialize(msg), "52=BAH\x0154=QUX\x0199=FOOBAR\x01");
  let buf = serialize("0", "SENDERCOMP", "TARGETCOMP", 1234, &msg);
  println!("{}", str::from_utf8(&buf).unwrap());
}

