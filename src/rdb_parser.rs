use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use winnow::{
    binary::{le_u16, le_u32, le_u64, le_u8},
    combinator::{fail, opt, preceded, repeat, separated_pair, trace},
    dispatch,
    token::{any, one_of, take},
    Parser, Result,
};

use crate::db_value::DbValue;

const HEADER_STRING: [u8; 9] = *b"REDIS0011";
const METADATA_HEADER: u8 = 0xFA;
const DATABASE_HEADER: u8 = 0xFE;
const EOF_HEADER: u8 = 0xFF;
const SECTION_HEADERS: [u8; 3] = [METADATA_HEADER, DATABASE_HEADER, EOF_HEADER];

enum Section {
    Metadata,
    Database,
    EOF,
}

#[derive(Debug)]
enum LengthEncodingResult {
    Int(u32),
    Str(String),
}

impl From<u32> for LengthEncodingResult {
    fn from(value: u32) -> Self {
        LengthEncodingResult::Int(value)
    }
}

impl From<String> for LengthEncodingResult {
    fn from(value: String) -> Self {
        LengthEncodingResult::Str(value)
    }
}

pub fn parse_db(input: &mut &[u8]) -> Result<HashMap<String, (DbValue, Option<SystemTime>)>> {
    parse_header.parse_next(input)?;
    parse_metadata.void().parse_next(input)?;
    0xFE.void().parse_next(input)?;
    0.void().parse_next(input)?;
    0xFB.void().parse_next(input)?;
    let _: Vec<_> = repeat(2, parse_length_encoding).parse_next(input)?;
    let values: Vec<_> = repeat(0.., parse_data_entry).parse_next(input)?;
    let mut result = HashMap::new();
    for (expiry, (key, value)) in values {
        result.insert(key, (value.into(), expiry));
    }
    parse_eof.void().parse_next(input)?;
    Ok(result)
}

fn parse_header(input: &mut &[u8]) -> Result<()> {
    b"REDIS0011".void().parse_next(input)
}

fn parse_metadata(input: &mut &[u8]) -> Result<Vec<(String, String)>> {
    repeat(0.., parse_metadata_subsection).parse_next(input)
}

fn parse_metadata_subsection(input: &mut &[u8]) -> Result<(String, String)> {
    preceded(METADATA_HEADER, (parse_string, parse_string)).parse_next(input)
}

fn parse_data_entry(input: &mut &[u8]) -> Result<(Option<SystemTime>, (String, String))> {
    separated_pair(opt(parse_expiry), 0, (parse_string, parse_string)).parse_next(input)
}

fn parse_expiry(input: &mut &[u8]) -> Result<SystemTime> {
    dispatch! {any;
               0xFC => le_u64.map(|ts| from_milli_timestamp(ts).unwrap()),
               0xFD => le_u32.map(|ts| from_secs_timestamp(ts).unwrap()),
               _ => fail::<_, SystemTime, _>
    }
    .parse_next(input)
}

fn parse_string(input: &mut &[u8]) -> Result<String> {
    match trace("string", parse_length_encoding).parse_next(input)? {
        LengthEncodingResult::Int(n) => take(n)
            .map(|inp| {
                let res = String::from_utf8_lossy(inp).to_string();
                println!("parsed {res}");
                res
            })
            .parse_next(input),
        LengthEncodingResult::Str(s) => {println!("parsed {s}"); Ok(s)},
    }
}

fn parse_length_encoding(input: &mut &[u8]) -> Result<LengthEncodingResult> {
    let first_byte = any.parse_next(input)?;
    let dispatch_value = first_byte >> 6;
    let rest = first_byte & 0b00111111;

    match dispatch_value {
        0b00 => Ok(u32::from(rest).into()),
        0b01 => {
            let next_byte = any.parse_next(input)?;
            Ok(u32::from_be_bytes([0, 0, rest, next_byte]).into())
        }
        0b10 => le_u32.parse_next(input).map(LengthEncodingResult::Int),
        0b11 => match first_byte {
            0xC0 => le_u8.parse_next(input).map(|v| v.to_string().into()),
            0xC1 => le_u16.parse_next(input).map(|v| v.to_string().into()),
            0xC2 => le_u32.parse_next(input).map(|v| v.to_string().into()),
            _ => fail.parse_next(input),
        },
        _ => unreachable!(),
    }
}

fn parse_section_header(input: &mut &[u8]) -> Result<Section> {
    let byte = one_of(SECTION_HEADERS).parse_next(input)?;
    match byte {
        METADATA_HEADER => Ok(Section::Metadata),
        DATABASE_HEADER => Ok(Section::Database),
        EOF_HEADER => Ok(Section::EOF),
        _ => unreachable!(),
    }
}

fn parse_eof(input: &mut &[u8]) -> Result<()> {
    preceded(0xFF, le_u64).void().parse_next(input)
}

fn from_milli_timestamp(timestamp: u64) -> Option<SystemTime> {
    UNIX_EPOCH.checked_add(Duration::from_millis(timestamp))
}

fn from_secs_timestamp(timestamp: u32) -> Option<SystemTime> {
    UNIX_EPOCH.checked_add(Duration::from_secs(timestamp.into()))
}
