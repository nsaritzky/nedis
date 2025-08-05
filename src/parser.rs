use std::{collections::HashMap, str::FromStr};

use num::BigInt;
use winnow::{
    ascii::{crlf, dec_int, dec_uint, digit1, float},
    combinator::{
        alt, delimited, dispatch, fail, opt, preceded, repeat, separated_pair, terminated,
    },
    stream::Stream,
    token::{any, literal, one_of, take, take_until},
    Parser, Result,
};

#[derive(PartialEq, Eq, Hash, Debug)]
pub enum PrimitiveParseResult<'a> {
    Str(String),
    Error(String),
    Int(isize),
    Null,
    Bool(bool),
    BigInt(BigInt),
    Verbatim(&'a [u8], &'a [u8]),
}

#[derive(Debug)]
pub enum ParseResult<'a> {
    Primitive(PrimitiveParseResult<'a>),
    Arr(Vec<ParseResult<'a>>),
    Double(f64),
    Map(HashMap<PrimitiveParseResult<'a>, ParseResult<'a>>),
    Attribute(HashMap<PrimitiveParseResult<'a>, ParseResult<'a>>),
    Set(Vec<ParseResult<'a>>),
    Push(Vec<ParseResult<'a>>),
}

pub fn parse_values<'a>(input: &mut &'a [u8]) -> Result<Vec<ParseResult<'a>>> {
    repeat(0.., parse_value).parse_next(input)
}

pub fn parse_value<'a>(input: &mut &'a [u8]) -> Result<ParseResult<'a>> {
    dispatch! {any;
            b'+' => parse_simple_string.map(|res| ParseResult::Primitive(PrimitiveParseResult::Str(res))),
            b'-' => parse_simple_string.map(|res| ParseResult::Primitive(PrimitiveParseResult::Str(res))),
            b'$' => parse_bulk_string.map(|res| ParseResult::Primitive(PrimitiveParseResult::Str(res))),
            b':' => dec_int.map(|res| ParseResult::Primitive(PrimitiveParseResult::Int(res))),
            b'*' => parse_array.map(ParseResult::Arr),
            b'_' => parse_null,
            b'#' => parse_bool,
            b',' => parse_double,
            b'(' => parse_bigint,
            b'!' => parse_bulk_string.map(|res| ParseResult::Primitive(PrimitiveParseResult::Error(res))),
            b'=' => parse_verbatim,
            b'%' => parse_map.map(ParseResult::Map),
            b'|' => parse_map.map(ParseResult::Attribute),
            b'~' => parse_set,
            b'>' => parse_array.map(ParseResult::Push),
            _ => fail::<_, ParseResult<'a>, _>
    }
    .parse_next(input)
}

fn parse_bulk_string<'a>(input: &mut &'a [u8]) -> Result<String> {
    let n: usize = dec_uint.parse_next(input)?;
    let result = delimited(crlf, take(n), crlf).parse_next(input)?;
    Ok(String::from_utf8_lossy(result).to_string())
}

fn parse_simple_string<'a>(input: &mut &'a [u8]) -> Result<String> {
    let result = terminated(take_until(0.., "\r\n"), crlf).parse_next(input)?;
    Ok(String::from_utf8_lossy(result).to_string())
}

fn parse_array<'a>(input: &mut &'a [u8]) -> Result<Vec<ParseResult<'a>>> {
    let n: usize = dec_uint.parse_next(input)?;
    preceded(crlf, repeat(n, parse_value)).parse_next(input)
}

fn parse_null<'a>(input: &mut &'a [u8]) -> Result<ParseResult<'a>> {
    Ok(ParseResult::Primitive(PrimitiveParseResult::Null))
}

fn parse_bool<'a>(input: &mut &'a [u8]) -> Result<ParseResult<'a>> {
    let c = terminated(alt(("t", "f")), crlf).parse_next(input)?;
    if c == b"t" {
        Ok(ParseResult::Primitive(PrimitiveParseResult::Bool(true)))
    } else {
        Ok(ParseResult::Primitive(PrimitiveParseResult::Bool(false)))
    }
}

fn parse_double<'a>(input: &mut &'a [u8]) -> Result<ParseResult<'a>> {
    terminated(float, crlf).parse_next(input).map(ParseResult::Double)
}

fn parse_bigint_str(input: &mut &[u8]) -> Result<String> {
    let sign = opt(one_of(['+', '-'])).parse_next(input)?;
    let num_string = String::from_utf8_lossy(terminated(digit1, crlf).parse_next(input)?).to_string();
    if let Some(sign) = sign {
        Ok(format!("{}{}", sign, num_string))
    } else {
        Ok(num_string)
    }
}

fn parse_bigint<'a>(input: &mut &'a [u8]) -> Result<ParseResult<'a>> {
    parse_bigint_str
        .try_map(|s: String| BigInt::from_str(&s))
        .parse_next(input)
        .map(|res| ParseResult::Primitive(PrimitiveParseResult::BigInt(res)))
}

fn parse_verbatim<'a>(input: &mut &'a [u8]) -> Result<ParseResult<'a>> {
    let n: usize = terminated(dec_uint, crlf).parse_next(input)?;
    let (encoding, data) = separated_pair(take(3usize), ":", take(n - 4)).parse_next(input)?;
    crlf.parse_next(input)?;
    Ok(ParseResult::Primitive(PrimitiveParseResult::Verbatim(
        encoding, data,
    )))
}

fn parse_map<'a>(
    input: &mut &'a [u8],
) -> Result<HashMap<PrimitiveParseResult<'a>, ParseResult<'a>>> {
    let n: usize = terminated(dec_uint, crlf).parse_next(input)?;
    let mut result = HashMap::new();
    for _ in 0..n {
        let (key, value) = (parse_value, parse_value).parse_next(input)?;
        if let ParseResult::Primitive(k) = key {
            result.insert(k, value);
        }
    }
    Ok(result)
}

fn parse_set<'a>(input: &mut &'a [u8]) -> Result<ParseResult<'a>> {
    let n: usize = terminated(dec_uint, crlf).parse_next(input)?;
    repeat(n, parse_value)
        .parse_next(input)
        .map(|res| ParseResult::Set(res))
}
