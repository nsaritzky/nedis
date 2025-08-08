use std::{
    collections::{HashMap, VecDeque},
    str::FromStr,
};

use crate::redis_value::{PrimitiveRedisValue, RedisValue};
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

pub fn parse_value<'a>(input: &mut &'a [u8]) -> Result<RedisValue> {
    dispatch! {any;
            b'+' => parse_simple_string.map(|res| RedisValue::Primitive(PrimitiveRedisValue::Str(res))),
            b'-' => parse_simple_string.map(|res| RedisValue::Primitive(PrimitiveRedisValue::Str(res))),
            b'$' => parse_bulk_string.map(|res| RedisValue::Primitive(PrimitiveRedisValue::Str(res))),
            b':' => dec_int.map(|res| RedisValue::Primitive(PrimitiveRedisValue::Int(res))),
            b'*' => parse_array.map(|arr| RedisValue::Arr(VecDeque::from(arr))),
            b'_' => parse_null,
            b'#' => parse_bool,
            b',' => parse_double,
            b'(' => parse_bigint,
            b'!' => parse_bulk_string.map(|res| RedisValue::Primitive(PrimitiveRedisValue::Error(res))),
            b'=' => parse_verbatim,
            b'%' => parse_map.map(RedisValue::Map),
            b'|' => parse_map.map(RedisValue::Attribute),
            b'~' => parse_set,
            b'>' => parse_array.map(RedisValue::Push),
            _ => fail::<_, RedisValue, _>
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

fn parse_array<'a>(input: &mut &'a [u8]) -> Result<Vec<RedisValue>> {
    let n: usize = dec_uint.parse_next(input)?;
    preceded(crlf, repeat(n, parse_value)).parse_next(input)
}

fn parse_null<'a>(_input: &mut &'a [u8]) -> Result<RedisValue> {
    Ok(RedisValue::Primitive(PrimitiveRedisValue::Null))
}

fn parse_bool<'a>(input: &mut &'a [u8]) -> Result<RedisValue> {
    let c = terminated(alt(("t", "f")), crlf).parse_next(input)?;
    if c == b"t" {
        Ok(RedisValue::Primitive(PrimitiveRedisValue::Bool(true)))
    } else {
        Ok(RedisValue::Primitive(PrimitiveRedisValue::Bool(false)))
    }
}

fn parse_double<'a>(input: &mut &'a [u8]) -> Result<RedisValue> {
    terminated(float, crlf)
        .parse_next(input)
        .map(RedisValue::Double)
}

fn parse_bigint_str(input: &mut &[u8]) -> Result<String> {
    let sign = opt(one_of(['+', '-'])).parse_next(input)?;
    let num_string =
        String::from_utf8_lossy(terminated(digit1, crlf).parse_next(input)?).to_string();
    if let Some(sign) = sign {
        Ok(format!("{}{}", sign, num_string))
    } else {
        Ok(num_string)
    }
}

fn parse_bigint<'a>(input: &mut &'a [u8]) -> Result<RedisValue> {
    parse_bigint_str
        .try_map(|s: String| BigInt::from_str(&s))
        .parse_next(input)
        .map(|res| RedisValue::Primitive(PrimitiveRedisValue::BigInt(res)))
}

fn parse_verbatim<'a>(input: &mut &'a [u8]) -> Result<RedisValue> {
    let n: usize = terminated(dec_uint, crlf).parse_next(input)?;
    let (encoding, data) = separated_pair(take(3usize), ":", take(n - 4)).parse_next(input)?;
    crlf.parse_next(input)?;
    Ok(RedisValue::Primitive(PrimitiveRedisValue::Verbatim(
        encoding.to_owned(),
        data.to_owned(),
    )))
}

fn parse_map<'a>(input: &mut &'a [u8]) -> Result<HashMap<PrimitiveRedisValue, RedisValue>> {
    let n: usize = terminated(dec_uint, crlf).parse_next(input)?;
    let mut result = HashMap::new();
    for _ in 0..n {
        let (key, value) = (parse_value, parse_value).parse_next(input)?;
        if let RedisValue::Primitive(k) = key {
            result.insert(k, value);
        }
    }
    Ok(result)
}

fn parse_set<'a>(input: &mut &'a [u8]) -> Result<RedisValue> {
    let n: usize = terminated(dec_uint, crlf).parse_next(input)?;
    repeat(n, parse_value)
        .parse_next(input)
        .map(|res| RedisValue::Set(res))
}
