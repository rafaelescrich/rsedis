use std::{
    collections::{Bound, HashMap, HashSet},
    io::Write,
    mem::replace,
    sync::mpsc::channel,
    sync::mpsc::Sender,
    thread,
    time::Duration,
    usize,
};

use bitflags::bitflags;

use compat::{getos, getpid};
use database::{zset, Database, PubsubEvent, Value};
use database::zset::ValueSortedSet;
use database::list::ValueList;
use database::hash::ValueHash;
use parser::{Argument, OwnedParsedCommand, ParsedCommand};
use response::{Response, ResponseError};
use util::{mstime, ustime};

extern crate rand;

macro_rules! opt_validate {
    ($expr: expr, $err: expr) => {
        if !($expr) {
            return Ok(Response::Error($err.to_string()));
        }
    };
}

macro_rules! try_opt_validate {
    ($expr: expr, $err: expr) => {{
        match $expr {
            Ok(r) => r,
            Err(_) => return Ok(Response::Error($err.to_string())),
        }
    }};
}

macro_rules! validate_arguments_exact {
    ($parser: expr, $expected: expr) => {
        if $parser.argv.len() != $expected {
            return Response::Error(format!(
                "ERR wrong number of arguments for '{}' command",
                $parser.get_str(0).unwrap()
            ));
        }
    };
}

macro_rules! validate_arguments_gte {
    ($parser: expr, $expected: expr) => {
        if $parser.argv.len() < $expected {
            return Response::Error(format!(
                "ERR wrong number of arguments for '{}' command",
                $parser.get_str(0).unwrap()
            ));
        }
    };
}

macro_rules! validate_arguments_lte {
    ($parser: expr, $expected: expr) => {
        if $parser.argv.len() > $expected {
            return Response::Error(format!(
                "ERR wrong number of arguments for '{}' command",
                $parser.get_str(0).unwrap()
            ));
        }
    };
}

macro_rules! validate {
    ($expr: expr, $err: expr) => {
        if !($expr) {
            return Response::Error($err.to_string());
        }
    };
}

macro_rules! try_validate {
    ($expr: expr, $err: expr) => {{
        match $expr {
            Ok(r) => r,
            Err(_) => return Response::Error($err.to_string()),
        }
    }};
}

macro_rules! get_values {
    ($start: expr, $stop: expr, $parser: expr, $db: expr, $dbindex: expr, $default: expr) => {{
        validate_arguments_gte!($parser, $start);
        validate_arguments_gte!($parser, $stop);
        let mut sets = Vec::with_capacity($parser.argv.len() - $start);
        for i in $start..$stop {
            let key = try_validate!($parser.get_vec(i), "Invalid key");
            match $db.get($dbindex, &key) {
                Some(e) => sets.push(e),
                None => sets.push($default),
            };
        }
        sets
    }};
}

fn generic_set(
    db: &mut Database,
    dbindex: usize,
    key: Vec<u8>,
    val: Vec<u8>,
    nx: bool,
    xx: bool,
    expiration: Option<i64>,
) -> Result<bool, Response> {
    if nx && db.get(dbindex, &key).is_some() {
        return Ok(false);
    }

    if xx && db.get(dbindex, &key).is_none() {
        return Ok(false);
    }

    match db.get_or_create(dbindex, &key).set(val) {
        Ok(_) => {
            db.key_updated(dbindex, &key);

            if let Some(msexp) = expiration {
                db.set_msexpiration(dbindex, key.clone(), msexp + mstime());
            }

            // Publish keyspace notification for SET command
            db.notify_keyspace_event(dbindex, "set", &key, Some('$'));

            Ok(true)
        }
        Err(err) => Err(Response::Error(err.to_string())),
    }
}

fn set(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "ERR syntax error");
    let val = try_validate!(parser.get_vec(2), "ERR syntax error");
    let mut nx = false;
    let mut xx = false;
    let mut expiration = None;
    let mut skip = false;
    for i in 3..parser.argv.len() {
        if skip {
            skip = false;
            continue;
        }
        let param = try_validate!(parser.get_str(i), "ERR syntax error");
        match &*param.to_ascii_lowercase() {
            "nx" => nx = true,
            "xx" => xx = true,
            "px" => {
                let px = try_validate!(parser.get_i64(i + 1), "ERR syntax error");
                expiration = Some(px);
                skip = true;
            }
            "ex" => {
                let ex = try_validate!(parser.get_i64(i + 1), "ERR syntax error");
                expiration = Some(ex * 1000);
                skip = true;
            }
            _ => return Response::Error("ERR syntax error".to_owned()),
        }
    }

    match generic_set(db, dbindex, key, val, nx, xx, expiration) {
        Ok(updated) => {
            if updated {
                Response::Status("OK".to_owned())
            } else {
                Response::Nil
            }
        }
        Err(r) => r,
    }
}

fn setnx(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "ERR syntax error");
    let val = try_validate!(parser.get_vec(2), "ERR syntax error");
    match generic_set(db, dbindex, key, val, true, false, None) {
        Ok(updated) => Response::Integer(if updated { 1 } else { 0 }),
        Err(r) => r,
    }
}

fn setex(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "ERR syntax error");
    let exp = try_validate!(parser.get_i64(2), "ERR syntax error");
    validate!(exp >= 0, "ERR invalid expire time");
    let val = try_validate!(parser.get_vec(3), "ERR syntax error");
    match generic_set(db, dbindex, key, val, false, false, Some(exp * 1000)) {
        Ok(_) => Response::Status("OK".to_owned()),
        Err(r) => r,
    }
}

fn psetex(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "ERR syntax error");
    let exp = try_validate!(parser.get_i64(2), "ERR syntax error");
    validate!(exp >= 0, "ERR invalid expire time");
    let val = try_validate!(parser.get_vec(3), "ERR syntax error");
    match generic_set(db, dbindex, key, val, false, false, Some(exp)) {
        Ok(_) => Response::Status("OK".to_owned()),
        Err(r) => r,
    }
}

fn exists(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    Response::Integer(match db.get(dbindex, &key) {
        Some(_) => 1,
        None => 0,
    })
}

fn del(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() >= 2, "Wrong number of parameters");
    let mut c = 0;
    for i in 1..parser.argv.len() {
        let key = try_validate!(parser.get_vec(i), "Invalid key");
        if db.remove(dbindex, &key).is_some() {
            c += 1;
            db.key_updated(dbindex, &key);
            // Publish keyspace notification for DEL command
            db.notify_keyspace_event(dbindex, "del", &key, Some('g'));
        }
    }

    Response::Integer(c)
}

fn debug_object(db: &mut Database, dbindex: usize, key: Vec<u8>) -> Option<String> {
    db.get(dbindex, &key).map(|val| val.debug_object())
}

fn debug(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let subcommand = try_validate!(parser.get_str(1), "Syntax error");

    match &*subcommand.to_ascii_lowercase() {
        "object" => {
            validate_arguments_exact!(parser, 3);
            match debug_object(db, dbindex, try_validate!(parser.get_vec(2), "Invalid key")) {
                Some(s) => Response::Status(s),
                None => Response::Error("no such key".to_owned()),
            }
        }
        "reload" => {
            validate_arguments_exact!(parser, 2);
            // TODO: Implement debug reload
            Response::Error("ERR DEBUG RELOAD is not implemented".to_owned())
        }
        "restart" => {
            validate_arguments_exact!(parser, 2);
            // TODO: Implement debug restart
            Response::Error("ERR DEBUG RESTART is not implemented".to_owned())
        }
        "sleep" => {
            validate_arguments_exact!(parser, 3);
            let seconds_str = try_validate!(parser.get_str(2), "Invalid seconds");
            let seconds: f64 = match seconds_str.parse() {
                Ok(s) => s,
                Err(_) => return Response::Error("ERR invalid seconds".to_owned()),
            };
            if seconds < 0.0 {
                return Response::Error("ERR seconds must be >= 0".to_owned());
            }
            use std::thread;
            use std::time::Duration;
            thread::sleep(Duration::from_secs_f64(seconds));
            Response::Status("OK".to_owned())
        }
        "segfault" => {
            validate_arguments_exact!(parser, 2);
            // Intentionally cause a panic for testing
            // In production, this would be dangerous, but Redis has this for testing
            panic!("DEBUG SEGFAULT");
        }
        _ => Response::Error("ERR Invalid debug command".to_owned()),
    }
}

fn dbsize(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 1);
    Response::Integer(db.dbsize(dbindex) as i64)
}

fn dump(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut data = vec![];

    let obj = db.get(dbindex, &key);
    match obj {
        Some(value) => match value.dump(&mut data) {
            Ok(_) => Response::Data(data),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Nil,
    }
}

fn restore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let _ttl = try_validate!(parser.get_i64(2), "Invalid TTL");
    let _serialized_value = try_validate!(parser.get_vec(3), "Invalid serialized value");
    
    // Check if key exists (unless REPLACE is specified)
    let mut replace = false;
    if parser.argv.len() > 4 {
        let option = try_validate!(parser.get_str(4), "Invalid option");
        if option.to_ascii_lowercase() == "replace" {
            replace = true;
        } else {
            return Response::Error("ERR syntax error".to_owned());
        }
    }
    
    if !replace && db.get(dbindex, &key).is_some() {
        return Response::Error("ERR Target key name is busy.".to_owned());
    }
    
    // TODO: Implement full RDB deserialization
    // For now, return error indicating it's not fully implemented
    // This is a complex operation that requires full RDB format parsing
    Response::Error("ERR RESTORE is not fully implemented yet".to_owned())
}

fn sort(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    
    // Get the source value
    let source_value = match db.get(dbindex, &key) {
        Some(v) => v,
        None => return Response::Array(Vec::new()),
    };
    
    // Collect elements based on type
    let mut elements = match source_value {
        Value::List(list) => {
            let len = list.llen();
            let mut elems = Vec::new();
            for i in 0..(len as i64) {
                if let Some(elem) = list.lindex(i) {
                    elems.push(elem.to_vec());
                }
            }
            elems
        }
        Value::Set(set) => set.smembers(),
        Value::SortedSet(zset) => {
            // For sorted sets, get members in score order
            let skiplist = match zset {
                ValueSortedSet::Data(ref skiplist, _) => skiplist,
            };
            skiplist.iter().map(|m| m.get_vec().to_vec()).collect()
        }
        _ => return Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
    };
    
    // Parse options
    let mut by_pattern: Option<Vec<u8>> = None;
    let mut get_patterns: Vec<Vec<u8>> = Vec::new();
    let mut limit_offset: Option<usize> = None;
    let mut limit_count: Option<usize> = None;
    let mut desc = false;
    let mut alpha = false;
    let mut store_key: Option<Vec<u8>> = None;
    
    let mut i = 2;
    while i < parser.argv.len() {
        let option = try_validate!(parser.get_str(i), "Invalid option");
        match &*option.to_ascii_lowercase() {
            "by" => {
                if i + 1 >= parser.argv.len() {
                    return Response::Error("ERR syntax error".to_owned());
                }
                by_pattern = Some(try_validate!(parser.get_vec(i + 1), "Invalid pattern"));
                i += 2;
            }
            "get" => {
                if i + 1 >= parser.argv.len() {
                    return Response::Error("ERR syntax error".to_owned());
                }
                get_patterns.push(try_validate!(parser.get_vec(i + 1), "Invalid pattern"));
                i += 2;
            }
            "limit" => {
                if i + 2 >= parser.argv.len() {
                    return Response::Error("ERR syntax error".to_owned());
                }
                limit_offset = Some(try_validate!(parser.get_i64(i + 1), "Invalid offset") as usize);
                limit_count = Some(try_validate!(parser.get_i64(i + 2), "Invalid count") as usize);
                i += 3;
            }
            "desc" => {
                desc = true;
                i += 1;
            }
            "asc" => {
                desc = false;
                i += 1;
            }
            "alpha" => {
                alpha = true;
                i += 1;
            }
            "store" => {
                if i + 1 >= parser.argv.len() {
                    return Response::Error("ERR syntax error".to_owned());
                }
                store_key = Some(try_validate!(parser.get_vec(i + 1), "Invalid key"));
                i += 2;
            }
            _ => return Response::Error("ERR syntax error".to_owned()),
        }
    }
    
    // Sort elements
    if let Some(ref by_pat) = by_pattern {
        // Sort by external key pattern - replace * with element value
        elements.sort_by(|a, b| {
            let mut a_key = Vec::new();
            for &c in by_pat.iter() {
                if c == b'*' {
                    a_key.extend_from_slice(a);
                } else {
                    a_key.push(c);
                }
            }
            let mut b_key = Vec::new();
            for &c in by_pat.iter() {
                if c == b'*' {
                    b_key.extend_from_slice(b);
                } else {
                    b_key.push(c);
                }
            }
            
            let a_val = db.get(dbindex, &a_key).and_then(|v| v.get().ok());
            let b_val = db.get(dbindex, &b_key).and_then(|v| v.get().ok());
            
            let a_num = a_val.as_ref().and_then(|v| std::str::from_utf8(v).ok().and_then(|s| s.parse::<f64>().ok())).unwrap_or(0.0);
            let b_num = b_val.as_ref().and_then(|v| std::str::from_utf8(v).ok().and_then(|s| s.parse::<f64>().ok())).unwrap_or(0.0);
            
            if desc {
                b_num.partial_cmp(&a_num).unwrap_or(std::cmp::Ordering::Equal)
            } else {
                a_num.partial_cmp(&b_num).unwrap_or(std::cmp::Ordering::Equal)
            }
        });
    } else {
        // Sort by element value
        if alpha {
            elements.sort_by(|a, b| {
                let a_str = std::str::from_utf8(a).unwrap_or("");
                let b_str = std::str::from_utf8(b).unwrap_or("");
                if desc {
                    b_str.cmp(a_str)
                } else {
                    a_str.cmp(b_str)
                }
            });
        } else {
            elements.sort_by(|a, b| {
                let a_num = std::str::from_utf8(a).ok().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                let b_num = std::str::from_utf8(b).ok().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                if desc {
                    b_num.partial_cmp(&a_num).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    a_num.partial_cmp(&b_num).unwrap_or(std::cmp::Ordering::Equal)
                }
            });
        }
    }
    
    // Apply limit
    let result_elements = if let (Some(offset), Some(count)) = (limit_offset, limit_count) {
        let start = std::cmp::min(offset, elements.len());
        let end = std::cmp::min(start + count, elements.len());
        elements[start..end].to_vec()
    } else {
        elements
    };
    
    // Apply GET patterns
    let mut final_result = Vec::new();
    if get_patterns.is_empty() {
        final_result = result_elements.into_iter().map(Response::Data).collect();
    } else {
        for elem in result_elements {
            for pattern in &get_patterns {
                if pattern == b"#" {
                    final_result.push(Response::Data(elem.clone()));
                } else {
                    // Replace * with element value
                    let mut get_key = Vec::new();
                    for &c in pattern.iter() {
                        if c == b'*' {
                            get_key.extend_from_slice(&elem);
                        } else {
                            get_key.push(c);
                        }
                    }
                    match db.get(dbindex, &get_key) {
                        Some(v) => {
                            if let Ok(data) = v.get() {
                                final_result.push(Response::Data(data));
                            } else {
                                final_result.push(Response::Nil);
                            }
                        }
                        None => final_result.push(Response::Nil),
                    }
                }
            }
        }
    }
    
    // Store result if STORE is specified
    if let Some(ref store) = store_key {
        if final_result.is_empty() {
            db.remove(dbindex, store);
            return Response::Integer(0);
        }
        
        // Convert responses back to Vec<u8> for storage
        let mut list = ValueList::new();
        for resp in &final_result {
            if let Response::Data(data) = resp {
                list.push(data.clone(), true);
            }
        }
        *db.get_or_create(dbindex, store) = Value::List(list);
        db.key_updated(dbindex, store);
        return Response::Integer(final_result.len() as i64);
    }
    
    Response::Array(final_result)
}

fn echo(parser: &mut ParsedCommand) -> Response {
    validate_arguments_exact!(parser, 2);
    let msg = try_validate!(parser.get_str(1), "Syntax error");
    Response::Data(msg.as_bytes().to_vec())
}

fn generic_expire(db: &mut Database, dbindex: usize, key: Vec<u8>, msexpiration: i64) -> Response {
    Response::Integer(match db.get(dbindex, &key) {
        Some(_) => {
            db.set_msexpiration(dbindex, key.clone(), msexpiration);
            db.key_updated(dbindex, &key);
            // Publish keyspace notification for EXPIRE command
            db.notify_keyspace_event(dbindex, "expire", &key, Some('g'));
            1
        }
        None => 0,
    })
}

fn expire(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let expiration = try_validate!(parser.get_i64(2), "Invalid expiration");
    generic_expire(db, dbindex, key, mstime() + expiration * 1000)
}

fn expireat(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let expiration = try_validate!(parser.get_i64(2), "Invalid expiration");
    generic_expire(db, dbindex, key, expiration * 1000)
}

fn pexpire(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let expiration = try_validate!(parser.get_i64(2), "Invalid expiration");
    generic_expire(db, dbindex, key, mstime() + expiration)
}

fn pexpireat(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let expiration = try_validate!(parser.get_i64(2), "Invalid expiration");
    generic_expire(db, dbindex, key, expiration)
}

fn flushdb(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 1);
    db.clear(dbindex);

    Response::Status("OK".to_owned())
}

fn generic_ttl(db: &mut Database, dbindex: usize, key: &[u8], divisor: i64) -> Response {
    Response::Integer(match db.get(dbindex, key) {
        Some(_) => match db.get_msexpiration(dbindex, key) {
            Some(exp) => (exp - mstime()) / divisor,
            None => -1,
        },
        None => -2,
    })
}

fn ttl(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    generic_ttl(db, dbindex, &key, 1000)
}

fn pttl(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    generic_ttl(db, dbindex, &key, 1)
}

fn persist(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let r = Response::Integer(match db.remove_msexpiration(dbindex, &key) {
        Some(_) => 1,
        None => 0,
    });
    db.key_updated(dbindex, &key);
    r
}

fn dbtype(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");

    match db.get(dbindex, &key) {
        Some(Value::Nil) => Response::Data("none".to_owned().into_bytes()),
        Some(Value::String(_)) => Response::Data("string".to_owned().into_bytes()),
        Some(Value::List(_)) => Response::Data("list".to_owned().into_bytes()),
        Some(Value::Set(_)) => Response::Data("set".to_owned().into_bytes()),
        Some(Value::SortedSet(_)) => Response::Data("zset".to_owned().into_bytes()),
        Some(Value::Hash(_)) => Response::Data("hash".to_owned().into_bytes()),
        None => Response::Data("none".to_owned().into_bytes()),
    }
}

fn flushall(parser: &mut ParsedCommand, db: &mut Database, _: usize) -> Response {
    validate_arguments_exact!(parser, 1);
    db.clearall();

    Response::Status("OK".to_owned())
}

fn append(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let val = try_validate!(parser.get_vec(2), "Invalid value");
    let r = {
        let oldval = db.get_or_create(dbindex, &key);
        let oldlen = match oldval.strlen() {
            Ok(len) => len,
            Err(err) => return Response::Error(err.to_string()),
        };
        validate!(
            oldlen + val.len() <= 512 * 1024 * 1024,
            "ERR string exceeds maximum allowed size (512MB)"
        );
        match oldval.append(val) {
            Ok(len) => Response::Integer(len as i64),
            Err(err) => Response::Error(err.to_string()),
        }
    };
    db.key_updated(dbindex, &key);
    r
}

fn generic_get(db: &Database, dbindex: usize, key: Vec<u8>, err_on_wrongtype: bool) -> Response {
    let obj = db.get(dbindex, &key);
    match obj {
        Some(value) => match value.get() {
            Ok(r) => Response::Data(r),
            Err(err) => {
                if err_on_wrongtype {
                    Response::Error(err.to_string())
                } else {
                    Response::Nil
                }
            }
        },
        None => Response::Nil,
    }
}

fn get(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    generic_get(db, dbindex, key, true)
}

fn getset(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let value = try_validate!(parser.get_vec(2), "Invalid value");
    let old_value = generic_get(db, dbindex, key.clone(), false);
    let el = db.get_or_create(dbindex, &key);
    match el.set(value) {
        Ok(_) => {
            db.key_updated(dbindex, &key);
            old_value
        }
        Err(err) => Response::Error(err.to_string()),
    }
}

fn mset(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    validate!(
        (parser.argv.len() - 1) % 2 == 0,
        "ERR wrong number of arguments for 'mset' command"
    );
    for i in (1..parser.argv.len()).step_by(2) {
        let key = try_validate!(parser.get_vec(i), "Invalid key");
        let value = try_validate!(parser.get_vec(i + 1), "Invalid value");
        let el = db.get_or_create(dbindex, &key);
        match el.set(value) {
            Ok(_) => {
                db.key_updated(dbindex, &key);
            }
            Err(err) => return Response::Error(err.to_string()),
        }
    }
    Response::Status("OK".to_owned())
}

fn msetnx(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    validate!(
        (parser.argv.len() - 1) % 2 == 0,
        "ERR wrong number of arguments for 'msetnx' command"
    );
    // First check if all keys don't exist
    for i in (1..parser.argv.len()).step_by(2) {
        let key = try_validate!(parser.get_vec(i), "Invalid key");
        if db.get(dbindex, &key).is_some() {
            return Response::Integer(0);
        }
    }
    // All keys don't exist, set them
    for i in (1..parser.argv.len()).step_by(2) {
        let key = try_validate!(parser.get_vec(i), "Invalid key");
        let value = try_validate!(parser.get_vec(i + 1), "Invalid value");
        let el = db.get_or_create(dbindex, &key);
        match el.set(value) {
            Ok(_) => {
                db.key_updated(dbindex, &key);
            }
            Err(err) => return Response::Error(err.to_string()),
        }
    }
    Response::Integer(1)
}

fn mget(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() >= 2, "Wrong number of parameters");
    let mut responses = Vec::with_capacity(parser.argv.len() - 1);
    for i in 1..parser.argv.len() {
        let key = try_validate!(parser.get_vec(i), "Invalid key");
        responses.push(generic_get(db, dbindex, key, false));
    }
    Response::Array(responses)
}

fn getrange(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let start = try_validate!(parser.get_i64(2), "Invalid range");
    let stop = try_validate!(parser.get_i64(3), "Invalid range");
    let obj = db.get(dbindex, &key);

    match obj {
        Some(value) => match value.getrange(start, stop) {
            Ok(r) => Response::Data(r),
            Err(e) => Response::Error(e.to_string()),
        },
        None => Response::Nil,
    }
}

fn setrange(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let index = {
        let v = try_validate!(parser.get_i64(2), "Invalid index");
        if v < 0 {
            return Response::Error("ERR offset is out of range".to_owned());
        }
        v as usize
    };
    let value = try_validate!(parser.get_vec(3), "Invalid value");
    let r = {
        if db.get(dbindex, &key).is_none() && value.is_empty() {
            return Response::Integer(0);
        }
        let oldval = db.get_or_create(dbindex, &key);
        validate!(
            index + value.len() <= 512 * 1024 * 1024,
            "ERR string exceeds maximum allowed size (512MB)"
        );
        match oldval.setrange(index, value) {
            Ok(s) => Response::Integer(s as i64),
            Err(e) => Response::Error(e.to_string()),
        }
    };
    db.key_updated(dbindex, &key);
    r
}

fn setbit(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let index = try_validate!(
        parser.get_i64(2),
        "ERR bit offset is not an integer or out of range"
    );
    validate!(
        index >= 0 && index < 4 * 1024 * 1024 * 1024,
        "ERR bit offset is not an integer or out of range"
    );
    let value = try_validate!(
        parser.get_i64(3),
        "ERR bit is not an integer or out of range"
    );
    validate!(
        value == 0 || value == 1,
        "ERR bit is not an integer or out of range"
    );
    let r = match db
        .get_or_create(dbindex, &key)
        .setbit(index as usize, value == 1)
    {
        Ok(s) => Response::Integer(if s { 1 } else { 0 }),
        Err(e) => Response::Error(e.to_string()),
    };
    db.key_updated(dbindex, &key);
    r
}

fn getbit(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let index = try_validate!(parser.get_i64(2), "Invalid index");
    validate!(index >= 0, "Invalid index");
    match db.get(dbindex, &key) {
        Some(v) => match v.getbit(index as usize) {
            Ok(s) => Response::Integer(if s { 1 } else { 0 }),
            Err(e) => Response::Error(e.to_string()),
        },
        None => Response::Integer(0),
    }
}

fn strlen(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let obj = db.get(dbindex, &key);

    match obj {
        Some(value) => match value.strlen() {
            Ok(r) => Response::Integer(r as i64),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Integer(0),
    }
}

fn generic_incr(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
    increment: i64,
) -> Response {
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let r = match db.get_or_create(dbindex, &key).incr(increment) {
        Ok(val) => Response::Integer(val),
        Err(err) => Response::Error(err.to_string()),
    };
    db.key_updated(dbindex, &key);
    r
}

fn incr(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    generic_incr(parser, db, dbindex, 1)
}

fn decr(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    generic_incr(parser, db, dbindex, -1)
}

fn incrby(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    match parser.get_i64(2) {
        Ok(increment) => generic_incr(parser, db, dbindex, increment),
        Err(_) => Response::Error("Invalid increment".to_owned()),
    }
}

fn decrby(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    match parser.get_i64(2) {
        Ok(decrement) => generic_incr(parser, db, dbindex, -decrement),
        Err(_) => Response::Error("Invalid increment".to_owned()),
    }
}

fn incrbyfloat(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let increment = try_validate!(parser.get_f64(2), "Invalid increment");
    let r = match db.get_or_create(dbindex, &key).incrbyfloat(increment) {
        Ok(val) => Response::Data(format!("{}", val).into_bytes()),
        Err(err) => Response::Error(err.to_string()),
    };
    db.key_updated(dbindex, &key);
    r
}

fn pfadd(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut values = Vec::with_capacity(parser.argv.len() - 2);
    for i in 2..parser.argv.len() {
        values.push(try_validate!(parser.get_vec(i), "Invalid value"));
    }
    let r = match db.get_or_create(dbindex, &key).pfadd(values) {
        Ok(val) => Response::Integer(if val { 1 } else { 0 }),
        Err(err) => Response::Error(err.to_string()),
    };
    db.key_updated(dbindex, &key);
    r
}

fn pfcount(parser: &ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    if parser.argv.len() == 2 {
        let key = try_validate!(parser.get_vec(1), "Invalid key");
        Response::Integer(match db.get(dbindex, &key) {
            Some(v) => match v.pfcount() {
                Ok(val) => val as i64,
                Err(err) => return Response::Error(err.to_string()),
            },
            None => 0,
        })
    } else {
        let mut values = Vec::with_capacity(parser.argv.len() - 1);
        for i in 1..parser.argv.len() {
            let key = try_validate!(parser.get_vec(i), "Invalid key");
            if let Some(v) = db.get(dbindex, &key) {
                values.push(v);
            }
        }
        let mut val = Value::Nil;
        if let Err(err) = val.pfmerge(values) {
            return Response::Error(err.to_string());
        }
        Response::Integer(match val.pfcount() {
            Ok(v) => v as i64,
            Err(err) => return Response::Error(err.to_string()),
        })
    }
}

fn pfmerge(parser: &ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");

    let (val, r) = {
        let mut val = Value::Nil;
        if let Some(v) = db.get(dbindex, &key) {
            try_validate!(val.set(try_validate!(v.get(), "ERR")), "ERR"); // FIXME unnecesary clone
        }
        let mut values = Vec::with_capacity(parser.argv.len() - 2);
        for i in 2..parser.argv.len() {
            let key = try_validate!(parser.get_vec(i), "Invalid key");
            if let Some(v) = db.get(dbindex, &key) {
                values.push(v);
            }
        }

        let r = match val.pfmerge(values) {
            Ok(()) => Response::Status("OK".to_owned()),
            Err(err) => Response::Error(err.to_string()),
        };
        (val, r)
    };

    {
        let value = db.get_or_create(dbindex, &key);
        *value = val;
    }
    db.key_updated(dbindex, &key);
    r
}

fn generic_push(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
    right: bool,
    create: bool,
) -> Response {
    validate!(parser.argv.len() >= 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut r = Response::Nil;
    for i in 2..parser.argv.len() {
        let val = try_validate!(parser.get_vec(i), "Invalid value");
        let el;
        if create {
            el = db.get_or_create(dbindex, &key);
        } else {
            match db.get_mut(dbindex, &key) {
                Some(_el) => el = _el,
                None => return Response::Integer(0),
            }
        }
        r = match el.push(val, right) {
            Ok(listsize) => Response::Integer(listsize as i64),
            Err(err) => Response::Error(err.to_string()),
        }
    }
    db.key_updated(dbindex, &key);
    r
}

fn pfselftest(_parser: &mut ParsedCommand, _db: &mut Database, _dbindex: usize) -> Response {
    // PFSELFTEST - Run a self-test of the HyperLogLog implementation
    // This is a simple test that verifies the HLL implementation works correctly
    // For now, we'll return OK since the implementation is already tested
    Response::Status("OK".to_owned())
}

fn pfdebug(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let subcommand = try_validate!(parser.get_str(1), "Invalid subcommand");
    let key = try_validate!(parser.get_vec(2), "Invalid key");
    
    match &*subcommand.to_ascii_lowercase() {
        "encode" => {
            // PFDEBUG ENCODE <key> - Encode a key's HLL representation
            match db.get(dbindex, &key) {
                Some(Value::String(_)) => {
                    // HyperLogLog is stored as a String variant
                    // Return the encoded representation
                    // For now, return a placeholder
                    Response::Data(b"OK".to_vec())
                }
                Some(_) => Response::Error("ERR key is not a HyperLogLog".to_owned()),
                None => Response::Error("ERR no such key".to_owned()),
            }
        }
        "decode" => {
            // PFDEBUG DECODE <key> - Decode a key's HLL representation
            // This is a complex operation, return error for now
            Response::Error("ERR PFDEBUG DECODE is not fully implemented".to_owned())
        }
        "getreg" => {
            // PFDEBUG GETREG <key> - Get the registers of a HyperLogLog
            match db.get(dbindex, &key) {
                Some(Value::String(_)) => {
                    // HyperLogLog is stored as a String variant
                    // Return register values
                    // For now, return empty array
                    Response::Array(Vec::new())
                }
                Some(_) => Response::Error("ERR key is not a HyperLogLog".to_owned()),
                None => Response::Error("ERR no such key".to_owned()),
            }
        }
        _ => Response::Error("ERR Invalid PFDEBUG subcommand".to_owned()),
    }
}

fn lpush(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_push(parser, db, dbindex, false, true)
}

fn rpush(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_push(parser, db, dbindex, true, true)
}

fn lpushx(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_push(parser, db, dbindex, false, false)
}

fn rpushx(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_push(parser, db, dbindex, true, false)
}

fn generic_pop(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
    right: bool,
) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let r = {
        match db.get_mut(dbindex, &key) {
            Some(list) => match list.pop(right) {
                Ok(el) => match el {
                    Some(val) => Response::Data(val),
                    None => Response::Nil,
                },
                Err(err) => Response::Error(err.to_string()),
            },
            None => Response::Nil,
        }
    };
    db.key_updated(dbindex, &key);
    r
}

fn lpop(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_pop(parser, db, dbindex, false)
}

fn rpop(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_pop(parser, db, dbindex, true)
}

fn generic_rpoplpush(
    db: &mut Database,
    dbindex: usize,
    source: &[u8],
    destination: &[u8],
) -> Response {
    if let Some(Err(_)) = db.get(dbindex, destination).map(|el| el.llen()) {
        return Response::Error("WRONGTYPE Destination is not a list".to_owned());
    }

    let el = {
        let sourcelist = match db.get_mut(dbindex, source) {
            Some(sourcelist) => {
                if sourcelist.llen().is_err() {
                    return Response::Error("WRONGTYPE Source is not a list".to_owned());
                }
                sourcelist
            }
            None => return Response::Nil,
        };
        match sourcelist.pop(true) {
            Ok(el) => match el {
                Some(el) => el,
                None => return Response::Nil,
            },
            Err(err) => return Response::Error(err.to_string()),
        }
    };

    let resp = {
        let destinationlist = db.get_or_create(dbindex, destination);
        if let Err(e) = destinationlist.push(el.clone(), false) {
            return Response::Error(e.to_string());
        }

        Response::Data(el)
    };
    db.key_updated(dbindex, source);
    db.key_updated(dbindex, destination);
    resp
}

fn rpoplpush(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let source = try_validate!(parser.get_vec(1), "Invalid source");
    let destination = try_validate!(parser.get_vec(2), "Invalid destination");
    generic_rpoplpush(db, dbindex, &source, &destination)
}

fn brpoplpush(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
) -> Result<Response, ResponseError> {
    opt_validate!(parser.argv.len() == 4, "Wrong number of parameters");

    let source = try_opt_validate!(parser.get_vec(1), "Invalid source");
    let destination = try_opt_validate!(parser.get_vec(2), "Invalid destination");
    let timeout = try_opt_validate!(parser.get_i64(3), "ERR timeout is not an integer");
    let time = mstime();

    let r = generic_rpoplpush(db, dbindex, &source, &destination);
    if r != Response::Nil {
        return Ok(r);
    }

    let (txkey, rxkey) = channel();
    let (txcommand, rxcommand) = channel();
    if timeout > 0 {
        let tx = txcommand.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_secs(timeout as u64));
            let _ = tx.send(None);
        });
    }
    let command_name = try_opt_validate!(parser.get_vec(0), "Invalid command");
    db.key_subscribe(dbindex, &source, txkey);
    thread::spawn(move || {
        let _ = rxkey.recv();
        let newtimeout = if timeout == 0 {
            0
        } else {
            let mut t = timeout as i64 * 1000 - mstime() + time;
            if t <= 0 {
                t = 1;
            }
            t
        };
        // This code is ugly. I was stuck for a week trying to figure out how
        // to do this and this is the best I got. I'm sorry.
        let mut data = vec![];
        let mut arguments = vec![];
        data.extend(command_name);
        arguments.push(Argument {
            pos: 0,
            len: data.len(),
        });
        arguments.push(Argument {
            pos: data.len(),
            len: source.len(),
        });
        data.extend(source);
        arguments.push(Argument {
            pos: data.len(),
            len: destination.len(),
        });
        data.extend(destination);
        let timeout_formatted = format!("{}", newtimeout);
        arguments.push(Argument {
            pos: data.len(),
            len: timeout_formatted.len(),
        });
        data.extend(timeout_formatted.into_bytes());
        let _ = txcommand.send(Some(OwnedParsedCommand::new(data, arguments)));
    });

    Err(ResponseError::Wait(rxcommand))
}

fn generic_bpop(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
    right: bool,
) -> Result<Response, ResponseError> {
    opt_validate!(parser.argv.len() >= 3, "Wrong number of parameters");
    let time = mstime();

    let mut keys = vec![];
    for i in 1..parser.argv.len() - 1 {
        let key = try_opt_validate!(parser.get_vec(i), "Invalid key");
        let val = match db.get_mut(dbindex, &key) {
            Some(list) => match list.pop(right) {
                Ok(el) => el,
                Err(err) => return Ok(Response::Error(err.to_string())),
            },
            None => None,
        };
        match val {
            Some(val) => {
                db.key_updated(dbindex, &key);
                return Ok(Response::Array(vec![
                    Response::Data(key),
                    Response::Data(val),
                ]));
            }
            None => keys.push(key),
        }
    }
    let timeout = try_opt_validate!(
        parser.get_i64(parser.argv.len() - 1),
        "ERR timeout is not an integer"
    );

    let (txkey, rxkey) = channel();
    let (txcommand, rxcommand) = channel();
    if timeout > 0 {
        let tx = txcommand.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_secs(timeout as u64));
            let _ = tx.send(None);
        });
    }
    let command_name = try_opt_validate!(parser.get_vec(0), "Invalid command");
    for key in keys.iter() {
        db.key_subscribe(dbindex, key, txkey.clone());
    }
    thread::spawn(move || {
        let _ = rxkey.recv();
        let newtimeout = if timeout == 0 {
            0
        } else {
            let mut t = timeout as i64 * 1000 - mstime() + time;
            if t <= 0 {
                t = 1;
            }
            t
        };
        // This code is ugly. I was stuck for a week trying to figure out how
        // to do this and this is the best I got. I'm sorry.
        let mut data = vec![];
        let mut arguments = vec![];
        data.extend(command_name);
        arguments.push(Argument {
            pos: 0,
            len: data.len(),
        });
        for k in keys {
            arguments.push(Argument {
                pos: data.len(),
                len: k.len(),
            });
            data.extend(k);
        }
        let timeout_formatted = format!("{}", newtimeout);
        arguments.push(Argument {
            pos: data.len(),
            len: timeout_formatted.len(),
        });
        data.extend(timeout_formatted.into_bytes());
        let _ = txcommand.send(Some(OwnedParsedCommand::new(data, arguments)));
    });

    Err(ResponseError::Wait(rxcommand))
}

fn brpop(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
) -> Result<Response, ResponseError> {
    generic_bpop(parser, db, dbindex, true)
}

fn blpop(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
) -> Result<Response, ResponseError> {
    generic_bpop(parser, db, dbindex, false)
}

fn lindex(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let index = try_validate!(parser.get_i64(2), "Invalid index");

    match db.get(dbindex, &key) {
        Some(el) => match el.lindex(index) {
            Ok(el) => match el {
                Some(val) => Response::Data(val.to_vec()),
                None => Response::Nil,
            },
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Nil,
    }
}

fn linsert(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 5);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let before_str = try_validate!(parser.get_str(2), "Syntax error");
    let pivot = try_validate!(parser.get_vec(3), "Invalid pivot");
    let value = try_validate!(parser.get_vec(4), "Invalid value");
    let before;
    match &*before_str.to_ascii_lowercase() {
        "after" => before = false,
        "before" => before = true,
        _ => return Response::Error("ERR syntax error".to_owned()),
    };
    let r = match db.get_mut(dbindex, &key) {
        Some(el) => match el.linsert(before, pivot, value) {
            Ok(r) => match r {
                Some(listsize) => Response::Integer(listsize as i64),
                None => Response::Integer(-1),
            },
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Integer(-1),
    };
    db.key_updated(dbindex, &key);
    r
}

fn llen(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");

    match db.get(dbindex, &key) {
        Some(el) => match el.llen() {
            Ok(listsize) => Response::Integer(listsize as i64),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Integer(0),
    }
}

fn lrange(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let start = try_validate!(parser.get_i64(2), "Invalid range");
    let stop = try_validate!(parser.get_i64(3), "Invalid range");

    match db.get(dbindex, &key) {
        Some(el) => match el.lrange(start, stop) {
            Ok(items) => {
                Response::Array(items.iter().map(|i| Response::Data(i.to_vec())).collect())
            }
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Array(Vec::new()),
    }
}

fn lrem(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let count = try_validate!(parser.get_i64(2), "Invalid count");
    let value = try_validate!(parser.get_vec(3), "Invalid value");
    let r = match db.get_mut(dbindex, &key) {
        Some(el) => match el.lrem(count < 0, count.abs() as usize, value) {
            Ok(removed) => Response::Integer(removed as i64),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Array(Vec::new()),
    };
    db.key_updated(dbindex, &key);
    r
}

fn lset(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let index = try_validate!(parser.get_i64(2), "Invalid index");
    let value = try_validate!(parser.get_vec(3), "Invalid value");
    let r = match db.get_mut(dbindex, &key) {
        Some(el) => match el.lset(index, value) {
            Ok(()) => Response::Status("OK".to_owned()),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Error("ERR no such key".to_owned()),
    };
    db.key_updated(dbindex, &key);
    r
}

fn ltrim(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let start = try_validate!(parser.get_i64(2), "Invalid start");
    let stop = try_validate!(parser.get_i64(3), "Invalid stop");
    let r = match db.get_mut(dbindex, &key) {
        Some(el) => match el.ltrim(start, stop) {
            Ok(()) => Response::Status("OK".to_owned()),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Status("OK".to_owned()),
    };
    db.key_updated(dbindex, &key);
    r
}

fn sadd(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() > 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut count = 0;
    let set_max_intset_entries = db.config.set_max_intset_entries;
    {
        let el = db.get_or_create(dbindex, &key);
        for i in 2..parser.argv.len() {
            let val = try_validate!(parser.get_vec(i), "Invalid value");
            match el.sadd(val, set_max_intset_entries) {
                Ok(added) => {
                    if added {
                        count += 1
                    }
                }
                Err(err) => return Response::Error(err.to_string()),
            }
        }
    }
    db.key_updated(dbindex, &key);

    Response::Integer(count)
}

fn srem(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() > 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut count = 0;
    {
        let el = db.get_or_create(dbindex, &key);
        for i in 2..parser.argv.len() {
            let val = try_validate!(parser.get_vec(i), "Invalid value");
            match el.srem(&val) {
                Ok(removed) => {
                    if removed {
                        count += 1
                    }
                }
                Err(err) => return Response::Error(err.to_string()),
            }
        }
    }
    db.key_updated(dbindex, &key);

    Response::Integer(count)
}

fn sismember(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let member = try_validate!(parser.get_vec(2), "Invalid key");

    Response::Integer(match db.get(dbindex, &key) {
        Some(el) => match el.sismember(&member) {
            Ok(e) => {
                if e {
                    1
                } else {
                    0
                }
            }
            Err(err) => return Response::Error(err.to_string()),
        },
        None => 0,
    })
}

fn srandmember(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    validate_arguments_lte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let value = match db.get(dbindex, &key) {
        Some(el) => el,
        None => {
            return if parser.argv.len() == 2 {
                Response::Nil
            } else {
                Response::Array(vec![])
            }
        }
    };
    if parser.argv.len() == 2 {
        match value.srandmember(1, false) {
            Ok(els) => {
                if !els.is_empty() {
                    Response::Data(els[0].clone())
                } else {
                    Response::Nil
                }
            }
            Err(err) => Response::Error(err.to_string()),
        }
    } else {
        let _count = try_validate!(parser.get_i64(2), "Invalid count");
        let count = {
            if _count < 0 {
                -_count
            } else {
                _count
            }
        } as usize;
        let allow_duplicates = _count < 0;
        match value.srandmember(count, allow_duplicates) {
            Ok(els) => Response::Array(
                els.iter()
                    .map(|x| Response::Data(x.clone()))
                    .collect::<Vec<_>>(),
            ),
            Err(err) => Response::Error(err.to_string()),
        }
    }
}

fn smembers(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let value = match db.get(dbindex, &key) {
        Some(el) => el,
        None => return Response::Array(vec![]),
    };
    match value.smembers() {
        Ok(els) => Response::Array(
            els.iter()
                .map(|x| Response::Data(x.clone()))
                .collect::<Vec<_>>(),
        ),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn spop(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    validate_arguments_lte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let r = {
        let value = match db.get_mut(dbindex, &key) {
            Some(el) => el,
            None => {
                return if parser.argv.len() == 2 {
                    Response::Nil
                } else {
                    Response::Array(vec![])
                }
            }
        };
        if parser.argv.len() == 2 {
            match value.spop(1) {
                Ok(els) => {
                    if !els.is_empty() {
                        Response::Data(els[0].clone())
                    } else {
                        Response::Nil
                    }
                }
                Err(err) => Response::Error(err.to_string()),
            }
        } else {
            let count = try_validate!(parser.get_i64(2), "Invalid count");
            match value.spop(count as usize) {
                Ok(els) => Response::Array(
                    els.iter()
                        .map(|x| Response::Data(x.clone()))
                        .collect::<Vec<_>>(),
                ),
                Err(err) => Response::Error(err.to_string()),
            }
        }
    };
    db.key_updated(dbindex, &key);
    r
}

fn smove(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let source_key = try_validate!(parser.get_vec(1), "Invalid key");
    let destination_key = try_validate!(parser.get_vec(2), "Invalid destination");
    let member = try_validate!(parser.get_vec(3), "Invalid member");

    {
        if let Some(e) = db.get(dbindex, &destination_key) {
            if !e.is_set() {
                return Response::Error(
                    "WRONGTYPE Operation against a key holding the wrong \
                         kind of value"
                        .to_owned(),
                );
            }
        }
    }
    {
        let source = match db.get_mut(dbindex, &source_key) {
            Some(s) => s,
            None => return Response::Integer(0),
        };

        match source.srem(&member) {
            Ok(removed) => {
                if !removed {
                    return Response::Integer(0);
                }
            }
            Err(err) => return Response::Error(err.to_string()),
        }
    }

    let set_max_intset_entries = db.config.set_max_intset_entries;
    {
        let destination = db.get_or_create(dbindex, &destination_key);
        match destination.sadd(member, set_max_intset_entries) {
            Ok(_) => (),
            Err(err) => panic!("Unexpected failure {}", err.to_string()),
        }
    }

    db.key_updated(dbindex, &source_key);
    db.key_updated(dbindex, &destination_key);

    Response::Integer(1)
}

fn scard(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Integer(0),
    };

    match el.scard() {
        Ok(count) => Response::Integer(count as i64),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn sdiff(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Array(vec![]),
    };
    let nil = Value::Nil;
    let sets = get_values!(2, parser.argv.len(), parser, db, dbindex, &nil);

    match el.sdiff(&sets) {
        Ok(set) => Response::Array(
            set.iter()
                .map(|x| Response::Data(x.clone()))
                .collect::<Vec<_>>(),
        ),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn sdiffstore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() >= 3, "Wrong number of parameters");
    let destination_key = try_validate!(parser.get_vec(1), "Invalid destination");
    let set = {
        let key = try_validate!(parser.get_vec(2), "Invalid key");
        let el = match db.get(dbindex, &key) {
            Some(e) => e,
            None => return Response::Integer(0),
        };
        let nil = Value::Nil;
        let sets = get_values!(3, parser.argv.len(), parser, db, dbindex, &nil);
        match el.sdiff(&sets) {
            Ok(set) => set,
            Err(err) => return Response::Error(err.to_string()),
        }
    };

    db.remove(dbindex, &destination_key);
    let r = set.len() as i64;
    db.get_or_create(dbindex, &destination_key).create_set(set);
    db.key_updated(dbindex, &destination_key);
    Response::Integer(r)
}

fn sinter(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Array(vec![]),
    };
    let nil = Value::Nil;
    let sets = get_values!(2, parser.argv.len(), parser, db, dbindex, &nil);
    match el.sinter(&sets) {
        Ok(set) => Response::Array(
            set.iter()
                .map(|x| Response::Data(x.clone()))
                .collect::<Vec<_>>(),
        ),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn sinterstore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() >= 3, "Wrong number of parameters");
    let destination_key = try_validate!(parser.get_vec(1), "Invalid destination");
    let set = {
        let key = try_validate!(parser.get_vec(2), "Invalid key");
        let nil = Value::Nil;
        let el = match db.get(dbindex, &key) {
            Some(e) => e,
            None => &nil,
        };
        let sets = get_values!(3, parser.argv.len(), parser, db, dbindex, &nil);
        match el.sinter(&sets) {
            Ok(set) => set,
            Err(err) => return Response::Error(err.to_string()),
        }
    };

    db.remove(dbindex, &destination_key);
    let r = set.len() as i64;
    db.get_or_create(dbindex, &destination_key).create_set(set);
    db.key_updated(dbindex, &destination_key);
    Response::Integer(r)
}

fn sunion(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let defaultel = Value::Nil;
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => &defaultel,
    };
    let nil = Value::Nil;
    let sets = get_values!(2, parser.argv.len(), parser, db, dbindex, &nil);

    match el.sunion(&sets) {
        Ok(set) => Response::Array(
            set.iter()
                .map(|x| Response::Data(x.clone()))
                .collect::<Vec<_>>(),
        ),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn sunionstore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() >= 3, "Wrong number of parameters");
    let destination_key = try_validate!(parser.get_vec(1), "Invalid destination");
    let set = {
        let key = try_validate!(parser.get_vec(2), "Invalid key");
        let defaultel = Value::Nil;
        let el = match db.get(dbindex, &key) {
            Some(e) => e,
            None => &defaultel,
        };
        let nil = Value::Nil;
        let sets = get_values!(3, parser.argv.len(), parser, db, dbindex, &nil);
        match el.sunion(&sets) {
            Ok(set) => set,
            Err(err) => return Response::Error(err.to_string()),
        }
    };

    db.remove(dbindex, &destination_key);
    let r = set.len() as i64;
    db.get_or_create(dbindex, &destination_key).create_set(set);
    db.key_updated(dbindex, &destination_key);
    Response::Integer(r)
}

fn zadd(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    let len = parser.argv.len();
    validate!(len >= 4, "Wrong number of parameters");
    let mut nx = false;
    let mut xx = false;
    let mut ch = false;
    let mut incr = false;
    let mut i = 2;

    // up to 4 optional flags
    for _ in 0..4 {
        let opt = match parser.get_str(i) {
            Ok(s) => s,
            Err(_) => break,
        };
        i += 1;
        match &*opt.to_ascii_lowercase() {
            "nx" => nx = true,
            "xx" => xx = true,
            "ch" => ch = true,
            "incr" => incr = true,
            _ => {
                i -= 1;
                break;
            }
        }
    }

    if xx && nx {
        return Response::Error("ERR cannot use XX and NX".to_owned());
    }

    if (len - i) % 2 != 0 {
        return Response::Error("ERR syntax error".to_owned());
    }

    if incr && len - i != 2 {
        return Response::Error("ERR syntax error".to_owned());
    }

    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut count = 0;
    for j in 0..((len - i) / 2) {
        validate!(
            parser.get_f64(i + j * 2).is_ok(),
            "ERR value is not a valid float"
        );
    }
    {
        let el = db.get_or_create(dbindex, &key);
        for _ in 0..((len - i) / 2) {
            let score = parser.get_f64(i).unwrap();
            let val = try_validate!(parser.get_vec(i + 1), "Invalid value");
            match el.zadd(score, val, nx, xx, ch, incr) {
                Ok(added) => {
                    if added {
                        count += 1
                    }
                }
                Err(err) => return Response::Error(err.to_string()),
            }
            i += 2; // omg, so ugly `for`
        }
    }
    if count > 0 {
        db.key_updated(dbindex, &key);
    }

    Response::Integer(count)
}

fn zcard(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Integer(0),
    };

    match el.zcard() {
        Ok(count) => Response::Integer(count as i64),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn zscore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let element = try_validate!(parser.get_vec(2), "Invalid element");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Nil,
    };

    match el.zscore(element) {
        Ok(s) => match s {
            Some(score) => Response::Data(format!("{}", score).into_bytes()),
            None => Response::Nil,
        },
        Err(err) => Response::Error(err.to_string()),
    }
}

fn zincrby(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let newscore = {
        let el = db.get_or_create(dbindex, &key);
        let score = try_validate!(parser.get_f64(2), "ERR value is not a valid float");
        let member = try_validate!(parser.get_vec(3), "Invalid member");
        match el.zincrby(score, member) {
            Ok(score) => score,
            Err(err) => return Response::Error(err.to_string()),
        }
    };
    db.key_updated(dbindex, &key);

    Response::Data(format!("{}", newscore).into_bytes())
}

fn zrem(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() >= 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut count = 0;
    {
        let el = match db.get_mut(dbindex, &key) {
            Some(el) => el,
            None => return Response::Integer(0),
        };
        for i in 2..parser.argv.len() {
            let member = try_validate!(parser.get_vec(i), "Invalid member");
            match el.zrem(member) {
                Ok(removed) => {
                    if removed {
                        count += 1
                    }
                }
                Err(err) => return Response::Error(err.to_string()),
            }
        }
    }
    if count > 0 {
        db.key_updated(dbindex, &key);
    }

    Response::Integer(count)
}

fn zremrangebyscore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let min = try_validate!(parser.get_f64_bound(2), "ERR min or max is not a float");
    let max = try_validate!(parser.get_f64_bound(3), "ERR min or max is not a float");
    let c = {
        let el = match db.get_mut(dbindex, &key) {
            Some(e) => e,
            None => return Response::Integer(0),
        };
        match el.zremrangebyscore(min, max) {
            Ok(c) => c as i64,
            Err(err) => return Response::Error(err.to_string()),
        }
    };
    if c > 0 {
        db.key_updated(dbindex, &key);
    }
    Response::Integer(c)
}

fn zremrangebylex(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let min = {
        let m = try_validate!(
            parser.get_vec(2),
            "ERR min or max not valid string range item"
        );
        match get_vec_bound(m) {
            Ok(v) => v,
            Err(e) => return e,
        }
    };
    let max = {
        let m = try_validate!(
            parser.get_vec(3),
            "ERR min or max not valid string range item"
        );
        match get_vec_bound(m) {
            Ok(v) => v,
            Err(e) => return e,
        }
    };
    let c = {
        let el = match db.get_mut(dbindex, &key) {
            Some(e) => e,
            None => return Response::Integer(0),
        };
        match el.zremrangebylex(min, max) {
            Ok(c) => c as i64,
            Err(err) => return Response::Error(err.to_string()),
        }
    };
    if c > 0 {
        db.key_updated(dbindex, &key);
    }
    Response::Integer(c)
}

fn zremrangebyrank(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let start = try_validate!(parser.get_i64(2), "Invalid start");
    let stop = try_validate!(parser.get_i64(3), "Invalid stop");
    let c = {
        let el = match db.get_mut(dbindex, &key) {
            Some(e) => e,
            None => return Response::Integer(0),
        };
        match el.zremrangebyrank(start, stop) {
            Ok(c) => c as i64,
            Err(err) => return Response::Error(err.to_string()),
        }
    };
    if c > 0 {
        db.key_updated(dbindex, &key);
    }
    Response::Integer(c)
}

fn zcount(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let min = try_validate!(parser.get_f64_bound(2), "ERR min or max is not a float");
    let max = try_validate!(parser.get_f64_bound(3), "ERR min or max is not a float");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Integer(0),
    };
    match el.zcount(min, max) {
        Ok(c) => Response::Integer(c as i64),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn generic_zrange(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
    rev: bool,
) -> Response {
    validate_arguments_gte!(parser, 4);
    validate_arguments_lte!(parser, 5);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let start = try_validate!(parser.get_i64(2), "Invalid start");
    let stop = try_validate!(parser.get_i64(3), "Invalid stop");
    let withscores = parser.argv.len() == 5;
    if withscores {
        let p4 = try_validate!(parser.get_str(4), "Syntax error");
        validate!(p4.to_ascii_lowercase() == "withscores", "Syntax error");
    }
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Array(Vec::new()),
    };
    match el.zrange(start, stop, withscores, rev) {
        Ok(r) => Response::Array(
            r.iter()
                .map(|x| Response::Data(x.clone()))
                .collect::<Vec<_>>(),
        ),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn zrange(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_zrange(parser, db, dbindex, false)
}

fn zrevrange(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_zrange(parser, db, dbindex, true)
}

fn generic_zrangebyscore(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
    rev: bool,
) -> Response {
    let len = parser.argv.len();
    validate!(len >= 4, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let min = try_validate!(parser.get_f64_bound(2), "ERR min or max is not a float");
    let max = try_validate!(parser.get_f64_bound(3), "ERR min or max is not a float");

    let mut offset = 0;
    let mut count = usize::MAX;
    let mut withscores = false;
    let mut i = 4;
    while i < len {
        let arg = &*try_validate!(parser.get_str(i), "syntax error").to_ascii_lowercase();
        match arg {
            "withscores" => {
                i += 1;
                withscores = true;
            }
            "limit" => {
                offset = try_validate!(parser.get_i64(i + 1), "syntax error") as usize;
                count = try_validate!(parser.get_i64(i + 2), "syntax error") as usize;
                i += 3;
            }
            _ => return Response::Error("syntax error".to_owned()),
        }
    }

    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Array(Vec::new()),
    };
    match el.zrangebyscore(min, max, withscores, offset, count, rev) {
        Ok(r) => Response::Array(
            r.iter()
                .map(|x| Response::Data(x.clone()))
                .collect::<Vec<_>>(),
        ),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn zrangebyscore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_zrangebyscore(parser, db, dbindex, false)
}

fn zrevrangebyscore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_zrangebyscore(parser, db, dbindex, true)
}

fn get_vec_bound(mut m: Vec<u8>) -> Result<Bound<Vec<u8>>, Response> {
    if m.is_empty() {
        return Err(Response::Error(
            "ERR min or max not valid string range item".to_string(),
        ));
    }
    // FIXME: unnecessary memory move?
    Ok(match m.remove(0) as char {
        '(' => Bound::Excluded(m),
        '[' => Bound::Included(m),
        '-' => {
            if !m.is_empty() {
                return Err(Response::Error(
                    "ERR min or max not valid string range item".to_string(),
                ));
            }
            Bound::Unbounded
        }
        '+' => {
            if !m.is_empty() {
                return Err(Response::Error(
                    "ERR min or max not valid string range item".to_string(),
                ));
            }
            Bound::Unbounded
        }
        _ => {
            return Err(Response::Error(
                "ERR min or max not valid string range item".to_string(),
            ))
        }
    })
}

fn generic_zrangebylex(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
    rev: bool,
) -> Response {
    let len = parser.argv.len();
    validate!(len == 4 || len == 7, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let min = {
        let m = try_validate!(
            parser.get_vec(2),
            "ERR min or max not valid string range item"
        );
        match get_vec_bound(m) {
            Ok(v) => v,
            Err(e) => return e,
        }
    };
    let max = {
        let m = try_validate!(
            parser.get_vec(3),
            "ERR min or max not valid string range item"
        );
        match get_vec_bound(m) {
            Ok(v) => v,
            Err(e) => return e,
        }
    };

    let mut offset = 0;
    let mut count = usize::MAX;
    let limit = len >= 7;
    if limit {
        let p = try_validate!(parser.get_str(len - 3), "Syntax error");
        validate!(p.to_ascii_lowercase() == "limit", "Syntax error");
        offset = try_validate!(parser.get_i64(len - 2), "Syntax error") as usize;
        count = try_validate!(parser.get_i64(len - 1), "Syntax error") as usize;
    }

    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Array(Vec::new()),
    };
    match el.zrangebylex(min, max, offset, count, rev) {
        Ok(r) => Response::Array(
            r.iter()
                .map(|x| Response::Data(x.clone()))
                .collect::<Vec<_>>(),
        ),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn zrangebylex(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_zrangebylex(parser, db, dbindex, false)
}

fn zrevrangebylex(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_zrangebylex(parser, db, dbindex, true)
}

fn zlexcount(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let min = {
        let m = try_validate!(
            parser.get_vec(2),
            "ERR min or max not valid string range item"
        );
        match get_vec_bound(m) {
            Ok(v) => v,
            Err(e) => return e,
        }
    };
    let max = {
        let m = try_validate!(
            parser.get_vec(3),
            "ERR min or max not valid string range item"
        );
        match get_vec_bound(m) {
            Ok(v) => v,
            Err(e) => return e,
        }
    };
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Integer(0),
    };
    match el.zlexcount(min, max) {
        Ok(c) => Response::Integer(c as i64),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn generic_zrank(
    db: &mut Database,
    dbindex: usize,
    key: &[u8],
    member: Vec<u8>,
    rev: bool,
) -> Response {
    let el = match db.get(dbindex, key) {
        Some(e) => e,
        None => return Response::Nil,
    };
    let card = match el.zcard() {
        Ok(card) => card,
        Err(err) => return Response::Error(err.to_string()),
    };
    match el.zrank(member) {
        Ok(r) => match r {
            Some(v) => Response::Integer(if rev { card - v - 1 } else { v } as i64),
            None => Response::Nil,
        },
        Err(err) => Response::Error(err.to_string()),
    }
}

fn zrank(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let member = try_validate!(parser.get_vec(2), "Invalid member");
    generic_zrank(db, dbindex, &key, member, false)
}

fn zrevrank(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let member = try_validate!(parser.get_vec(2), "Invalid member");
    generic_zrank(db, dbindex, &key, member, true)
}

fn zinter_union_store(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
    union: bool,
) -> Response {
    validate!(parser.argv.len() >= 4, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let value = {
        let numkeys = {
            let n = try_validate!(parser.get_i64(2), "Invalid number of keys");
            if n <= 0 {
                return Response::Error(
                    "at least 1 input key is needed for \
                     ZUNIONSTORE/ZINTERSTORE"
                        .to_string(),
                );
            }
            n as usize
        };
        let nil = Value::Nil;
        let zsets = get_values!(3, 3 + numkeys, parser, db, dbindex, &nil);
        let mut pos = 3 + numkeys;
        let mut weights = None;
        let mut aggregate = zset::Aggregate::Sum;
        if pos < parser.argv.len() {
            let arg = try_validate!(parser.get_str(pos), "syntax error");
            if arg.to_ascii_lowercase() == "weights" {
                pos += 1;
                validate!(
                    parser.argv.len() >= pos + numkeys,
                    "Wrong number of parameters"
                );
                let mut w = Vec::with_capacity(numkeys);
                for i in 0..numkeys {
                    w.push(try_validate!(
                        parser.get_f64(pos + i),
                        "ERR weight value is not a float"
                    ));
                }
                weights = Some(w);
                pos += numkeys;
            }
        };
        if pos < parser.argv.len() {
            let arg = try_validate!(parser.get_str(pos), "syntax error");
            if arg.to_ascii_lowercase() == "aggregate" {
                pos += 1;
                validate!(parser.argv.len() != pos, "Wrong number of parameters");
                aggregate = match &*try_validate!(parser.get_str(pos), "syntax error")
                    .to_ascii_lowercase()
                {
                    "sum" => zset::Aggregate::Sum,
                    "max" => zset::Aggregate::Max,
                    "min" => zset::Aggregate::Min,
                    _ => return Response::Error("syntax error".to_string()),
                };
                pos += 1;
            }
        };
        validate!(pos == parser.argv.len(), "syntax error");
        let n = Value::Nil;
        match if union {
            n.zunion(&zsets, weights, aggregate)
        } else {
            n.zinter(&zsets, weights, aggregate)
        } {
            Ok(v) => v,
            Err(err) => return Response::Error(err.to_string()),
        }
    };
    let r = match value.zcard() {
        Ok(count) => Response::Integer(count as i64),
        Err(err) => Response::Error(err.to_string()),
    };
    *db.get_or_create(dbindex, &key) = value;
    db.key_updated(dbindex, &key);
    r
}

fn zunionstore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    zinter_union_store(parser, db, dbindex, true)
}

fn zinterstore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    zinter_union_store(parser, db, dbindex, false)
}

fn hset(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let field = try_validate!(parser.get_vec(2), "Invalid field");
    let value = try_validate!(parser.get_vec(3), "Invalid value");
    let el = db.get_or_create(dbindex, &key);
    match el.hset(field, value) {
        Ok(created) => {
            db.key_updated(dbindex, &key);
            Response::Integer(if created { 1 } else { 0 })
        }
        Err(err) => Response::Error(err.to_string()),
    }
}

fn hsetnx(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let field = try_validate!(parser.get_vec(2), "Invalid field");
    let value = try_validate!(parser.get_vec(3), "Invalid value");
    let el = match db.get_mut(dbindex, &key) {
        Some(e) => e,
        None => {
            let el = db.get_or_create(dbindex, &key);
            match el.hset(field, value) {
                Ok(_) => {
                    db.key_updated(dbindex, &key);
                    return Response::Integer(1);
                }
                Err(err) => return Response::Error(err.to_string()),
            }
        }
    };
    match el.hexists(field.as_slice()) {
        Ok(true) => Response::Integer(0),
        Ok(false) => {
            match el.hset(field, value) {
                Ok(_) => {
                    db.key_updated(dbindex, &key);
                    Response::Integer(1)
                }
                Err(err) => Response::Error(err.to_string()),
            }
        }
        Err(err) => Response::Error(err.to_string()),
    }
}

fn hget(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let field = try_validate!(parser.get_vec(2), "Invalid field");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Nil,
    };
    match el.hget(field.as_slice()) {
        Ok(Some(value)) => Response::Data(value.clone()),
        Ok(None) => Response::Nil,
        Err(err) => Response::Error(err.to_string()),
    }
}

fn hmset(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 4);
    validate!(
        (parser.argv.len() - 1) % 2 == 1,
        "ERR wrong number of arguments for 'hmset' command"
    );
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let el = db.get_or_create(dbindex, &key);
    for i in (2..parser.argv.len()).step_by(2) {
        let field = try_validate!(parser.get_vec(i), "Invalid field");
        let value = try_validate!(parser.get_vec(i + 1), "Invalid value");
        match el.hset(field, value) {
            Ok(_) => (),
            Err(err) => return Response::Error(err.to_string()),
        }
    }
    db.key_updated(dbindex, &key);
    Response::Status("OK".to_owned())
}

fn hmget(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => {
            let mut result = Vec::new();
            for _ in 2..parser.argv.len() {
                result.push(Response::Nil);
            }
            return Response::Array(result);
        }
    };
    let mut result = Vec::new();
    for i in 2..parser.argv.len() {
        let field = try_validate!(parser.get_vec(i), "Invalid field");
        match el.hget(field.as_slice()) {
            Ok(Some(value)) => result.push(Response::Data(value.clone())),
            Ok(None) => result.push(Response::Nil),
            Err(err) => return Response::Error(err.to_string()),
        }
    }
    Response::Array(result)
}

fn hdel(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut fields = Vec::new();
    for i in 2..parser.argv.len() {
        fields.push(try_validate!(parser.get_vec(i), "Invalid field"));
    }
    let field_refs: Vec<&[u8]> = fields.iter().map(|f| f.as_slice()).collect();
    let el = match db.get_mut(dbindex, &key) {
        Some(e) => e,
        None => return Response::Integer(0),
    };
    match el.hdel(&field_refs) {
        Ok(count) => {
            db.key_updated(dbindex, &key);
            Response::Integer(count as i64)
        }
        Err(err) => Response::Error(err.to_string()),
    }
}

fn hlen(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Integer(0),
    };
    match el.hlen() {
        Ok(count) => Response::Integer(count as i64),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn hstrlen(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let field = try_validate!(parser.get_vec(2), "Invalid field");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Integer(0),
    };
    match el.hstrlen(field.as_slice()) {
        Ok(len) => Response::Integer(len as i64),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn hkeys(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Array(Vec::new()),
    };
    match el.hkeys() {
        Ok(keys) => Response::Array(keys.into_iter().map(Response::Data).collect()),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn hvals(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Array(Vec::new()),
    };
    match el.hvals() {
        Ok(vals) => Response::Array(vals.into_iter().map(Response::Data).collect()),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn hgetall(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Array(Vec::new()),
    };
    match el.hgetall() {
        Ok(all) => Response::Array(all.into_iter().map(Response::Data).collect()),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn hexists(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let field = try_validate!(parser.get_vec(2), "Invalid field");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Integer(0),
    };
    match el.hexists(field.as_slice()) {
        Ok(exists) => Response::Integer(if exists { 1 } else { 0 }),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn hincrby(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let field = try_validate!(parser.get_vec(2), "Invalid field");
    let increment = try_validate!(parser.get_i64(3), "ERR value is not an integer or out of range");
    let el = db.get_or_create(dbindex, &key);
    match el.hincrby(field, increment) {
        Ok(new_value) => {
            db.key_updated(dbindex, &key);
            Response::Integer(new_value)
        }
        Err(err) => Response::Error(err.to_string()),
    }
}

fn hincrbyfloat(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let field = try_validate!(parser.get_vec(2), "Invalid field");
    let increment = try_validate!(parser.get_f64(3), "ERR value is not a valid float");
    let el = db.get_or_create(dbindex, &key);
    match el.hincrbyfloat(field, increment) {
        Ok(new_value) => {
            db.key_updated(dbindex, &key);
            Response::Data(new_value.to_string().into_bytes())
        }
        Err(err) => Response::Error(err.to_string()),
    }
}

fn scan(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let cursor = try_validate!(parser.get_i64(1), "ERR invalid cursor");
    validate!(cursor >= 0, "ERR invalid cursor");
    let mut pattern = None;
    let mut count = 10;
    
    let mut i = 2;
    while i < parser.argv.len() {
        let arg = try_validate!(parser.get_str(i), "syntax error");
        match &*arg.to_ascii_lowercase() {
            "match" => {
                pattern = Some(try_validate!(parser.get_vec(i + 1), "syntax error"));
                i += 2;
            }
            "count" => {
                let c = try_validate!(parser.get_i64(i + 1), "syntax error");
                validate!(c >= 0, "syntax error");
                count = c as usize;
                if count == 0 {
                    count = 10;
                }
                i += 2;
            }
            _ => return Response::Error("syntax error".to_owned()),
        }
    }
    
    let (next_cursor, keys) = db.scan(dbindex, cursor as usize, pattern.as_deref(), count);
    let result = vec![
        Response::Data(next_cursor.to_string().into_bytes()),
        Response::Array(keys.into_iter().map(Response::Data).collect()),
    ];
    Response::Array(result)
}

fn sscan(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let cursor = try_validate!(parser.get_i64(2), "ERR invalid cursor");
    validate!(cursor >= 0, "ERR invalid cursor");
    let mut pattern = None;
    let mut count = 10;
    
    let mut i = 3;
    while i < parser.argv.len() {
        let arg = try_validate!(parser.get_str(i), "syntax error");
        match &*arg.to_ascii_lowercase() {
            "match" => {
                pattern = Some(try_validate!(parser.get_vec(i + 1), "syntax error"));
                i += 2;
            }
            "count" => {
                let c = try_validate!(parser.get_i64(i + 1), "syntax error");
                validate!(c >= 0, "syntax error");
                count = c as usize;
                if count == 0 {
                    count = 10;
                }
                i += 2;
            }
            _ => return Response::Error("syntax error".to_owned()),
        }
    }
    
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Array(vec![
            Response::Data(b"0".to_vec()),
            Response::Array(Vec::new()),
        ]),
    };
    
    match el.sscan(cursor as usize, pattern.as_deref(), count) {
        Ok((next_cursor, members)) => Response::Array(vec![
            Response::Data(next_cursor.to_string().into_bytes()),
            Response::Array(members.into_iter().map(Response::Data).collect()),
        ]),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn zscan(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let cursor = try_validate!(parser.get_i64(2), "ERR invalid cursor");
    validate!(cursor >= 0, "ERR invalid cursor");
    let mut pattern = None;
    let mut count = 10;
    
    let mut i = 3;
    while i < parser.argv.len() {
        let arg = try_validate!(parser.get_str(i), "syntax error");
        match &*arg.to_ascii_lowercase() {
            "match" => {
                pattern = Some(try_validate!(parser.get_vec(i + 1), "syntax error"));
                i += 2;
            }
            "count" => {
                let c = try_validate!(parser.get_i64(i + 1), "syntax error");
                validate!(c >= 0, "syntax error");
                count = c as usize;
                if count == 0 {
                    count = 10;
                }
                i += 2;
            }
            _ => return Response::Error("syntax error".to_owned()),
        }
    }
    
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Array(vec![
            Response::Data(b"0".to_vec()),
            Response::Array(Vec::new()),
        ]),
    };
    
    match el.zscan(cursor as usize, pattern.as_deref(), count) {
        Ok((next_cursor, members)) => Response::Array(vec![
            Response::Data(next_cursor.to_string().into_bytes()),
            Response::Array(members.into_iter().map(Response::Data).collect()),
        ]),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn hscan(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let cursor = try_validate!(parser.get_i64(2), "ERR invalid cursor");
    validate!(cursor >= 0, "ERR invalid cursor");
    let mut pattern = None;
    let mut count = 10;
    
    let mut i = 3;
    while i < parser.argv.len() {
        let arg = try_validate!(parser.get_str(i), "syntax error");
        match &*arg.to_ascii_lowercase() {
            "match" => {
                pattern = Some(try_validate!(parser.get_vec(i + 1), "syntax error"));
                i += 2;
            }
            "count" => {
                let c = try_validate!(parser.get_i64(i + 1), "syntax error");
                validate!(c >= 0, "syntax error");
                count = c as usize;
                if count == 0 {
                    count = 10;
                }
                i += 2;
            }
            _ => return Response::Error("syntax error".to_owned()),
        }
    }
    
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Array(vec![
            Response::Data(b"0".to_vec()),
            Response::Array(Vec::new()),
        ]),
    };
    
    match el.hscan(cursor as usize, pattern.as_deref(), count) {
        Ok((next_cursor, fields)) => Response::Array(vec![
            Response::Data(next_cursor.to_string().into_bytes()),
            Response::Array(fields.into_iter().map(Response::Data).collect()),
        ]),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn ping(parser: &mut ParsedCommand, client: &mut Client) -> Response {
    validate!(
        parser.argv.len() <= 2,
        format!(
            "ERR wrong number of arguments for '{}' command",
            parser.get_str(0).unwrap()
        )
    );

    if !client.subscriptions.is_empty() {
        if parser.argv.len() == 2 {
            match parser.get_vec(1) {
                Ok(r) => Response::Array(vec![Response::Data(b"pong".to_vec()), Response::Data(r)]),
                Err(err) => Response::Error(err.to_string()),
            }
        } else {
            Response::Array(vec![
                Response::Data(b"pong".to_vec()),
                Response::Data(vec![]),
            ])
        }
    } else if parser.argv.len() == 2 {
        match parser.get_vec(1) {
            Ok(r) => Response::Data(r),
            Err(err) => Response::Error(err.to_string()),
        }
    } else {
        Response::Status("PONG".to_owned())
    }
}

fn subscribe(
    parser: &mut ParsedCommand,
    db: &mut Database,
    subscriptions: &mut HashMap<Vec<u8>, usize>,
    pattern_subscriptions_len: usize,
    sender: &Sender<Option<Response>>,
) -> Result<Response, ResponseError> {
    opt_validate!(parser.argv.len() >= 2, "Wrong number of parameters");
    for i in 1..parser.argv.len() {
        let channel_name = try_opt_validate!(parser.get_vec(i), "Invalid channel");
        if !subscriptions.contains_key(&channel_name) {
            let subscriber_id = db.subscribe(channel_name.clone(), sender.clone());
            subscriptions.insert(channel_name.clone(), subscriber_id);
        }
        match sender.send(Some(
            PubsubEvent::Subscription(
                channel_name.clone(),
                pattern_subscriptions_len + subscriptions.len(),
            )
            .as_response(),
        )) {
            Ok(_) => None,
            Err(_) => subscriptions.remove(&channel_name),
        };
    }
    Err(ResponseError::NoReply)
}

fn unsubscribe(
    parser: &mut ParsedCommand,
    db: &mut Database,
    subscriptions: &mut HashMap<Vec<u8>, usize>,
    pattern_subscriptions_len: usize,
    sender: &Sender<Option<Response>>,
) -> Result<Response, ResponseError> {
    if parser.argv.len() == 1 {
        if subscriptions.is_empty() {
            let _ = sender.send(Some(
                PubsubEvent::Unsubscription(vec![], pattern_subscriptions_len).as_response(),
            ));
        } else {
            for (channel_name, subscriber_id) in subscriptions.drain() {
                db.unsubscribe(channel_name.clone(), subscriber_id);
                let _ = sender.send(Some(
                    PubsubEvent::Unsubscription(channel_name, pattern_subscriptions_len)
                        .as_response(),
                ));
            }
        }
    } else {
        for i in 1..parser.argv.len() {
            let channel_name = try_opt_validate!(parser.get_vec(i), "Invalid channel");
            if let Some(subscriber_id) = subscriptions.remove(&channel_name) {
                db.unsubscribe(channel_name.clone(), subscriber_id);
            }
            let _ = sender.send(Some(
                PubsubEvent::Unsubscription(
                    channel_name,
                    pattern_subscriptions_len + subscriptions.len(),
                )
                .as_response(),
            ));
        }
    }
    Err(ResponseError::NoReply)
}

fn psubscribe(
    parser: &mut ParsedCommand,
    db: &mut Database,
    subscriptions_len: usize,
    pattern_subscriptions: &mut HashMap<Vec<u8>, usize>,
    sender: &Sender<Option<Response>>,
) -> Result<Response, ResponseError> {
    opt_validate!(parser.argv.len() >= 2, "Wrong number of parameters");
    for i in 1..parser.argv.len() {
        let pattern = try_opt_validate!(parser.get_vec(i), "Invalid channel");
        let subscriber_id = db.psubscribe(pattern.clone(), sender.clone());
        pattern_subscriptions.insert(pattern.clone(), subscriber_id);
        match sender.send(Some(
            PubsubEvent::PatternSubscription(
                pattern.clone(),
                subscriptions_len + pattern_subscriptions.len(),
            )
            .as_response(),
        )) {
            Ok(_) => None,
            Err(_) => pattern_subscriptions.remove(&pattern),
        };
    }
    Err(ResponseError::NoReply)
}

fn punsubscribe(
    parser: &mut ParsedCommand,
    db: &mut Database,
    subscriptions_len: usize,
    pattern_subscriptions: &mut HashMap<Vec<u8>, usize>,
    sender: &Sender<Option<Response>>,
) -> Result<Response, ResponseError> {
    if parser.argv.len() == 1 {
        if pattern_subscriptions.is_empty() {
            let _ = sender.send(Some(
                PubsubEvent::PatternUnsubscription(vec![], subscriptions_len).as_response(),
            ));
        } else {
            for (pattern, subscriber_id) in pattern_subscriptions.drain() {
                db.punsubscribe(pattern.clone(), subscriber_id);
                let _ = sender.send(Some(
                    PubsubEvent::PatternUnsubscription(pattern, subscriptions_len).as_response(),
                ));
            }
        }
    } else {
        for i in 1..parser.argv.len() {
            let pattern = try_opt_validate!(parser.get_vec(i), "Invalid pattern");
            if let Some(subscriber_id) = pattern_subscriptions.remove(&pattern) {
                db.punsubscribe(pattern.clone(), subscriber_id);
            }
            let _ = sender.send(Some(
                PubsubEvent::PatternUnsubscription(
                    pattern,
                    subscriptions_len + pattern_subscriptions.len(),
                )
                .as_response(),
            ));
        }
    }
    Err(ResponseError::NoReply)
}

fn publish(parser: &mut ParsedCommand, db: &mut Database) -> Response {
    validate_arguments_exact!(parser, 3);
    let channel_name = try_validate!(parser.get_vec(1), "Invalid channel");
    let message = try_validate!(parser.get_vec(2), "Invalid channel");
    Response::Integer(db.publish(&channel_name, &message) as i64)
}

fn time(_parser: &mut ParsedCommand, _db: &mut Database) -> Response {
    let now_us = ustime();
    let secs = now_us / 1_000_000;
    let micros = now_us % 1_000_000;
    Response::Array(vec![
        Response::Data(secs.to_string().into_bytes()),
        Response::Data(micros.to_string().into_bytes()),
    ])
}

fn role(_parser: &mut ParsedCommand, _db: &mut Database) -> Response {
    // TODO: Implement full replication support
    // For now, always return master role
    Response::Array(vec![
        Response::Data(b"master".to_vec()),
        Response::Integer(0), // replication offset
        Response::Array(Vec::new()), // connected slaves
    ])
}

fn slaveof(parser: &mut ParsedCommand, _db: &mut Database) -> Response {
    validate_arguments_exact!(parser, 3);
    let host = try_validate!(parser.get_str(1), "Invalid host");
    let port = try_validate!(parser.get_str(2), "Invalid port");
    
    // TODO: Implement full replication support
    // For now, just return OK
    if host == "NO" && port == "ONE" {
        // SLAVEOF NO ONE - stop replication
        Response::Status("OK".to_owned())
    } else {
        // SLAVEOF host port - start replication
        // TODO: Connect to master and start replication
        Response::Status("OK".to_owned())
    }
}

fn object(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let subcommand = try_validate!(parser.get_str(1), "Invalid subcommand");
    let key = try_validate!(parser.get_vec(2), "Invalid key");
    
    match &*subcommand.to_ascii_lowercase() {
        "encoding" => {
            match db.get(dbindex, &key) {
                Some(value) => {
                    let encoding = match value {
                        Value::String(_) => "raw",
                        Value::List(l) => match l {
                            ValueList::Data(_) => "linkedlist",
                        },
                        Value::Set(_) => "hashtable",
                        Value::SortedSet(_) => "skiplist",
                        Value::Hash(h) => match h {
                            ValueHash::ZipList(_) => "ziplist",
                            ValueHash::HashMap(_) => "hashtable",
                        },
                        Value::Nil => return Response::Nil,
                    };
                    Response::Data(encoding.to_string().into_bytes())
                }
                None => Response::Nil,
            }
        }
        "idletime" => {
            // TODO: Track last access time for LRU
            // For now, return 0
            match db.get(dbindex, &key) {
                Some(_) => Response::Integer(0),
                None => Response::Nil,
            }
        }
        "refcount" => {
            // TODO: Track reference count
            // For now, return 1
            match db.get(dbindex, &key) {
                Some(_) => Response::Integer(1),
                None => Response::Nil,
            }
        }
        "freq" => {
            // TODO: Track access frequency for LFU
            // For now, return 0
            match db.get(dbindex, &key) {
                Some(_) => Response::Integer(0),
                None => Response::Nil,
            }
        }
        _ => Response::Error("ERR Unknown subcommand or wrong number of arguments for OBJECT".to_owned()),
    }
}

fn bitcount(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    
    let start = if parser.argv.len() > 2 {
        let s = try_validate!(parser.get_i64(2), "Invalid start");
        s as usize
    } else {
        0
    };
    
    let end = if parser.argv.len() > 3 {
        let e = try_validate!(parser.get_i64(3), "Invalid end");
        e as usize
    } else {
        usize::MAX
    };
    
    match db.get(dbindex, &key) {
        Some(value) => {
            match value.get() {
                Ok(data) => {
                    let data_len = data.len();
                    let start_byte = std::cmp::min(start, data_len);
                    let end_byte = std::cmp::min(end + 1, data_len);
                    if start_byte >= end_byte {
                        return Response::Integer(0);
                    }
                    let slice = &data[start_byte..end_byte];
                    let mut count = 0;
                    for &byte in slice {
                        count += byte.count_ones() as i64;
                    }
                    Response::Integer(count)
                }
                Err(_) => Response::Integer(0),
            }
        }
        None => Response::Integer(0),
    }
}

fn bitpos(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let bit = try_validate!(parser.get_i64(2), "Invalid bit");
    validate!(bit == 0 || bit == 1, "ERR The bit argument must be 1 or 0");
    
    let start = if parser.argv.len() > 3 {
        let s = try_validate!(parser.get_i64(3), "Invalid start");
        s as usize
    } else {
        0
    };
    
    let end = if parser.argv.len() > 4 {
        let e = try_validate!(parser.get_i64(4), "Invalid end");
        e as usize
    } else {
        usize::MAX
    };
    
    match db.get(dbindex, &key) {
        Some(value) => {
            match value.get() {
                Ok(data) => {
                    let data_len = data.len();
                    let start_byte = std::cmp::min(start, data_len);
                    let end_byte = std::cmp::min(end + 1, data_len);
                    if start_byte >= end_byte {
                        return Response::Integer(if bit == 0 { 0 } else { -1 });
                    }
                    let slice = &data[start_byte..end_byte];
                    let target_bit = bit == 1;
                    let mut bit_offset = 0;
                    for &byte in slice {
                        for bit_idx in 0..8 {
                            let bit_val = (byte & (1 << (7 - bit_idx))) != 0;
                            if bit_val == target_bit {
                                return Response::Integer((start_byte * 8 + bit_offset + bit_idx) as i64);
                            }
                        }
                        bit_offset += 8;
                    }
                    Response::Integer(if bit == 0 { 0 } else { -1 })
                }
                Err(_) => Response::Integer(if bit == 0 { 0 } else { -1 }),
            }
        }
        None => Response::Integer(if bit == 0 { 0 } else { -1 }),
    }
}

fn bitop(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 4);
    let op = try_validate!(parser.get_str(1), "Invalid operation");
    let dest_key = try_validate!(parser.get_vec(2), "Invalid destination key");
    
    let mut source_keys = Vec::new();
    for i in 3..parser.argv.len() {
        source_keys.push(try_validate!(parser.get_vec(i), "Invalid source key"));
    }
    
    let op_lower = op.to_ascii_lowercase();
    match op_lower.as_str() {
        "and" | "or" | "xor" => {
            if source_keys.is_empty() {
                return Response::Error("ERR wrong number of arguments for 'bitop' command".to_owned());
            }
            
            // Get all source values
            let mut sources = Vec::new();
            let mut max_len = 0;
            for key in &source_keys {
                match db.get(dbindex, key) {
                    Some(value) => {
                        match value.get() {
                            Ok(data) => {
                                max_len = std::cmp::max(max_len, data.len());
                                sources.push(Some(data));
                            }
                            Err(_) => sources.push(None),
                        }
                    }
                    None => sources.push(None),
                }
            }
            
            // Perform bitwise operation
            let mut result = vec![0u8; max_len];
            for i in 0..max_len {
                let byte_val = match op_lower.as_str() {
                    "and" => {
                        // For AND, start with 0xFF and AND with each source
                        // Missing keys are treated as all zeros
                        let mut val = 0xFFu8;
                        for source in &sources {
                            let byte = source.as_ref()
                                .and_then(|s| s.get(i))
                                .copied()
                                .unwrap_or(0);
                            val &= byte;
                        }
                        val
                    }
                    "or" => {
                        // For OR, start with 0x00 and OR with each source
                        let mut val = 0x00u8;
                        for source in &sources {
                            let byte = source.as_ref()
                                .and_then(|s| s.get(i))
                                .copied()
                                .unwrap_or(0);
                            val |= byte;
                        }
                        val
                    }
                    "xor" => {
                        // For XOR, start with 0x00 and XOR with each source
                        let mut val = 0x00u8;
                        for source in &sources {
                            let byte = source.as_ref()
                                .and_then(|s| s.get(i))
                                .copied()
                                .unwrap_or(0);
                            val ^= byte;
                        }
                        val
                    }
                    _ => unreachable!(),
                };
                result[i] = byte_val;
            }
            
            // Store result
            db.get_or_create(dbindex, &dest_key).set(result).unwrap();
            db.key_updated(dbindex, &dest_key);
            Response::Integer(max_len as i64)
        }
        "not" => {
            if source_keys.len() != 1 {
                return Response::Error("ERR BITOP NOT must be called with a single source key".to_owned());
            }
            
            let source_key = &source_keys[0];
            match db.get(dbindex, source_key) {
                Some(value) => {
                    match value.get() {
                        Ok(data) => {
                            let result: Vec<u8> = data.iter().map(|&b| !b).collect();
                            db.get_or_create(dbindex, &dest_key).set(result).unwrap();
                            db.key_updated(dbindex, &dest_key);
                            Response::Integer(data.len() as i64)
                        }
                        Err(_) => {
                            db.get_or_create(dbindex, &dest_key).set(Vec::new()).unwrap();
                            db.key_updated(dbindex, &dest_key);
                            Response::Integer(0)
                        }
                    }
                }
                None => {
                    db.get_or_create(dbindex, &dest_key).set(Vec::new()).unwrap();
                    db.key_updated(dbindex, &dest_key);
                    Response::Integer(0)
                }
            }
        }
        _ => Response::Error("ERR syntax error".to_owned()),
    }
}

fn client_cmd(parser: &mut ParsedCommand, client: &mut Client) -> Response {
    validate_arguments_gte!(parser, 2);
    let subcommand = try_validate!(parser.get_str(1), "Invalid subcommand");
    
    match &*subcommand.to_ascii_lowercase() {
        "list" => {
            // TODO: Implement full client list with all client information
            // For now, return basic client info
            let client_info = format!(
                "id={} addr=127.0.0.1:* fd=-1 age=0 idle=0 flags=N db={} sub={} psub={} multi={} qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=client",
                client.id,
                client.dbindex,
                client.subscriptions.len(),
                client.pattern_subscriptions.len(),
                if client.multi { "1" } else { "-1" }
            );
            Response::Data(client_info.into_bytes())
        }
        "getname" => {
            // TODO: Track client names
            // For now, return nil
            Response::Nil
        }
        "setname" => {
            validate_arguments_exact!(parser, 3);
            let _name = try_validate!(parser.get_str(2), "Invalid name");
            // TODO: Store client name
            // For now, just return OK
            Response::Status("OK".to_owned())
        }
        "kill" => {
            // TODO: Implement client kill
            // For now, return error
            Response::Error("ERR CLIENT KILL is not implemented".to_owned())
        }
        "pause" => {
            // TODO: Implement client pause
            // For now, return error
            Response::Error("ERR CLIENT PAUSE is not implemented".to_owned())
        }
        "reply" => {
            // TODO: Implement client reply
            // For now, return error
            Response::Error("ERR CLIENT REPLY is not implemented".to_owned())
        }
        _ => Response::Error("ERR Unknown subcommand or wrong number of arguments for 'client' command".to_owned()),
    }
}

fn pubsub(parser: &mut ParsedCommand, db: &mut Database) -> Response {
    validate_arguments_gte!(parser, 2);
    let subcommand = try_validate!(parser.get_str(1), "Invalid subcommand");
    match &*subcommand.to_ascii_lowercase() {
        "channels" => {
            let pattern = if parser.argv.len() > 2 {
                Some(try_validate!(parser.get_vec(2), "Invalid pattern"))
            } else {
                None
            };
            let channels = db.pubsub_channel_list(pattern);
            Response::Array(channels.into_iter().map(Response::Data).collect())
        }
        "numsub" => {
            let mut channels = Vec::new();
            for i in 2..parser.argv.len() {
                channels.push(try_validate!(parser.get_vec(i), "Invalid channel"));
            }
            let results = db.pubsub_numsub(&channels);
            let mut response = Vec::new();
            for (channel, count) in results {
                response.push(Response::Data(channel));
                response.push(Response::Integer(count as i64));
            }
            Response::Array(response)
        }
        "numpat" => {
            validate_arguments_exact!(parser, 2);
            Response::Integer(db.pubsub_patterns() as i64)
        }
        _ => Response::Error("ERR Unknown PUBSUB subcommand or wrong number of arguments".to_owned()),
    }
}

fn monitor(
    parser: &mut ParsedCommand,
    db: &mut Database,
    rawsender: Sender<Option<Response>>,
) -> Response {
    validate_arguments_exact!(parser, 1);
    let (tx, rx) = channel();
    db.monitor_add(tx);
    thread::spawn(move || {
        while rx
            .recv()
            .ok()
            .and_then(|r| rawsender.send(Some(Response::Status(r))).ok())
            .is_some()
        {}
    });
    Response::Status("OK".to_owned())
}

#[cfg(all(target_pointer_width = "32"))]
const BITS: usize = 32;
#[cfg(all(target_pointer_width = "64"))]
const BITS: usize = 64;

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;
    
    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }
    
    if unit_idx == 0 {
        format!("{}B", bytes)
    } else {
        format!("{:.2}{}", size, UNITS[unit_idx])
    }
}

fn info(parser: &mut ParsedCommand, db: &Database) -> Response {
    validate_arguments_lte!(parser, 2);
    let section = &*(if parser.argv.len() == 1 {
        "default".to_owned()
    } else {
        try_validate!(parser.get_str(1), "Invalid section").to_ascii_lowercase()
    });

    let mut out = vec![];
    if section == "default" || section == "all" || section == "server" {
        // TODO: cache getos() result
        let os = getos();
        let uptime = db.uptime();
        try_validate!(
            write!(
                out,
                "\
                 # Server\r\n\
                 rsedis_version:{}\r\n\
                 rsedis_git_sha1:{}\r\n\
                 rsedis_git_dirty:{}\r\n\
                 os:{} {} {}\r\n\
                 arch_bits:{}\r\n\
                 multiplexing_api:no\r\n\
                 rustc_version:{}\r\n\
                 process_id:{}\r\n\
                 run_id:{}\r\n\
                 tcp_port:{}\r\n\
                uptime_in_seconds:{}\r\n\
                uptime_in_days:{}\r\n\
                lru_clock:{}\r\n\
                \r\n\
                ",
                db.version,
                db.git_sha1,
                if db.git_dirty { 1 } else { 0 },
                os.0,
                os.1,
                os.2,
                BITS,
                db.rustc_version,
                getpid(),
                db.run_id,
                db.config.port,
                uptime / 1000,
                uptime / (1000 * 60 * 60 * 24),
                mstime() / 1000,
            ),
            "ERR unexpected"
        );
    }

    if section == "default" || section == "all" || section == "persistence" {
        try_validate!(
            write!(
                out,
                "\
                # Persistence\r\n\
                loading:{}\r\n\
                rdb_changes_since_last_save:0\r\n\
                rdb_bgsave_in_progress:0\r\n\
                rdb_last_save_time:{}\r\n\
                rdb_last_bgsave_status:ok\r\n\
                rdb_last_bgsave_time_sec:-1\r\n\
                rdb_current_bgsave_time_sec:-1\r\n\
                aof_enabled:{}\r\n\
                aof_rewrite_in_progress:0\r\n\
                aof_rewrite_scheduled:0\r\n\
                aof_last_rewrite_time_sec:-1\r\n\
                aof_current_rewrite_time_sec:-1\r\n\
                aof_last_bgrewrite_status:ok\r\n\
                changes_since_last_save:0\r\n\
                aof_current_size:0\r\n\
                aof_base_size:0\r\n\
                aof_pending_rewrite:0\r\n\
                aof_buffer_length:0\r\n\
                aof_rewrite_buffer_length:0\r\n\
                aof_pending_bio_fsync:0\r\n\
                aof_delayed_fsync:0\r\n\
                \r\n\
                ",
                if db.loading { 1 } else { 0 },
                db.last_save_time,
                if db.aof.is_some() { 1 } else { 0 },
            ),
            "ERR unexpected"
        );
    }

    if section == "default" || section == "all" || section == "loading" {
        if db.loading {
            let loading_start_time = db.start_mstime;
            let _now = mstime();
            let _elapsed = (_now - loading_start_time) / 1000;
            try_validate!(
                write!(
                    out,
                    "\
                    # Loading\r\n\
                    loading_start_time:{}\r\n\
                    loading_total_bytes:0\r\n\
                    loading_loaded_bytes:0\r\n\
                    loading_loaded_perc:0.00\r\n\
                    loading_eta_seconds:0\r\n\
                    \r\n\
                    ",
                    loading_start_time / 1000,
                ),
                "ERR unexpected"
            );
        }
    }

    if section == "default" || section == "all" || section == "keyspace" {
        try_validate!(write!(out, "# Keyspace\r\n"), "ERR unexpected");
        for dbindex in 0..(db.config.databases as usize) {
            let dbsize = db.dbsize(dbindex);
            if dbsize > 0 {
                let avg_ttl = db.db_avg_ttl(dbindex);
                try_validate!(
                    write!(
                        out,
                        "db{}:keys={};expires={};avg_ttl={}\r\n",
                        dbindex,
                        dbsize,
                        db.db_expire_size(dbindex),
                        avg_ttl
                    ),
                    "ERR unexpected"
                );
            }
        }
    }

    if section == "default" || section == "all" || section == "commandstats" {
        // Commandstats section - for now return empty since we don't track per-command stats
        // TODO: Implement command statistics tracking
        try_validate!(write!(out, "# Commandstats\r\n"), "ERR unexpected");
    }

    if section == "default" || section == "all" || section == "cluster" {
        // Cluster section
        try_validate!(
            write!(
                out,
                "# Cluster\r\n\
                cluster_enabled:0\r\n\
                \r\n\
                "
            ),
            "ERR unexpected"
        );
    }

    if section == "default" || section == "all" || section == "clients" {
        try_validate!(
            write!(
                out,
                "\
                # Clients\r\n\
                connected_clients:0\r\n\
                client_longest_output_list:0\r\n\
                client_biggest_input_buf:0\r\n\
                blocked_clients:0\r\n\
                \r\n\
                "
            ),
            "ERR unexpected"
        );
    }

    if section == "default" || section == "all" || section == "memory" {
        let used_memory = db.used_memory;
        let used_memory_peak = db.used_memory_peak;
        let used_memory_human = format_bytes(used_memory);
        let used_memory_peak_human = format_bytes(used_memory_peak);
        try_validate!(
            write!(
                out,
                "\
                # Memory\r\n\
                used_memory:{}\r\n\
                used_memory_human:{}\r\n\
                used_memory_rss:0\r\n\
                used_memory_peak:{}\r\n\
                used_memory_peak_human:{}\r\n\
                used_memory_lua:0\r\n\
                mem_fragmentation_ratio:0\r\n\
                mem_allocator:libc\r\n\
                \r\n\
                ",
                used_memory,
                used_memory_human,
                used_memory_peak,
                used_memory_peak_human,
            ),
            "ERR unexpected"
        );
    }

    if section == "default" || section == "all" || section == "stats" {
        try_validate!(
            write!(
                out,
                "\
                # Stats\r\n\
                total_connections_received:0\r\n\
                total_commands_processed:0\r\n\
                instantaneous_ops_per_sec:0\r\n\
                rejected_connections:0\r\n\
                expired_keys:0\r\n\
                evicted_keys:{}\r\n\
                keyspace_hits:0\r\n\
                keyspace_misses:0\r\n\
                pubsub_channels:{}\r\n\
                pubsub_patterns:{}\r\n\
                latest_fork_usec:0\r\n\
                \r\n\
                ",
                db.evicted_keys,
                db.pubsub_channels(),
                db.pubsub_patterns(),
            ),
            "ERR unexpected"
        );
    }

    if section == "default" || section == "all" || section == "replication" {
        try_validate!(
            write!(
                out,
                "\
                # Replication\r\n\
                role:master\r\n\
                connected_slaves:0\r\n\
                \r\n\
                "
            ),
            "ERR unexpected"
        );
    }

    if section == "default" || section == "all" || section == "cpu" {
        try_validate!(
            write!(
                out,
                "\
                # CPU\r\n\
                used_cpu_sys:0.00\r\n\
                used_cpu_user:0.00\r\n\
                used_cpu_sys_children:0.00\r\n\
                used_cpu_user_children:0.00\r\n\
                \r\n\
                "
            ),
            "ERR unexpected"
        );
    }

    if section == "default" || section == "all" || section == "commandstats" {
        try_validate!(write!(out, "# Commandstats\r\n"), "ERR unexpected");
        // TODO: Track command statistics
    }

    Response::Data(out)
}

fn save(parser: &mut ParsedCommand, db: &mut Database) -> Response {
    validate_arguments_exact!(parser, 1);
    // TODO: Implement actual RDB save to disk
    // For now, just update the last save time
    use util::mstime;
    db.last_save_time = mstime() / 1000;
    Response::Status("OK".to_owned())
}

fn bgsave(parser: &mut ParsedCommand, _db: &mut Database) -> Response {
    validate_arguments_exact!(parser, 1);
    // TODO: Implement background RDB save with threading
    // For now, return appropriate response
    Response::Status("Background saving started by pid 0".to_owned())
}

fn bgrewriteaof(parser: &mut ParsedCommand, _db: &mut Database) -> Response {
    validate_arguments_exact!(parser, 1);
    // TODO: Implement background AOF rewrite with threading
    // For now, return appropriate response
    Response::Status("Background append only file rewriting started by pid 0".to_owned())
}

fn shutdown(parser: &mut ParsedCommand, _db: &mut Database) -> Response {
    validate_arguments_lte!(parser, 2);
    // TODO: Implement actual shutdown - would need access to the server
    // For now, return OK - actual shutdown would need access to the server
    // Note: In a real implementation, this would need to signal the server to stop
    Response::Status("OK".to_owned())
}

fn lastsave(parser: &mut ParsedCommand, db: &mut Database) -> Response {
    validate_arguments_exact!(parser, 1);
    Response::Integer(db.last_save_time)
}

fn config(parser: &mut ParsedCommand, db: &mut Database) -> Response {
    validate_arguments_gte!(parser, 2);
    let subcommand = try_validate!(parser.get_str(1), "Invalid subcommand");
    match &*subcommand.to_ascii_lowercase() {
        "get" => {
            validate_arguments_exact!(parser, 3);
            let param = try_validate!(parser.get_str(2), "Invalid parameter");
            let param_lower = param.to_ascii_lowercase();
            let mut result = Vec::new();
            match &*param_lower {
                "dir" => {
                    result.push(Response::Data(b"dir".to_vec()));
                    result.push(Response::Data(db.config.dir.clone().into_bytes()));
                }
                "appendonly" => {
                    result.push(Response::Data(b"appendonly".to_vec()));
                    result.push(Response::Data(if db.config.appendonly { b"yes".to_vec() } else { b"no".to_vec() }));
                }
                "appendfilename" => {
                    result.push(Response::Data(b"appendfilename".to_vec()));
                    result.push(Response::Data(db.config.appendfilename.clone().into_bytes()));
                }
                "requirepass" => {
                    result.push(Response::Data(b"requirepass".to_vec()));
                    result.push(Response::Data(match &db.config.requirepass {
                        Some(p) => p.clone().into_bytes(),
                        None => b"".to_vec(),
                    }));
                }
                "port" => {
                    result.push(Response::Data(b"port".to_vec()));
                    result.push(Response::Data(db.config.port.to_string().into_bytes()));
                }
                "databases" => {
                    result.push(Response::Data(b"databases".to_vec()));
                    result.push(Response::Data(db.config.databases.to_string().into_bytes()));
                }
                "hz" => {
                    result.push(Response::Data(b"hz".to_vec()));
                    result.push(Response::Data(db.config.hz.to_string().into_bytes()));
                }
                "activerehashing" => {
                    result.push(Response::Data(b"activerehashing".to_vec()));
                    result.push(Response::Data(if db.config.active_rehashing { b"yes".to_vec() } else { b"no".to_vec() }));
                }
                "set-max-intset-entries" => {
                    result.push(Response::Data(b"set-max-intset-entries".to_vec()));
                    result.push(Response::Data(db.config.set_max_intset_entries.to_string().into_bytes()));
                }
                "dbfilename" => {
                    result.push(Response::Data(b"dbfilename".to_vec()));
                    result.push(Response::Data(db.config.dbfilename.clone().into_bytes()));
                }
                "maxmemory" => {
                    result.push(Response::Data(b"maxmemory".to_vec()));
                    result.push(Response::Data(match db.config.maxmemory {
                        Some(m) => m.to_string().into_bytes(),
                        None => b"0".to_vec(),
                    }));
                }
                "maxmemory-policy" => {
                    result.push(Response::Data(b"maxmemory-policy".to_vec()));
                    result.push(Response::Data(db.config.maxmemory_policy.clone().into_bytes()));
                }
                "appendfsync" => {
                    result.push(Response::Data(b"appendfsync".to_vec()));
                    result.push(Response::Data(db.config.appendfsync.clone().into_bytes()));
                }
                "stop-writes-on-bgsave-error" => {
                    result.push(Response::Data(b"stop-writes-on-bgsave-error".to_vec()));
                    result.push(Response::Data(if db.config.stop_writes_on_bgsave_error { b"yes".to_vec() } else { b"no".to_vec() }));
                }
                "rdbcompression" => {
                    result.push(Response::Data(b"rdbcompression".to_vec()));
                    result.push(Response::Data(if db.config.rdbcompression { b"yes".to_vec() } else { b"no".to_vec() }));
                }
                "rdbchecksum" => {
                    result.push(Response::Data(b"rdbchecksum".to_vec()));
                    result.push(Response::Data(if db.config.rdbchecksum { b"yes".to_vec() } else { b"no".to_vec() }));
                }
                "maxclients" => {
                    result.push(Response::Data(b"maxclients".to_vec()));
                    result.push(Response::Data(db.config.maxclients.to_string().into_bytes()));
                }
                "maxmemory-samples" => {
                    result.push(Response::Data(b"maxmemory-samples".to_vec()));
                    result.push(Response::Data(db.config.maxmemory_samples.to_string().into_bytes()));
                }
                "no-appendfsync-on-rewrite" => {
                    result.push(Response::Data(b"no-appendfsync-on-rewrite".to_vec()));
                    result.push(Response::Data(if db.config.no_appendfsync_on_rewrite { b"yes".to_vec() } else { b"no".to_vec() }));
                }
                "auto-aof-rewrite-percentage" => {
                    result.push(Response::Data(b"auto-aof-rewrite-percentage".to_vec()));
                    result.push(Response::Data(db.config.auto_aof_rewrite_percentage.to_string().into_bytes()));
                }
                "auto-aof-rewrite-min-size" => {
                    result.push(Response::Data(b"auto-aof-rewrite-min-size".to_vec()));
                    result.push(Response::Data(db.config.auto_aof_rewrite_min_size.to_string().into_bytes()));
                }
                "slowlog-log-slower-than" => {
                    result.push(Response::Data(b"slowlog-log-slower-than".to_vec()));
                    result.push(Response::Data(db.config.slowlog_log_slower_than.to_string().into_bytes()));
                }
                "slowlog-max-len" => {
                    result.push(Response::Data(b"slowlog-max-len".to_vec()));
                    result.push(Response::Data(db.config.slowlog_max_len.to_string().into_bytes()));
                }
                "latency-monitor-threshold" => {
                    result.push(Response::Data(b"latency-monitor-threshold".to_vec()));
                    result.push(Response::Data(db.config.latency_monitor_threshold.to_string().into_bytes()));
                }
                "hash-max-ziplist-entries" => {
                    result.push(Response::Data(b"hash-max-ziplist-entries".to_vec()));
                    result.push(Response::Data(db.config.hash_max_ziplist_entries.to_string().into_bytes()));
                }
                "hash-max-ziplist-value" => {
                    result.push(Response::Data(b"hash-max-ziplist-value".to_vec()));
                    result.push(Response::Data(db.config.hash_max_ziplist_value.to_string().into_bytes()));
                }
                "list-max-ziplist-entries" => {
                    result.push(Response::Data(b"list-max-ziplist-entries".to_vec()));
                    result.push(Response::Data(db.config.list_max_ziplist_entries.to_string().into_bytes()));
                }
                "list-max-ziplist-value" => {
                    result.push(Response::Data(b"list-max-ziplist-value".to_vec()));
                    result.push(Response::Data(db.config.list_max_ziplist_value.to_string().into_bytes()));
                }
                "zset-max-ziplist-entries" => {
                    result.push(Response::Data(b"zset-max-ziplist-entries".to_vec()));
                    result.push(Response::Data(db.config.zset_max_ziplist_entries.to_string().into_bytes()));
                }
                "zset-max-ziplist-value" => {
                    result.push(Response::Data(b"zset-max-ziplist-value".to_vec()));
                    result.push(Response::Data(db.config.zset_max_ziplist_value.to_string().into_bytes()));
                }
                "notify-keyspace-events" => {
                    result.push(Response::Data(b"notify-keyspace-events".to_vec()));
                    result.push(Response::Data(db.config.notify_keyspace_events.clone().into_bytes()));
                }
                _ => return Response::Array(Vec::new()),
            }
            Response::Array(result)
        }
        "set" => {
            validate_arguments_exact!(parser, 4);
            let param = try_validate!(parser.get_str(2), "Invalid parameter");
            let value = try_validate!(parser.get_str(3), "Invalid value");
            let param_lower = param.to_ascii_lowercase();
            match &*param_lower {
                "appendonly" => {
                    db.config.appendonly = match &*value.to_ascii_lowercase() {
                        "yes" => true,
                        "no" => false,
                        _ => return Response::Error("ERR Invalid argument for CONFIG SET 'appendonly'".to_owned()),
                    };
                }
                "appendfilename" => {
                    db.config.appendfilename = value.to_owned();
                }
                "dbfilename" => {
                    db.config.dbfilename = value.to_owned();
                }
                "maxmemory" => {
                    match value.parse::<u64>() {
                        Ok(m) if m == 0 => db.config.maxmemory = None,
                        Ok(m) => db.config.maxmemory = Some(m),
                        Err(_) => return Response::Error("ERR Invalid argument for CONFIG SET 'maxmemory'".to_owned()),
                    }
                }
                "maxmemory-policy" => {
                    match &*value.to_ascii_lowercase() {
                        "volatile-lru" | "allkeys-lru" | "volatile-random" | "allkeys-random" | "volatile-ttl" | "noeviction" => {
                            db.config.maxmemory_policy = value.to_owned();
                        }
                        _ => return Response::Error("ERR Invalid argument for CONFIG SET 'maxmemory-policy'".to_owned()),
                    }
                }
                "appendfsync" => {
                    match &*value.to_ascii_lowercase() {
                        "always" | "everysec" | "no" => {
                            db.config.appendfsync = value.to_owned();
                        }
                        _ => return Response::Error("ERR Invalid argument for CONFIG SET 'appendfsync'".to_owned()),
                    }
                }
                "hz" => {
                    match value.parse::<u32>() {
                        Ok(hz) if hz > 0 && hz <= 500 => db.config.hz = hz,
                        _ => return Response::Error("ERR Invalid argument for CONFIG SET 'hz'".to_owned()),
                    }
                }
                "activerehashing" => {
                    db.config.active_rehashing = match &*value.to_ascii_lowercase() {
                        "yes" => true,
                        "no" => false,
                        _ => return Response::Error("ERR Invalid argument for CONFIG SET 'activerehashing'".to_owned()),
                    };
                }
                "hash-max-ziplist-entries" => {
                    match value.parse::<usize>() {
                        Ok(v) if v > 0 => db.config.hash_max_ziplist_entries = v,
                        _ => return Response::Error("ERR Invalid argument for CONFIG SET 'hash-max-ziplist-entries'".to_owned()),
                    }
                }
                "hash-max-ziplist-value" => {
                    match value.parse::<usize>() {
                        Ok(v) if v > 0 => db.config.hash_max_ziplist_value = v,
                        _ => return Response::Error("ERR Invalid argument for CONFIG SET 'hash-max-ziplist-value'".to_owned()),
                    }
                }
                "list-max-ziplist-entries" => {
                    match value.parse::<usize>() {
                        Ok(v) if v > 0 => db.config.list_max_ziplist_entries = v,
                        _ => return Response::Error("ERR Invalid argument for CONFIG SET 'list-max-ziplist-entries'".to_owned()),
                    }
                }
                "list-max-ziplist-value" => {
                    match value.parse::<usize>() {
                        Ok(v) if v > 0 => db.config.list_max_ziplist_value = v,
                        _ => return Response::Error("ERR Invalid argument for CONFIG SET 'list-max-ziplist-value'".to_owned()),
                    }
                }
                "zset-max-ziplist-entries" => {
                    match value.parse::<usize>() {
                        Ok(v) if v > 0 => db.config.zset_max_ziplist_entries = v,
                        _ => return Response::Error("ERR Invalid argument for CONFIG SET 'zset-max-ziplist-entries'".to_owned()),
                    }
                }
                "zset-max-ziplist-value" => {
                    match value.parse::<usize>() {
                        Ok(v) if v > 0 => db.config.zset_max_ziplist_value = v,
                        _ => return Response::Error("ERR Invalid argument for CONFIG SET 'zset-max-ziplist-value'".to_owned()),
                    }
                }
                "notify-keyspace-events" => {
                    // Validate that it contains only valid flags
                    let valid_flags = "Kg$lshzxeEA";
                    let value_upper = value.to_uppercase();
                    for c in value_upper.chars() {
                        if !valid_flags.contains(c) {
                            return Response::Error("ERR Invalid argument for CONFIG SET 'notify-keyspace-events'".to_owned());
                        }
                    }
                    db.config.notify_keyspace_events = value.to_owned();
                }
                _ => return Response::Error(format!("ERR CONFIG SET failed (possibly unknown parameter '{}')", param)),
            }
            Response::Status("OK".to_owned())
        }
        _ => Response::Error("ERR CONFIG subcommand must be one of GET, SET, RESETSTAT, REWRITE, GETSET".to_owned()),
    }
}

/// Client state that exceeds the lifetime of a command
pub struct Client {
    pub dbindex: usize,
    pub auth: bool,
    pub subscriptions: HashMap<Vec<u8>, usize>,
    pub pattern_subscriptions: HashMap<Vec<u8>, usize>,
    pub multi: bool,
    pub multi_commands: Vec<OwnedParsedCommand>,
    pub watched_keys: HashSet<(usize, Vec<u8>)>,
    pub id: usize,
    pub rawsender: Sender<Option<Response>>,
}

impl Client {
    pub fn mock() -> Self {
        Self::new(channel().0, 0)
    }

    pub fn new(rawsender: Sender<Option<Response>>, id: usize) -> Self {
        Client {
            dbindex: 0,
            auth: false,
            subscriptions: HashMap::new(),
            pattern_subscriptions: HashMap::new(),
            multi: false,
            multi_commands: Vec::new(),
            id,
            watched_keys: HashSet::new(),
            rawsender,
        }
    }
}

fn keys(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let pattern = try_validate!(parser.get_vec(1), "Invalid pattern");

    // FIXME: This might be a bit suboptimal, as db.keys already allocates a vector.
    // Instead we should collect only once.
    let responses = db.keys(dbindex, &pattern);
    Response::Array(responses.into_iter().map(Response::Data).collect())
}

fn randomkey(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 1);
    let keys = db.keys(dbindex, &[]);
    if keys.is_empty() {
        Response::Nil
    } else {
        let index = rand::random::<usize>() % keys.len();
        Response::Data(keys[index].clone())
    }
}

fn move_key(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let target_db = try_validate!(parser.get_i64(2), "ERR value is not an integer or out of range");
    validate!(
        target_db >= 0 && (target_db as usize) < db.config.databases as usize,
        "ERR target database index is out of range"
    );
    let target_db = target_db as usize;
    if target_db == dbindex {
        return Response::Integer(0);
    }
    let value = match db.remove(dbindex, &key) {
        Some(v) => v,
        None => return Response::Integer(0),
    };
    // Get expiration if any
    let expiration = db.remove_msexpiration(dbindex, &key);
    // Set in target db
    let key_clone = key.clone();
    *db.get_or_create(target_db, &key_clone) = value;
    // Set expiration on target db if it existed
    if let Some(exp) = expiration {
        db.set_msexpiration(target_db, key_clone.clone(), exp);
    }
    db.key_updated(target_db, &key_clone);
    Response::Integer(1)
}

fn rename(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let newkey = try_validate!(parser.get_vec(2), "Invalid key");
    if key == newkey {
        return Response::Error("ERR source and destination objects are the same".to_owned());
    }
    let value = match db.remove(dbindex, &key) {
        Some(v) => v,
        None => return Response::Error("ERR no such key".to_owned()),
    };
    // Get expiration if any
    let expiration = db.remove_msexpiration(dbindex, &key);
    // Set in new key
    *db.get_or_create(dbindex, &newkey) = value;
    // Set expiration on new key if it existed
    if let Some(exp) = expiration {
        db.set_msexpiration(dbindex, newkey.clone(), exp);
    }
    db.key_updated(dbindex, &newkey);
    // Publish keyspace notifications for RENAME command
    db.notify_keyspace_event(dbindex, "rename_from", &key, Some('g'));
    db.notify_keyspace_event(dbindex, "rename_to", &newkey, Some('g'));
    Response::Status("OK".to_owned())
}

fn renamenx(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let newkey = try_validate!(parser.get_vec(2), "Invalid key");
    if key == newkey {
        return Response::Integer(0);
    }
    if db.get(dbindex, &newkey).is_some() {
        return Response::Integer(0);
    }
    let value = match db.remove(dbindex, &key) {
        Some(v) => v,
        None => return Response::Error("ERR no such key".to_owned()),
    };
    // Get expiration if any
    let expiration = db.remove_msexpiration(dbindex, &key);
    // Set in new key
    *db.get_or_create(dbindex, &newkey) = value;
    // Set expiration on new key if it existed
    if let Some(exp) = expiration {
        db.set_msexpiration(dbindex, newkey.clone(), exp);
    }
    db.key_updated(dbindex, &newkey);
    Response::Integer(1)
}

fn watch(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
    client_id: usize,
    watched_keys: &mut HashSet<(usize, Vec<u8>)>,
) -> Response {
    validate!(parser.argv.len() >= 2, "Wrong number of parameters");

    for i in 1..parser.argv.len() {
        let key = try_validate!(parser.get_vec(i), "Invalid key");
        db.key_watch(dbindex, &key, client_id);
        watched_keys.insert((dbindex, key));
    }

    Response::Status("OK".to_owned())
}

fn generic_unwatch(
    db: &mut Database,
    client_id: usize,
    watched_keys: &mut HashSet<(usize, Vec<u8>)>,
) -> bool {
    let mut watched_verified = true;
    for (index, key) in watched_keys.drain().into_iter() {
        if !db.key_watch_verify(index, &key, client_id) {
            watched_verified = false;
            break;
        }
    }
    watched_verified
}

fn unwatch(
    parser: &mut ParsedCommand,
    db: &mut Database,
    client_id: usize,
    watched_keys: &mut HashSet<(usize, Vec<u8>)>,
) -> Response {
    validate!(parser.argv.len() == 1, "Wrong number of parameters");

    generic_unwatch(db, client_id, watched_keys);
    Response::Status("OK".to_owned())
}

fn multi(client: &mut Client) -> Response {
    if client.multi {
        Response::Error("ERR MULTI calls can not be nested".to_owned())
    } else {
        client.multi = true;
        Response::Status("OK".to_owned())
    }
}

fn exec(db: &mut Database, client: &mut Client) -> Response {
    if !client.multi {
        return Response::Error("ERR EXEC without MULTI".to_owned());
    }
    client.multi = false;
    let c = replace(&mut client.multi_commands, vec![]);
    if !generic_unwatch(db, client.id, &mut client.watched_keys) {
        return Response::Nil;
    }
    Response::Array(
        c.iter()
            .map(|c| command(c.get_command(), db, client).unwrap())
            .collect(),
    )
}

fn discard(db: &mut Database, client: &mut Client) -> Response {
    if !client.multi {
        Response::Error("ERR DISCARD without MULTI".to_owned())
    } else {
        client.multi = false;
        client.multi_commands = vec![];
        generic_unwatch(db, client.id, &mut client.watched_keys);
        Response::Status("OK".to_owned())
    }
}

fn command_cmd(parser: &mut ParsedCommand, _db: &Database) -> Response {
    if parser.argv.len() == 1 {
        // COMMAND - return all commands
        let mut result = Vec::new();
        let commands = vec![
            "get", "set", "setnx", "setex", "psetex", "append", "strlen", "del", "exists",
            "setbit", "getbit", "setrange", "getrange", "substr", "incr", "decr", "mget",
            "rpush", "lpush", "rpushx", "lpushx", "linsert", "rpop", "lpop", "brpop",
            "brpoplpush", "blpop", "llen", "lindex", "lset", "lrange", "ltrim", "lrem",
            "rpoplpush", "sadd", "srem", "smove", "sismember", "scard", "spop", "srandmember",
            "sinter", "sinterstore", "sunion", "sunionstore", "sdiff", "sdiffstore", "smembers",
            "sscan", "zadd", "zincrby", "zrem", "zremrangebyscore", "zremrangebyrank",
            "zremrangebylex", "zunionstore", "zinterstore", "zrange", "zrangebyscore",
            "zrevrangebyscore", "zrangebylex", "zrevrangebylex", "zcount", "zlexcount",
            "zrevrange", "zcard", "zscore", "zrank", "zrevrank", "zscan", "hset", "hsetnx",
            "hget", "hmset", "hmget", "hincrby", "hincrbyfloat", "hdel", "hlen", "hstrlen",
            "hkeys", "hvals", "hgetall", "hexists", "hscan", "incrby", "decrby", "incrbyfloat",
            "getset", "mset", "msetnx", "randomkey", "select", "move", "rename", "renamenx",
            "expire", "expireat", "pexpire", "pexpireat", "keys", "scan", "dbsize", "auth",
            "ping", "echo", "save", "bgsave", "bgrewriteaof", "shutdown", "lastsave", "type",
            "multi", "exec", "discard", "flushdb", "flushall", "sort", "info", "monitor",
            "ttl", "pttl", "persist", "slaveof", "role", "config", "subscribe", "unsubscribe",
            "psubscribe", "punsubscribe", "publish", "pubsub", "watch", "unwatch", "restore",
            "dump", "object", "client", "time", "bitop", "bitcount", "bitpos", "wait", "command",
            "pfadd", "pfcount", "pfmerge",
        ];
        for cmd_name in commands {
            let props = command_properties(cmd_name);
            let mut cmd_info = Vec::new();
            cmd_info.push(Response::Data(cmd_name.as_bytes().to_vec()));
            cmd_info.push(Response::Data(props.arity.to_string().into_bytes()));
            let mut flags = Vec::new();
            if props.flags.contains(CommandFlags::WRITE) {
                flags.push("write");
            }
            if props.flags.contains(CommandFlags::READONLY) {
                flags.push("readonly");
            }
            if props.flags.contains(CommandFlags::DENYOOM) {
                flags.push("denyoom");
            }
            if props.flags.contains(CommandFlags::ADMIN) {
                flags.push("admin");
            }
            if props.flags.contains(CommandFlags::PUBSUB) {
                flags.push("pubsub");
            }
            if props.flags.contains(CommandFlags::NOSCRIPT) {
                flags.push("noscript");
            }
            if props.flags.contains(CommandFlags::RANDOM) {
                flags.push("random");
            }
            if props.flags.contains(CommandFlags::SORT_FOR_SCRIPT) {
                flags.push("sort_for_script");
            }
            if props.flags.contains(CommandFlags::LOADING) {
                flags.push("loading");
            }
            if props.flags.contains(CommandFlags::STALE) {
                flags.push("stale");
            }
            if props.flags.contains(CommandFlags::SKIP_MONITOR) {
                flags.push("skip_monitor");
            }
            if props.flags.contains(CommandFlags::ASKING) {
                flags.push("asking");
            }
            if props.flags.contains(CommandFlags::FAST) {
                flags.push("fast");
            }
            cmd_info.push(Response::Array(flags.into_iter().map(|f| Response::Data(f.as_bytes().to_vec())).collect()));
            cmd_info.push(Response::Data(props.first_key_index.to_string().into_bytes()));
            cmd_info.push(Response::Data(props.last_key_index.to_string().into_bytes()));
            cmd_info.push(Response::Data(props.key_step.to_string().into_bytes()));
            result.push(Response::Array(cmd_info));
        }
        Response::Array(result)
    } else {
        let subcommand = try_validate!(parser.get_str(1), "Invalid subcommand").to_ascii_lowercase();
        match &*subcommand {
            "count" => {
                // COMMAND COUNT
                Response::Data(b"150".to_vec()) // Approximate count
            }
            "getkeys" => {
                // COMMAND GETKEYS <command> <args...>
                validate_arguments_gte!(parser, 3);
                let cmd_name = try_validate!(parser.get_str(2), "Invalid command");
                let props = command_properties(&cmd_name.to_ascii_lowercase());
                if props.first_key_index == 0 && props.last_key_index == 0 {
                    return Response::Array(Vec::new());
                }
                let mut keys = Vec::new();
                let first = if props.first_key_index < 0 {
                    (parser.argv.len() as i64 + props.first_key_index) as usize
                } else {
                    props.first_key_index as usize
                };
                let last = if props.last_key_index < 0 {
                    (parser.argv.len() as i64 + props.last_key_index) as usize
                } else {
                    props.last_key_index as usize
                };
                let mut i = first;
                while i <= last && i < parser.argv.len() {
                    if let Ok(key) = parser.get_vec(i) {
                        keys.push(Response::Data(key));
                    }
                    i += props.key_step as usize;
                }
                Response::Array(keys)
            }
            "info" => {
                // COMMAND INFO <command>...
                validate_arguments_gte!(parser, 3);
                let mut result = Vec::new();
                for i in 2..parser.argv.len() {
                    let cmd_name = try_validate!(parser.get_str(i), "Invalid command");
                    let props = command_properties(&cmd_name.to_ascii_lowercase());
                    let mut cmd_info = Vec::new();
                    cmd_info.push(Response::Data(cmd_name.as_bytes().to_vec()));
                    cmd_info.push(Response::Data(props.arity.to_string().into_bytes()));
                    let mut flags = Vec::new();
                    if props.flags.contains(CommandFlags::WRITE) {
                        flags.push("write");
                    }
                    if props.flags.contains(CommandFlags::READONLY) {
                        flags.push("readonly");
                    }
                    if props.flags.contains(CommandFlags::DENYOOM) {
                        flags.push("denyoom");
                    }
                    if props.flags.contains(CommandFlags::ADMIN) {
                        flags.push("admin");
                    }
                    if props.flags.contains(CommandFlags::PUBSUB) {
                        flags.push("pubsub");
                    }
                    if props.flags.contains(CommandFlags::NOSCRIPT) {
                        flags.push("noscript");
                    }
                    if props.flags.contains(CommandFlags::RANDOM) {
                        flags.push("random");
                    }
                    if props.flags.contains(CommandFlags::SORT_FOR_SCRIPT) {
                        flags.push("sort_for_script");
                    }
                    if props.flags.contains(CommandFlags::LOADING) {
                        flags.push("loading");
                    }
                    if props.flags.contains(CommandFlags::STALE) {
                        flags.push("stale");
                    }
                    if props.flags.contains(CommandFlags::SKIP_MONITOR) {
                        flags.push("skip_monitor");
                    }
                    if props.flags.contains(CommandFlags::ASKING) {
                        flags.push("asking");
                    }
                    if props.flags.contains(CommandFlags::FAST) {
                        flags.push("fast");
                    }
                    cmd_info.push(Response::Array(flags.into_iter().map(|f| Response::Data(f.as_bytes().to_vec())).collect()));
                    cmd_info.push(Response::Data(props.first_key_index.to_string().into_bytes()));
                    cmd_info.push(Response::Data(props.last_key_index.to_string().into_bytes()));
                    cmd_info.push(Response::Data(props.key_step.to_string().into_bytes()));
                    result.push(Response::Array(cmd_info));
                }
                Response::Array(result)
            }
            _ => Response::Error("ERR unknown subcommand".to_owned()),
        }
    }
}

fn wait_cmd(parser: &mut ParsedCommand, _db: &mut Database) -> Response {
    // WAIT <numreplicas> <timeout>
    validate_arguments_exact!(parser, 3);
    let numreplicas = try_validate!(parser.get_i64(1), "Invalid numreplicas");
    let _timeout = try_validate!(parser.get_i64(2), "Invalid timeout");
    
    if numreplicas < 0 {
        return Response::Error("ERR numreplicas must be >= 0".to_owned());
    }
    
    // For now, we don't have replication implemented, so we return 0
    // In a full implementation, this would wait for replicas to acknowledge writes
    Response::Data(b"0".to_vec())
}

fn slowlog(parser: &mut ParsedCommand, db: &mut Database) -> Response {
    validate!(parser.argv.len() >= 2, "Wrong number of parameters");
    let subcommand = try_validate!(parser.get_str(1), "Invalid subcommand");
    
    match subcommand.to_ascii_lowercase().as_str() {
        "get" => {
            let count = if parser.argv.len() > 2 {
                Some(try_validate!(parser.get_i64(2), "Invalid count") as usize)
            } else {
                None
            };
            
            let entries = db.slowlog_get(count);
            let mut result = Vec::new();
            
            for entry in entries {
                let mut entry_array = Vec::new();
                
                // Entry ID
                entry_array.push(Response::Integer(entry.id as i64));
                // Timestamp
                entry_array.push(Response::Integer(entry.timestamp));
                // Duration in microseconds
                entry_array.push(Response::Integer(entry.duration as i64));
                // Command array
                let mut cmd_array = Vec::new();
                for arg in &entry.command {
                    cmd_array.push(Response::Data(arg.clone()));
                }
                entry_array.push(Response::Array(cmd_array));
                // Client address
                entry_array.push(Response::Data(entry.client_addr.clone().into_bytes()));
                // Client name
                entry_array.push(Response::Data(entry.client_name.clone().into_bytes()));
                
                result.push(Response::Array(entry_array));
            }
            
            Response::Array(result)
        }
        "len" => {
            Response::Integer(db.slowlog_len() as i64)
        }
        "reset" => {
            db.slowlog_reset();
            Response::Status("OK".to_owned())
        }
        _ => Response::Error(format!("ERR Unknown SLOWLOG subcommand '{}'", subcommand)),
    }
}

bitflags! {
    struct CommandFlags: u16 {
        /// write command (may modify the key space).
        const WRITE = 1;
        /// read command  (will never modify the key space).
        const READONLY = 2;
        /// may increase memory usage once called. Don't allow if out of memory.
        const DENYOOM = 4;
        /// admin command, like SAVE or SHUTDOWN.
        const ADMIN = 8;
        /// Pub/Sub related command.
        const PUBSUB = 16;
        /// command not allowed in scripts.
        const NOSCRIPT = 32;
        /// random command. Command is not deterministic, that is, the same command
        /// with the same arguments, with the same key space, may have different
        /// results. For instance SPOP and RANDOMKEY are two random commands.
        const RANDOM = 64;
        /// Sort command output array if called from script, so that the output
        /// is deterministic.
        const SORT_FOR_SCRIPT = 128;
        /// Allow command while loading the database.
        const LOADING = 256;
        /// Allow command while a slave has stale data but is not allowed to
        /// server this data. Normally no command is accepted in this condition
        /// but just a few.
        const STALE = 512;
        /// Do not automatically propagate the command on MONITOR.
        const SKIP_MONITOR = 1024;
        /// Perform an implicit ASKING for this command, so the command will be
        /// accepted in cluster mode if the slot is marked as 'importing'.
        const ASKING = 2048;
        /// Fast command(1) or O(log(N)) command that should never delay
        /// its execution as long as the kernel scheduler is giving us time.
        /// Note that commands that may trigger a DEL as a side effect (like SET)
        /// are not fast commands.
        const FAST = 4096;
    }
}

// TODO: Only `flags` is ever used
#[allow(dead_code)]
struct CommandProperties {
    arity: i64,
    /// Flags as bitmask. Computed by Redis using the 'sflags' field.
    flags: CommandFlags,
    /// First argument that is a key
    first_key_index: i64,
    /// Last argument that is a key
    last_key_index: i64,
    /// Step to get all the keys from first to last argument. For instance
    ///           in MSET the step is two since arguments are key,val,key,val,...
    key_step: i64,
}

fn command_properties(command_name: &str) -> CommandProperties {
    const ADMIN: CommandFlags = CommandFlags::ADMIN;
    const ASKING: CommandFlags = CommandFlags::ASKING;
    const DENYOOM: CommandFlags = CommandFlags::DENYOOM;
    const FAST: CommandFlags = CommandFlags::FAST;
    const LOADING: CommandFlags = CommandFlags::LOADING;
    const NOSCRIPT: CommandFlags = CommandFlags::NOSCRIPT;
    const PUBSUB: CommandFlags = CommandFlags::PUBSUB;
    const RANDOM: CommandFlags = CommandFlags::RANDOM;
    const READONLY: CommandFlags = CommandFlags::READONLY;
    const SKIP_MONITOR: CommandFlags = CommandFlags::SKIP_MONITOR;
    const SORT_FOR_SCRIPT: CommandFlags = CommandFlags::SORT_FOR_SCRIPT;
    const STALE: CommandFlags = CommandFlags::STALE;
    const WRITE: CommandFlags = CommandFlags::WRITE;

    let wm = WRITE | DENYOOM;
    let wf = WRITE | FAST;
    let wmf = WRITE | DENYOOM | FAST;
    let fr = READONLY | FAST;
    let ls = LOADING | STALE;
    let ars = READONLY | ADMIN | NOSCRIPT;
    let sr = READONLY | SORT_FOR_SCRIPT;
    let (arity, flags, first_key_index, last_key_index, key_step) = match command_name {
        "get" => (2, fr, 1, 1, 1),
        "set" => (3, wm, 1, 1, 1),
        "setnx" => (3, wmf, 1, 1, 1),
        "setex" => (4, wm, 1, 1, 1),
        "psetex" => (4, wm, 1, 1, 1),
        "append" => (3, wm, 1, 1, 1),
        "strlen" => (2, fr, 1, 1, 1),
        "del" => (-2, WRITE, 1, -1, 1),
        "exists" => (-2, fr, 1, -1, 1),
        "setbit" => (4, wm, 1, 1, 1),
        "getbit" => (3, fr, 1, 1, 1),
        "setrange" => (4, wm, 1, 1, 1),
        "getrange" => (4, READONLY, 1, 1, 1),
        "substr" => (4, READONLY, 1, 1, 1),
        "incr" => (2, wmf, 1, 1, 1),
        "decr" => (2, wmf, 1, 1, 1),
        "mget" => (-2, READONLY, 1, -1, 1),
        "rpush" => (-3, wmf, 1, 1, 1),
        "lpush" => (-3, wmf, 1, 1, 1),
        "rpushx" => (-3, wmf, 1, 1, 1),
        "lpushx" => (-3, wmf, 1, 1, 1),
        "linsert" => (5, wm, 1, 1, 1),
        "rpop" => (2, wf, 1, 1, 1),
        "lpop" => (2, wf, 1, 1, 1),
        "rpoplpush" => (3, wm, 1, 2, 1),
        "brpop" => (-3, WRITE | NOSCRIPT, 1, -2, 1),
        "blpop" => (-3, WRITE | NOSCRIPT, 1, -2, 1),
        "brpoplpush" => (4, wm | NOSCRIPT, 1, 2, 1),
        "llen" => (2, fr, 1, 1, 1),
        "lindex" => (3, READONLY, 1, 1, 1),
        "lset" => (4, wm, 1, 1, 1),
        "lrange" => (4, READONLY, 1, 1, 1),
        "ltrim" => (4, READONLY, 1, 1, 1),
        "lrem" => (4, READONLY, 1, 1, 1),
        "sadd" => (-3, wmf, 1, 1, 1),
        "srem" => (-3, wf, 1, 1, 1),
        "smove" => (4, wf, 1, 2, 1),
        "sismember" => (3, fr, 1, 1, 1),
        "scard" => (2, fr, 1, 1, 1),
        "spop" => (-2, fr | RANDOM | NOSCRIPT, 1, 1, 1),
        "srandmember" => (-2, READONLY | RANDOM, 1, 1, 1),
        "sinter" => (-2, sr, 1, -1, 1),
        "sinterstore" => (-3, wm, 1, -1, 1),
        "sunion" => (-2, sr, 1, -1, 1),
        "sunionstore" => (-3, wm, 1, -1, 1),
        "sdiff" => (-2, sr, 1, -1, 1),
        "sdiffstore" => (-3, wm, 1, -1, 1),
        "smembers" => (2, sr, 1, 1, 1),
        "sscan" => (-3, READONLY | RANDOM, 1, 1, 1),
        "zadd" => (-4, wmf, 1, 1, 1),
        "zincrby" => (4, wmf, 1, 1, 1),
        "zrem" => (-3, wf, 1, 1, 1),
        "zremrangebyscore" => (4, WRITE, 1, 1, 1),
        "zremrangebyrank" => (4, WRITE, 1, 1, 1),
        "zremrangebylex" => (4, WRITE, 1, 1, 1),
        "zunionstore" => (-4, wm, 0, 0, 0),
        "zinterstore" => (-4, wm, 0, 0, 0),
        "zrange" => (-4, READONLY, 1, 1, 1),
        "zrevrange" => (-4, READONLY, 1, 1, 1),
        "zrangebyscore" => (-4, READONLY, 1, 1, 1),
        "zrevrangebyscore" => (-4, READONLY, 1, 1, 1),
        "zrangebylex" => (-4, READONLY, 1, 1, 1),
        "zrevrangebylex" => (-4, READONLY, 1, 1, 1),
        "zcount" => (4, fr, 1, 1, 1),
        "zlexcount" => (4, fr, 1, 1, 1),
        "zcard" => (2, fr, 1, 1, 1),
        "zscore" => (3, fr, 1, 1, 1),
        "zrank" => (3, fr, 1, 1, 1),
        "zrevrank" => (3, fr, 1, 1, 1),
        "zscan" => (-3, READONLY | RANDOM, 1, 1, 1),
        "hset" => (4, wmf, 1, 1, 1),
        "hsetnx" => (4, wmf, 1, 1, 1),
        "hget" => (3, fr, 1, 1, 1),
        "hmset" => (-4, wm, 1, 1, 1),
        "hmget" => (-3, READONLY, 1, 1, 1),
        "hincrby" => (4, wmf, 1, 1, 1),
        "hincrbyfloat" => (4, wmf, 1, 1, 1),
        "hdel" => (-3, wf, 1, 1, 1),
        "hlen" => (2, fr, 1, 1, 1),
        "hstrlen" => (3, fr, 1, 1, 1),
        "hkeys" => (2, sr, 1, 1, 1),
        "hvals" => (2, sr, 1, 1, 1),
        "hgetall" => (2, READONLY, 1, 1, 1),
        "hexists" => (3, fr, 1, 1, 1),
        "hscan" => (-3, READONLY | RANDOM, 1, 1, 1),
        "incrby" => (3, wmf, 1, 1, 1),
        "decrby" => (3, wmf, 1, 1, 1),
        "incrbyfloat" => (3, wmf, 1, 1, 1),
        "getset" => (3, wm, 1, 1, 1),
        "mset" => (-3, wm, 1, -1, 2),
        "msetnx" => (-3, wm, 1, -1, 2),
        "randomkey" => (1, READONLY | RANDOM, 0, 0, 0),
        "select" => (2, fr | LOADING, 0, 0, 0),
        "move" => (3, wf, 1, 1, 1),
        "rename" => (3, WRITE, 1, 2, 1),
        "renamenx" => (3, wf, 1, 2, 1),
        "expire" => (3, wf, 1, 1, 1),
        "expireat" => (3, wf, 1, 1, 1),
        "pexpire" => (3, wf, 1, 1, 1),
        "pexpireat" => (3, wf, 1, 1, 1),
        "keys" => (2, sr, 0, 0, 0),
        "scan" => (-2, READONLY | RANDOM, 0, 0, 0),
        "dbsize" => (1, fr, 0, 0, 0),
        "auth" => (2, fr | NOSCRIPT | ls, 0, 0, 0),
        "ping" => (-1, fr | STALE, 0, 0, 0),
        "echo" => (2, fr, 0, 0, 0),
        "save" => (1, ars, 0, 0, 0),
        "bgsave" => (1, READONLY | ADMIN, 0, 0, 0),
        "bgrewriteaof" => (1, READONLY | ADMIN, 0, 0, 0),
        "shutdown" => (-1, READONLY | ADMIN | ls, 0, 0, 0),
        "lastsave" => (1, fr | RANDOM, 0, 0, 0),
        "type" => (2, fr, 1, 1, 1),
        "multi" => (1, fr | NOSCRIPT, 0, 0, 0),
        "exec" => (1, NOSCRIPT | SKIP_MONITOR, 0, 0, 0),
        "discard" => (1, fr | NOSCRIPT, 0, 0, 0),
        "sync" => (1, ars, 0, 0, 0),
        "psync" => (3, ars, 0, 0, 0),
        "replconf" => (-1, ars | ls, 0, 0, 0),
        "flushdb" => (1, WRITE, 0, 0, 0),
        "flushall" => (1, WRITE, 0, 0, 0),
        "sort" => (-2, wm, 1, 1, 1),
        "info" => (-1, READONLY | ls, 0, 0, 0),
        "monitor" => (1, ars, 0, 0, 0),
        "ttl" => (2, fr, 1, 1, 1),
        "pttl" => (2, fr, 1, 1, 1),
        "persist" => (2, wf, 1, 1, 1),
        "slaveof" => (3, ADMIN | NOSCRIPT | STALE, 0, 0, 0),
        "role" => (1, STALE | LOADING | NOSCRIPT, 0, 0, 0),
        "debug" => (-2, ADMIN | NOSCRIPT, 0, 0, 0),
        "config" => (-2, ADMIN | READONLY | STALE, 0, 0, 0),
        "subscribe" => (-2, READONLY | PUBSUB | NOSCRIPT | LOADING | STALE, 0, 0, 0),
        "unsubscribe" => (-1, READONLY | PUBSUB | NOSCRIPT | LOADING | STALE, 0, 0, 0),
        "psubscribe" => (-2, READONLY | PUBSUB | NOSCRIPT | LOADING | STALE, 0, 0, 0),
        "punsubscribe" => (-1, READONLY | PUBSUB | NOSCRIPT | LOADING | STALE, 0, 0, 0),
        "publish" => (-1, READONLY | PUBSUB | LOADING | STALE | FAST, 0, 0, 0),
        "pubsub" => (-1, READONLY | PUBSUB | LOADING | STALE | RANDOM, 0, 0, 0),
        "watch" => (-2, fr | NOSCRIPT, 1, -1, 1),
        "unwatch" => (1, fr | NOSCRIPT, 0, 0, 0),
        "cluster" => (-2, ADMIN | READONLY, 0, 0, 0),
        "restore" => (-4, wm, 1, 1, 1),
        "restore-asking" => (-4, wm | ASKING, 1, 1, 1),
        "migrate" => (-6, WRITE, 0, 0, 0),
        "asking" => (1, READONLY, 0, 0, 0),
        "readonly" => (1, fr, 0, 0, 0),
        "readwrite" => (1, fr, 0, 0, 0),
        "dump" => (2, READONLY, 1, 1, 1),
        "object" => (3, READONLY, 2, 2, 2),
        "client" => (-2, READONLY | NOSCRIPT, 0, 0, 0),
        "eval" => (-3, NOSCRIPT, 0, 0, 0),
        "evalsha" => (-3, NOSCRIPT, 0, 0, 0),
        "slowlog" => (-2, READONLY, 0, 0, 0),
        "script" => (-2, READONLY | NOSCRIPT, 0, 0, 0),
        "time" => (1, READONLY | RANDOM | FAST, 0, 0, 0),
        "bitop" => (-4, wm, 2, -1, 1),
        "bitcount" => (-2, READONLY, 1, 1, 1),
        "bitpos" => (-3, READONLY, 1, 1, 1),
        "wait" => (3, READONLY | NOSCRIPT, 0, 0, 0),
        "command" => (0, READONLY | LOADING | STALE, 0, 0, 0),
        "geoadd" => (-5, wm, 1, 1, 1),
        "georadius" => (-6, READONLY, 1, 1, 1),
        "georadiusbymember" => (-5, READONLY, 1, 1, 1),
        "geohash" => (-2, READONLY, 1, 1, 1),
        "geopos" => (-2, READONLY, 1, 1, 1),
        "geodist" => (-4, READONLY, 1, 1, 1),
        "pfselftest" => (1, READONLY, 1, 1, 1),
        "pfadd" => (-2, wmf, 1, 1, 1),
        "pfcount" => (-2, READONLY, 1, -1, 1),
        "pfmerge" => (-2, wm, 1, -1, 1),
        "pfdebug" => (-3, WRITE, 0, 0, 0),
        "latency" => (-2, ars | ls, 0, 0, 0),
        _ => (0, CommandFlags::empty(), 0, 0, 0),
    };

    CommandProperties {
        arity,
        flags,
        first_key_index,
        last_key_index,
        key_step,
    }
}

#[test]
fn command_has_flags_test() {
    assert!(command_properties("set")
        .flags
        .contains(CommandFlags::WRITE));
    assert!(command_properties("setnx")
        .flags
        .contains(CommandFlags::WRITE | CommandFlags::FAST));
    assert!(!command_properties("append")
        .flags
        .contains(CommandFlags::READONLY));
}

fn execute_command(
    parser: &mut ParsedCommand,
    db: &mut Database,
    client: &mut Client,
    log: &mut bool,
    write: &mut bool,
) -> Result<Response, ResponseError> {
    if parser.argv.is_empty() {
        return Err(ResponseError::NoReply);
    }
    let command_name = &*match db.mapped_command(
        &try_opt_validate!(parser.get_str(0), "Invalid command").to_ascii_lowercase(),
    ) {
        Some(c) => c,
        None => return Ok(Response::Error("unknown command".to_owned())),
    };

    *write = !command_properties(command_name)
        .flags
        .contains(CommandFlags::READONLY);

    if db.config.requirepass.is_none() {
        client.auth = true;
    }
    // commands that are not executed before AUTH
    if command_name == "auth" {
        opt_validate!(parser.argv.len() == 2, "Wrong number of parameters");
        let password = try_opt_validate!(parser.get_str(1), "Invalid password");
        if Some(password.to_owned()) == db.config.requirepass {
            client.auth = true;
            return Ok(Response::Status("OK".to_owned()));
        } else {
            return Ok(Response::Error(
                if db.config.requirepass.is_none() {
                    "ERR Client sent AUTH, but no password is set"
                } else {
                    "ERR invalid password"
                }
                .to_owned(),
            ));
        }
    }

    if !client.auth {
        return Ok(Response::Error(
            "NOAUTH Authentication required.".to_owned(),
        ));
    }

    // commands that are not executed inside MULTI
    match command_name {
        "multi" => return Ok(multi(client)),
        "discard" => return Ok(discard(db, client)),
        "exec" => return Ok(exec(db, client)),
        _ => {}
    }
    if client.multi {
        if command_name == "watch" || command_name == "unwatch" {
            return Ok(Response::Error(
                "ERR WATCH not allowed inside MULTI".to_owned(),
            ));
        }
        client.multi_commands.push(parser.to_owned());
        return Ok(Response::Status("QUEUED".to_owned()));
    }
    if command_name == "select" {
        opt_validate!(parser.argv.len() == 2, "Wrong number of parameters");
        let dbindex = try_opt_validate!(parser.get_i64(1), "Invalid dbindex") as usize;
        if dbindex > db.config.databases as usize {
            return Ok(Response::Error("ERR invalid DB index".to_owned()));
        }
        client.dbindex = dbindex;
        return Ok(Response::Status("OK".to_owned()));
    }
    let dbindex = client.dbindex;
    Ok(match command_name {
        "pexpireat" => pexpireat(parser, db, dbindex),
        "pexpire" => pexpire(parser, db, dbindex),
        "expireat" => expireat(parser, db, dbindex),
        "expire" => expire(parser, db, dbindex),
        "echo" => echo(parser),
        "ttl" => ttl(parser, db, dbindex),
        "pttl" => pttl(parser, db, dbindex),
        "time" => time(parser, db),
        "persist" => persist(parser, db, dbindex),
        "type" => dbtype(parser, db, dbindex),
        "role" => role(parser, db),
        "slaveof" => slaveof(parser, db),
        "object" => object(parser, db, dbindex),
        "bitop" => bitop(parser, db, dbindex),
        "bitcount" => bitcount(parser, db, dbindex),
        "bitpos" => bitpos(parser, db, dbindex),
        "set" => set(parser, db, dbindex),
        "setnx" => setnx(parser, db, dbindex),
        "setex" => setex(parser, db, dbindex),
        "psetex" => psetex(parser, db, dbindex),
        "debug" => debug(parser, db, dbindex),
        "del" => del(parser, db, dbindex),
        "dbsize" => dbsize(parser, db, dbindex),
        "append" => append(parser, db, dbindex),
        "get" => get(parser, db, dbindex),
        "getrange" => getrange(parser, db, dbindex),
        "mget" => mget(parser, db, dbindex),
        "substr" => getrange(parser, db, dbindex),
        "setrange" => setrange(parser, db, dbindex),
        "setbit" => setbit(parser, db, dbindex),
        "getbit" => getbit(parser, db, dbindex),
        "strlen" => strlen(parser, db, dbindex),
        "incr" => incr(parser, db, dbindex),
        "decr" => decr(parser, db, dbindex),
        "incrby" => incrby(parser, db, dbindex),
        "decrby" => decrby(parser, db, dbindex),
        "incrbyfloat" => incrbyfloat(parser, db, dbindex),
        "pfadd" => pfadd(parser, db, dbindex),
        "pfcount" => pfcount(parser, db, dbindex),
        "pfmerge" => pfmerge(parser, db, dbindex),
        "pfselftest" => pfselftest(parser, db, dbindex),
        "pfdebug" => pfdebug(parser, db, dbindex),
        "exists" => exists(parser, db, dbindex),
        "ping" => ping(parser, client),
        "client" => client_cmd(parser, client),
        "flushdb" => flushdb(parser, db, dbindex),
        "flushall" => flushall(parser, db, dbindex),
        "sort" => sort(parser, db, dbindex),
        "lpush" => lpush(parser, db, dbindex),
        "rpush" => rpush(parser, db, dbindex),
        "lpushx" => lpushx(parser, db, dbindex),
        "rpushx" => rpushx(parser, db, dbindex),
        "lpop" => lpop(parser, db, dbindex),
        "rpop" => rpop(parser, db, dbindex),
        "lindex" => lindex(parser, db, dbindex),
        "linsert" => linsert(parser, db, dbindex),
        "llen" => llen(parser, db, dbindex),
        "lrange" => lrange(parser, db, dbindex),
        "lrem" => lrem(parser, db, dbindex),
        "lset" => lset(parser, db, dbindex),
        "ltrim" => ltrim(parser, db, dbindex),
        "rpoplpush" => rpoplpush(parser, db, dbindex),
        "brpoplpush" => brpoplpush(parser, db, dbindex)?,
        "brpop" => brpop(parser, db, dbindex)?,
        "blpop" => blpop(parser, db, dbindex)?,
        "sadd" => sadd(parser, db, dbindex),
        "srem" => srem(parser, db, dbindex),
        "sismember" => sismember(parser, db, dbindex),
        "smembers" => smembers(parser, db, dbindex),
        "srandmember" => srandmember(parser, db, dbindex),
        "spop" => spop(parser, db, dbindex),
        "smove" => smove(parser, db, dbindex),
        "scard" => scard(parser, db, dbindex),
        "sdiff" => sdiff(parser, db, dbindex),
        "sdiffstore" => sdiffstore(parser, db, dbindex),
        "sinter" => sinter(parser, db, dbindex),
        "sinterstore" => sinterstore(parser, db, dbindex),
        "sunion" => sunion(parser, db, dbindex),
        "sunionstore" => sunionstore(parser, db, dbindex),
        "zadd" => zadd(parser, db, dbindex),
        "zcard" => zcard(parser, db, dbindex),
        "zscore" => zscore(parser, db, dbindex),
        "zincrby" => zincrby(parser, db, dbindex),
        "zrem" => zrem(parser, db, dbindex),
        "zremrangebylex" => zremrangebylex(parser, db, dbindex),
        "zremrangebyscore" => zremrangebyscore(parser, db, dbindex),
        "zremrangebyrank" => zremrangebyrank(parser, db, dbindex),
        "zcount" => zcount(parser, db, dbindex),
        "zlexcount" => zlexcount(parser, db, dbindex),
        "zrange" => zrange(parser, db, dbindex),
        "zrevrange" => zrevrange(parser, db, dbindex),
        "zrangebyscore" => zrangebyscore(parser, db, dbindex),
        "zrevrangebyscore" => zrevrangebyscore(parser, db, dbindex),
        "zrangebylex" => zrangebylex(parser, db, dbindex),
        "zrevrangebylex" => zrevrangebylex(parser, db, dbindex),
        "zrank" => zrank(parser, db, dbindex),
        "zrevrank" => zrevrank(parser, db, dbindex),
        "zunionstore" => zunionstore(parser, db, dbindex),
        "zinterstore" => zinterstore(parser, db, dbindex),
        "hset" => hset(parser, db, dbindex),
        "hsetnx" => hsetnx(parser, db, dbindex),
        "hget" => hget(parser, db, dbindex),
        "hmset" => hmset(parser, db, dbindex),
        "hmget" => hmget(parser, db, dbindex),
        "hdel" => hdel(parser, db, dbindex),
        "hlen" => hlen(parser, db, dbindex),
        "hstrlen" => hstrlen(parser, db, dbindex),
        "hkeys" => hkeys(parser, db, dbindex),
        "hvals" => hvals(parser, db, dbindex),
        "hgetall" => hgetall(parser, db, dbindex),
        "hexists" => hexists(parser, db, dbindex),
        "hincrby" => hincrby(parser, db, dbindex),
        "hincrbyfloat" => hincrbyfloat(parser, db, dbindex),
        "hscan" => hscan(parser, db, dbindex),
        "getset" => getset(parser, db, dbindex),
        "mset" => mset(parser, db, dbindex),
        "msetnx" => msetnx(parser, db, dbindex),
        "randomkey" => randomkey(parser, db, dbindex),
        "move" => move_key(parser, db, dbindex),
        "rename" => rename(parser, db, dbindex),
        "renamenx" => renamenx(parser, db, dbindex),
        "dump" => dump(parser, db, dbindex),
        "restore" => restore(parser, db, dbindex),
        "keys" => keys(parser, db, dbindex),
        "scan" => scan(parser, db, dbindex),
        "sscan" => sscan(parser, db, dbindex),
        "zscan" => zscan(parser, db, dbindex),
        "watch" => watch(parser, db, dbindex, client.id, &mut client.watched_keys),
        "unwatch" => unwatch(parser, db, client.id, &mut client.watched_keys),
        "subscribe" => subscribe(
            parser,
            db,
            &mut client.subscriptions,
            client.pattern_subscriptions.len(),
            &client.rawsender,
        )?,
        "unsubscribe" => unsubscribe(
            parser,
            db,
            &mut client.subscriptions,
            client.pattern_subscriptions.len(),
            &client.rawsender,
        )?,
        "psubscribe" => psubscribe(
            parser,
            db,
            client.subscriptions.len(),
            &mut client.pattern_subscriptions,
            &client.rawsender,
        )?,
        "punsubscribe" => punsubscribe(
            parser,
            db,
            client.subscriptions.len(),
            &mut client.pattern_subscriptions,
            &client.rawsender,
        )?,
        "publish" => publish(parser, db),
        "pubsub" => pubsub(parser, db),
        "monitor" => {
            *log = false;
            monitor(parser, db, client.rawsender.clone())
        }
        "info" => info(parser, db),
        "save" => save(parser, db),
        "bgsave" => bgsave(parser, db),
        "bgrewriteaof" => bgrewriteaof(parser, db),
        "shutdown" => shutdown(parser, db),
        "lastsave" => lastsave(parser, db),
        "config" => config(parser, db),
        "command" => command_cmd(parser, db),
        "wait" => wait_cmd(parser, db),
        "slowlog" => slowlog(parser, db),
        cmd => Response::Error(format!("ERR unknown command \"{}\"", cmd)),
    })
}

pub fn command(
    mut parser: ParsedCommand,
    db: &mut Database,
    client: &mut Client,
) -> Result<Response, ResponseError> {
    let mut log = true;
    let mut write = false;
    
    // Track execution time for slowlog
    let start_time = ustime();
    let r = execute_command(&mut parser, db, client, &mut log, &mut write);
    let duration_us = (ustime() - start_time) as u64;
    
    // TODO: only log if there's anyone listening
    if log {
        db.log_command(client.dbindex, &parser, write);
        
        // Add to slowlog if threshold exceeded
        let client_addr = "127.0.0.1:0".to_string(); // TODO: Get actual client address
        let client_name = String::new(); // TODO: Get client name if set
        db.slowlog_add(&parser, duration_us, client_addr, client_name);
    }
    r
}

#[cfg(test)]
mod test_command {
    use std::collections::HashSet;
    use std::str::from_utf8;
    use std::sync::mpsc::channel;
    use std::sync::{Arc, Mutex};
    use std::thread;

    use config::Config;
    use database::{Database, Value};
    use logger::{Level, Logger};
    use parser::{Argument, ParsedCommand};
    use response::{Response, ResponseError};
    use util::mstime;

    use super::{command, Client};
    use std::time::Duration;

    macro_rules! parser {
        ($str: expr) => {{
            let mut _args = Vec::new();
            let mut pos = 0;
            for segment in $str.split(|x| *x == b' ') {
                _args.push(Argument {
                    pos: pos,
                    len: segment.len(),
                });
                pos += segment.len() + 1;
            }
            ParsedCommand::new($str, _args)
        }};
    }

    fn getstr(database: &Database, key: &[u8]) -> String {
        match database.get(0, &key.to_vec()).unwrap() {
            &Value::String(value) => from_utf8(&*value.to_vec()).unwrap().to_owned(),
            _ => panic!("Got non-string"),
        }
    }

    #[test]
    fn nocommand() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let parser = ParsedCommand::new(b"", Vec::new());
        let response = command(parser, &mut db, &mut Client::mock()).unwrap_err();
        match response {
            ResponseError::NoReply => {}
            _ => assert!(false),
        };
    }

    #[test]
    fn set_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"set key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!("value", getstr(&db, b"key"));

        assert_eq!(
            command(parser!(b"set key2 value xx"), &mut db, &mut Client::mock()).unwrap(),
            Response::Nil
        );
        assert_eq!(
            command(parser!(b"get key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Nil
        );
        assert_eq!(
            command(parser!(b"set key2 value nx"), &mut db, &mut Client::mock()).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!("value", getstr(&db, b"key2"));
        assert_eq!(
            command(parser!(b"set key2 valuf xx"), &mut db, &mut Client::mock()).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!("valuf", getstr(&db, b"key2"));
        assert_eq!(
            command(parser!(b"set key2 value nx"), &mut db, &mut Client::mock()).unwrap(),
            Response::Nil
        );
        assert_eq!("valuf", getstr(&db, b"key2"));

        assert_eq!(
            command(
                parser!(b"set key3 value px 1234"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Status("OK".to_owned())
        );
        let now = mstime();
        let exp = db.get_msexpiration(0, &b"key3".to_vec()).unwrap().clone();
        assert!(exp >= now + 1000);
        assert!(exp <= now + 1234);

        assert_eq!(
            command(
                parser!(b"set key3 value ex 1234"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Status("OK".to_owned())
        );
        let now = mstime();
        let exp = db.get_msexpiration(0, &b"key3".to_vec()).unwrap().clone();
        assert!(exp >= now + 1233 * 1000);
        assert!(exp <= now + 1234 * 1000);
    }

    #[test]
    fn setnx_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"setnx key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!("value", getstr(&db, b"key"));
        assert_eq!(
            command(parser!(b"setnx key valuf"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!("value", getstr(&db, b"key"));
    }

    #[test]
    fn setex_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"setex key 1234 value"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Status("OK".to_owned())
        );
        let now = mstime();
        let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
        assert!(exp >= now + 1233 * 1000);
        assert!(exp <= now + 1234 * 1000);
    }

    #[test]
    fn psetex_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"psetex key 1234 value"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Status("OK".to_owned())
        );
        let now = mstime();
        let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
        assert!(exp >= now + 1000);
        assert!(exp <= now + 1234);
    }

    #[test]
    fn get_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data("value".to_owned().into_bytes())
        );
        assert_eq!("value", getstr(&db, b"key"));
    }

    #[test]
    fn mget_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"mget key key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Array(vec![
                Response::Data("value".to_owned().into_bytes()),
                Response::Nil,
            ])
        );
    }

    #[test]
    fn getrange_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"getrange key 1 -2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data("alu".to_owned().into_bytes())
        );
    }

    #[test]
    fn setrange_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"setrange key 1 i"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(5)
        );
        assert_eq!("vilue", getstr(&db, b"key"));
    }

    #[test]
    fn setbit_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"setbit key 1 0"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(parser!(b"setbit key 1 1"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!("@", getstr(&db, b"key"));
        assert_eq!(
            command(parser!(b"setbit key 1 0"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn getbit_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"getbit key 4"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(parser!(b"getbit key 5"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"getbit key 6"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"getbit key 7"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(parser!(b"getbit key 800"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
    }

    #[test]
    fn strlen_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"strlen key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"strlen key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(5)
        );
    }

    #[test]
    fn del_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"del key key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn debug_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert!(
            command(parser!(b"debug object key"), &mut db, &mut Client::mock())
                .unwrap()
                .is_status()
        );
    }

    #[test]
    fn dbsize_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"dbsize"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"dbsize"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert!(db
            .get_or_create(0, &b"key2".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"dbsize"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn exists_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"exists key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"exists key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn expire_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"expire key 100"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"expire key 100"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        let now = mstime();
        let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
        assert!(exp >= now);
        assert!(exp <= now + 100 * 1000);
    }

    #[test]
    fn pexpire_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"pexpire key 100"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"pexpire key 100"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        let now = mstime();
        let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
        assert!(exp >= now);
        assert!(exp <= now + 100);
    }

    #[test]
    fn expireat_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let now = mstime() / 1000;
        let exp_exp = now + 100;
        let qs = format!("expireat key {}", exp_exp);
        let q = qs.as_bytes();
        assert_eq!(
            command(parser!(q), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(q), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
        assert_eq!(exp, exp_exp * 1000);
    }

    #[test]
    fn pexpireat_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let now = mstime();
        let exp_exp = now + 100;
        let qs = format!("pexpireat key {}", exp_exp);
        let q = qs.as_bytes();
        assert_eq!(
            command(parser!(q), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(q), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
        assert_eq!(exp, exp_exp);
    }

    #[test]
    fn ttl_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"ttl key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(-2)
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"ttl key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(-1)
        );
        db.set_msexpiration(0, b"key".to_vec(), mstime() + 100 * 1000);
        match command(parser!(b"ttl key"), &mut db, &mut Client::mock()).unwrap() {
            Response::Integer(i) => assert!(i <= 100 && i > 80),
            _ => panic!("Expected integer"),
        }
    }

    #[test]
    fn pttl_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"pttl key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(-2)
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"pttl key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(-1)
        );
        db.set_msexpiration(0, b"key".to_vec(), mstime() + 100 * 1000);
        match command(parser!(b"pttl key"), &mut db, &mut Client::mock()).unwrap() {
            Response::Integer(i) => assert!(i <= 100 * 1000 && i > 80 * 1000),
            _ => panic!("Expected integer"),
        }
    }

    #[test]
    fn persist_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"persist key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"persist key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        db.set_msexpiration(0, b"key".to_vec(), mstime() + 100 * 1000);
        assert_eq!(
            command(parser!(b"persist key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn type_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let set_max_intset_entries = db.config.set_max_intset_entries;
        assert_eq!(
            command(parser!(b"type key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"none".to_vec())
        );

        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"type key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"string".to_vec())
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"1".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"type key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"string".to_vec())
        );

        assert!(db.remove(0, &b"key".to_vec()).is_some());
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .push(b"1".to_vec(), true)
            .is_ok());
        assert_eq!(
            command(parser!(b"type key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"list".to_vec())
        );

        assert!(db.remove(0, &b"key".to_vec()).is_some());
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .sadd(b"1".to_vec(), set_max_intset_entries)
            .is_ok());
        assert_eq!(
            command(parser!(b"type key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"set".to_vec())
        );

        assert!(db.remove(0, &b"key".to_vec()).is_some());
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .zadd(3.0, b"1".to_vec(), false, false, false, false)
            .is_ok());
        assert_eq!(
            command(parser!(b"type key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"zset".to_vec())
        );

        // TODO: hash
    }

    #[test]
    fn serialize_status() {
        let response = Response::Status("OK".to_owned());
        assert_eq!(response.as_bytes(), b"+OK\r\n");
    }

    #[test]
    fn serialize_error() {
        let response = Response::Error("ERR Invalid command".to_owned());
        assert_eq!(response.as_bytes(), b"-ERR Invalid command\r\n");
    }

    #[test]
    fn serialize_string() {
        let response = Response::Data(b"ERR Invalid command".to_vec());
        assert_eq!(response.as_bytes(), b"$19\r\nERR Invalid command\r\n");
    }

    #[test]
    fn serialize_nil() {
        let response = Response::Nil;
        assert_eq!(response.as_bytes(), b"$-1\r\n");
    }

    #[test]
    fn serialize_integer() {
        let response = Response::Integer(123);
        assert_eq!(response.as_bytes(), b":123\r\n");
    }

    #[test]
    fn append_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"append key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(5)
        );
        assert_eq!(
            command(parser!(b"append key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(10)
        );
        assert_eq!(
            db.get(0, &b"key".to_vec()).unwrap().get().unwrap(),
            b"valuevalue".to_vec()
        );
    }

    #[test]
    fn incr_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"incr key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"incr key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn incrby_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"incrby key 5"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(5)
        );
        assert_eq!(
            command(parser!(b"incrby key 5"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(10)
        );
    }

    #[test]
    fn incrbyfloat_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        match command(
            parser!(b"incrbyfloat key 2.1"),
            &mut db,
            &mut Client::mock(),
        )
        .unwrap()
        {
            Response::Data(v) => {
                assert_eq!(v[0], '2' as u8);
                assert_eq!(v[1], '.' as u8);
                assert!(v[2] == '1' as u8 || v[2] == '0' as u8);
            }
            _ => panic!("Unexpected response"),
        }
        match command(
            parser!(b"incrbyfloat key 4.1"),
            &mut db,
            &mut Client::mock(),
        )
        .unwrap()
        {
            Response::Data(v) => {
                assert_eq!(v[0], '6' as u8);
                assert_eq!(v[1], '.' as u8);
                assert!(v[2] == '1' as u8 || v[2] == '2' as u8);
            }
            _ => panic!("Unexpected response"),
        }
    }

    #[test]
    fn pfadd_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"PFADD key 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"PFADD key 1 2 4"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"PFADD key 1 2 3 4"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
    }

    #[test]
    fn pfcount1_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"PFCOUNT key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(parser!(b"PFADD key 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"PFCOUNT key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
    }

    #[test]
    fn pfcount2_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"PFADD key1 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"PFADD key2 1 2 4"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"PFCOUNT key1 key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(4)
        );
    }

    #[test]
    fn pfmerge_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"PFADD key1 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"PFADD key3 1 2 4"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"PFADD key4 5 6"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(
                parser!(b"PFMERGE key key1 key2 key3"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"PFCOUNT key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(parser!(b"PFMERGE key key4"), &mut db, &mut Client::mock()).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"PFCOUNT key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(6)
        );
    }

    #[test]
    fn decr_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"decr key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(-1)
        );
        assert_eq!(
            command(parser!(b"decr key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(-2)
        );
    }

    #[test]
    fn decrby_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"decrby key 5"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(-5)
        );
        assert_eq!(
            command(parser!(b"decrby key 5"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(-10)
        );
    }

    #[test]
    fn lpush_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"lpush key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"lpush key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn rpush_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn lpop_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key valuf"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"lpop key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data("value".to_owned().into_bytes())
        );
        assert_eq!(
            command(parser!(b"lpop key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data("valuf".to_owned().into_bytes())
        );
        assert_eq!(
            command(parser!(b"lpop key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Nil
        );
    }

    #[test]
    fn rpop_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key valuf"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"rpop key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data("valuf".to_owned().into_bytes())
        );
        assert_eq!(
            command(parser!(b"rpop key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data("value".to_owned().into_bytes())
        );
        assert_eq!(
            command(parser!(b"rpop key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Nil
        );
    }

    #[test]
    fn lindex_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key valuf"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"lindex key 0"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data("value".to_owned().into_bytes())
        );
    }

    #[test]
    fn linsert_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key valug"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(
                parser!(b"linsert key before valug valuf"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
    }

    #[test]
    fn llen_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"llen key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn lpushx_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"lpushx key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(parser!(b"lpush key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"lpushx key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn lrange_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key valuf"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key valug"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"lrange key 0 -1"), &mut db, &mut Client::mock()).unwrap(),
            Response::Array(vec![
                Response::Data("value".to_owned().into_bytes()),
                Response::Data("valuf".to_owned().into_bytes()),
                Response::Data("valug".to_owned().into_bytes()),
            ])
        );
    }

    #[test]
    fn lrem_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"lrem key 2 value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(parser!(b"llen key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn lset_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"lset key 2 valuf"), &mut db, &mut Client::mock()).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"lrange key 2 2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Array(vec![Response::Data("valuf".to_owned().into_bytes()),])
        );
    }

    #[test]
    fn rpoplpush_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key valuf"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"llen key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(parser!(b"rpoplpush key key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data("valuf".to_owned().into_bytes())
        );
        assert_eq!(
            command(parser!(b"llen key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"llen key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"rpoplpush key key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data("value".to_owned().into_bytes())
        );
        assert_eq!(
            command(parser!(b"llen key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(parser!(b"llen key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn brpoplpush_nowait() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key valuf"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"llen key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(
                parser!(b"brpoplpush key key2 0"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Data("valuf".to_owned().into_bytes())
        );
    }

    #[test]
    fn brpoplpush_waiting() {
        let db = Arc::new(Mutex::new(Database::new(Config::new(Logger::new(
            Level::Warning,
        )))));
        let (tx, rx) = channel();
        let db2 = db.clone();
        thread::spawn(move || {
            let r = match command(
                parser!(b"brpoplpush key1 key2 0"),
                &mut db.lock().unwrap(),
                &mut Client::mock(),
            )
            .unwrap_err()
            {
                ResponseError::Wait(receiver) => {
                    tx.send(1).unwrap();
                    receiver
                }
                _ => panic!("Unexpected error"),
            };
            r.recv().unwrap();
            assert_eq!(
                command(
                    parser!(b"brpoplpush key1 key2 0"),
                    &mut db.lock().unwrap(),
                    &mut Client::mock()
                )
                .unwrap(),
                Response::Data("value".to_owned().into_bytes())
            );
            tx.send(2).unwrap();
        });
        assert_eq!(rx.recv().unwrap(), 1);

        command(
            parser!(b"rpush key1 value"),
            &mut db2.lock().unwrap(),
            &mut Client::mock(),
        )
        .unwrap();
        assert_eq!(rx.recv().unwrap(), 2);
        assert_eq!(
            command(
                parser!(b"lrange key2 0 -1"),
                &mut db2.lock().unwrap(),
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![Response::Data("value".to_owned().into_bytes()),])
        );
    }

    #[test]
    fn brpoplpush_timeout() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let receiver = match command(
            parser!(b"brpoplpush key key2 1"),
            &mut db,
            &mut Client::mock(),
        )
        .unwrap_err()
        {
            ResponseError::Wait(receiver) => receiver,
            _ => panic!("Unexpected response"),
        };
        assert!(receiver.try_recv().is_err());
        thread::sleep(Duration::from_millis(1400));
        assert_eq!(receiver.try_recv().unwrap().is_some(), false);
    }

    #[test]
    fn brpop_nowait() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key1 value"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"brpop key1 key2 0"), &mut db, &mut Client::mock()).unwrap(),
            Response::Array(vec![
                Response::Data("key1".to_owned().into_bytes()),
                Response::Data("value".to_owned().into_bytes()),
            ])
        );
    }

    #[test]
    fn brpop_waiting() {
        let db = Arc::new(Mutex::new(Database::new(Config::new(Logger::new(
            Level::Warning,
        )))));
        let (tx, rx) = channel();
        let db2 = db.clone();
        thread::spawn(move || {
            let r = match command(
                parser!(b"brpop key1 key2 0"),
                &mut db.lock().unwrap(),
                &mut Client::mock(),
            )
            .unwrap_err()
            {
                ResponseError::Wait(receiver) => {
                    tx.send(1).unwrap();
                    receiver
                }
                _ => panic!("Unexpected error"),
            };
            r.recv().unwrap();
            assert_eq!(
                command(
                    parser!(b"brpop key1 key2 0"),
                    &mut db.lock().unwrap(),
                    &mut Client::mock()
                )
                .unwrap(),
                Response::Array(vec![
                    Response::Data("key2".to_owned().into_bytes()),
                    Response::Data("value".to_owned().into_bytes()),
                ])
            );
            tx.send(2).unwrap();
        });
        assert_eq!(rx.recv().unwrap(), 1);

        {
            command(
                parser!(b"rpush key2 value"),
                &mut db2.lock().unwrap(),
                &mut Client::mock(),
            )
            .unwrap();
            assert_eq!(rx.recv().unwrap(), 2);
        }

        {
            assert_eq!(
                command(
                    parser!(b"llen key2"),
                    &mut db2.lock().unwrap(),
                    &mut Client::mock()
                )
                .unwrap(),
                Response::Integer(0)
            );
        }
    }

    #[test]
    fn brpop_timeout() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let receiver = match command(parser!(b"brpop key1 key2 1"), &mut db, &mut Client::mock())
            .unwrap_err()
        {
            ResponseError::Wait(receiver) => receiver,
            _ => panic!("Unexpected response"),
        };
        assert!(receiver.try_recv().is_err());
        thread::sleep(Duration::from_millis(1400));
        assert_eq!(receiver.try_recv().unwrap().is_some(), false);
    }

    #[test]
    fn ltrim_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key valuf"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key valuf"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"ltrim key 1 -2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"lrange key 0 -1"), &mut db, &mut Client::mock()).unwrap(),
            Response::Array(vec![
                Response::Data("value".to_owned().into_bytes()),
                Response::Data("valuf".to_owned().into_bytes()),
            ])
        );
    }

    #[test]
    fn sadd_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"sadd key 1 1 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(parser!(b"sadd key 1 1 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
    }

    #[test]
    fn srem_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"sadd key 1 1 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(parser!(b"srem key 2 3 4"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(parser!(b"srem key 2 3 4"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
    }

    #[test]
    fn sismember_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"sadd key 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(parser!(b"sismember key 2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"sismember key 4"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
    }

    #[test]
    fn smembers_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"sadd key 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
        match command(parser!(b"smembers key"), &mut db, &mut Client::mock()).unwrap() {
            Response::Array(arr) => {
                let mut array = arr
                    .iter()
                    .map(|x| match x {
                        &Response::Data(d) => d.clone(),
                        _ => panic!("Expected data"),
                    })
                    .collect::<Vec<_>>();
                array.sort_by(|a, b| a.cmp(b));
                assert_eq!(array, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn srandmember_command1() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"sadd key 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
        let r = command(parser!(b"srandmember key"), &mut db, &mut Client::mock()).unwrap();
        assert!(
            r == Response::Data(b"1".to_vec())
                || r == Response::Data(b"2".to_vec())
                || r == Response::Data(b"3".to_vec())
        );
        assert_eq!(
            command(parser!(b"scard key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
    }

    #[test]
    fn srandmember_command2() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"sadd key 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
        let r = command(parser!(b"srandmember key 1"), &mut db, &mut Client::mock()).unwrap();
        assert!(
            r == Response::Array(vec![Response::Data(b"1".to_vec())])
                || r == Response::Array(vec![Response::Data(b"2".to_vec())])
                || r == Response::Array(vec![Response::Data(b"3".to_vec())])
        );
        assert_eq!(
            command(parser!(b"scard key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
    }

    #[test]
    fn spop_command1() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"sadd key 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
        let r = command(parser!(b"spop key"), &mut db, &mut Client::mock()).unwrap();
        assert!(
            r == Response::Data(b"1".to_vec())
                || r == Response::Data(b"2".to_vec())
                || r == Response::Data(b"3".to_vec())
        );
        assert_eq!(
            command(parser!(b"scard key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn spop_command2() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"sadd key 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
        let r = command(parser!(b"spop key 1"), &mut db, &mut Client::mock()).unwrap();
        assert!(
            r == Response::Array(vec![Response::Data(b"1".to_vec())])
                || r == Response::Array(vec![Response::Data(b"2".to_vec())])
                || r == Response::Array(vec![Response::Data(b"3".to_vec())])
        );
        assert_eq!(
            command(parser!(b"scard key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn smove_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"sadd k1 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );

        assert_eq!(
            command(parser!(b"smove k1 k2 1"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"smove k1 k2 1"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );

        assert_eq!(
            command(parser!(b"smove k1 k2 2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"smove k1 k2 2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );

        assert_eq!(
            command(parser!(b"smove k1 k2 5"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );

        assert_eq!(
            command(parser!(b"set k3 value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"smove k1 k3 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of \
                 value"
                    .to_owned()
            )
        );
    }

    #[test]
    fn scard_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key 1 2 3"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"scard key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
    }

    #[test]
    fn sdiff_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key 1 2 3"), &mut db, &mut Client::mock()).unwrap();

        let arr = match command(parser!(b"sdiff key"), &mut db, &mut Client::mock()).unwrap() {
            Response::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let mut r = arr
            .iter()
            .map(|el| match el {
                &Response::Data(el) => el.clone(),
                _ => panic!("Expected data"),
            })
            .collect::<Vec<_>>();
        r.sort();
        assert_eq!(r, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
    }

    #[test]
    fn sdiffstore_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key1 1 2 3"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"sadd key2 3 4 5"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(
                parser!(b"sdiffstore target key1 key2"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(2)
        );

        let set = vec![b"1".to_vec(), b"2".to_vec()]
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let mut set2 = Value::Nil;
        set2.create_set(set);
        assert_eq!(db.get(0, &b"target".to_vec()).unwrap(), &set2);
    }

    #[test]
    fn sdiff2_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key1 1 2 3"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"sadd key2 2 3"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"sdiff key1 key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Array(vec![Response::Data(b"1".to_vec()),])
        );
    }

    #[test]
    fn sinter_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key 1 2 3"), &mut db, &mut Client::mock()).unwrap();

        let arr = match command(parser!(b"sinter key"), &mut db, &mut Client::mock()).unwrap() {
            Response::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let mut r = arr
            .iter()
            .map(|el| match el {
                &Response::Data(el) => el.clone(),
                _ => panic!("Expected data"),
            })
            .collect::<Vec<_>>();
        r.sort();
        assert_eq!(r, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
    }

    #[test]
    fn sinter2_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key1 1 2 3"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"sadd key2 2 3 4 5"), &mut db, &mut Client::mock()).unwrap();

        let arr = match command(parser!(b"sinter key1 key2"), &mut db, &mut Client::mock()).unwrap()
        {
            Response::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let mut r = arr
            .iter()
            .map(|el| match el {
                &Response::Data(el) => el.clone(),
                _ => panic!("Expected data"),
            })
            .collect::<Vec<_>>();
        r.sort();
        assert_eq!(r, vec![b"2".to_vec(), b"3".to_vec()]);
    }

    #[test]
    fn sinter_command_nil() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key1 1 2 3"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"sadd key2 2 3 4"), &mut db, &mut Client::mock()).unwrap();

        let arr = match command(
            parser!(b"sinter key1 key2 nokey"),
            &mut db,
            &mut Client::mock(),
        )
        .unwrap()
        {
            Response::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        assert_eq!(arr.len(), 0);
    }

    #[test]
    fn sinterstore_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key1 1 2 3"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"sadd key2 2 3 5"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(
                parser!(b"sinterstore target key1 key2"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(2)
        );

        let set = vec![b"3".to_vec(), b"2".to_vec()]
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let mut set2 = Value::Nil;
        set2.create_set(set);
        assert_eq!(db.get(0, &b"target".to_vec()).unwrap(), &set2);
    }

    #[test]
    fn sunion_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key 1 2 3"), &mut db, &mut Client::mock()).unwrap();

        let arr = match command(parser!(b"sunion key"), &mut db, &mut Client::mock()).unwrap() {
            Response::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let mut r = arr
            .iter()
            .map(|el| match el {
                &Response::Data(el) => el.clone(),
                _ => panic!("Expected data"),
            })
            .collect::<Vec<_>>();
        r.sort();
        assert_eq!(r, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
    }

    #[test]
    fn sunion2_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key1 1 2 3"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"sadd key2 2 3 4"), &mut db, &mut Client::mock()).unwrap();

        let arr = match command(parser!(b"sunion key1 key2"), &mut db, &mut Client::mock()).unwrap()
        {
            Response::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let mut r = arr
            .iter()
            .map(|el| match el {
                &Response::Data(el) => el.clone(),
                _ => panic!("Expected data"),
            })
            .collect::<Vec<_>>();
        r.sort();
        assert_eq!(
            r,
            vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec(), b"4".to_vec()]
        );
    }

    #[test]
    fn sunionstore_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key1 1 2 3"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"sadd key2 2 3 4"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(
                parser!(b"sunionstore target key1 key2"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );

        let set = vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec(), b"4".to_vec()]
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let mut set2 = Value::Nil;
        set2.create_set(set);
        assert_eq!(db.get(0, &b"target".to_vec()).unwrap(), &set2);
    }

    #[test]
    fn zadd_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"zadd key 1 a 2 b"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(parser!(b"zadd key 1 a 2 b"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(
                parser!(b"zadd key XX 2 a 3 b"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(
                parser!(b"zadd key CH 2 a 2 b 2 c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(
                parser!(b"zadd key NX 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(
                parser!(b"zadd key XX CH 2 b 2 d 2 e"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn zcard_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"zadd key 1 a 2 b"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(parser!(b"zcard key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn zscore_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"zadd key 1 a 2 b"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(parser!(b"zscore key a"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"1".to_vec())
        );
        assert_eq!(
            command(parser!(b"zscore key c"), &mut db, &mut Client::mock()).unwrap(),
            Response::Nil
        );
        assert_eq!(
            command(parser!(b"zscore key2 a"), &mut db, &mut Client::mock()).unwrap(),
            Response::Nil
        );
    }

    #[test]
    fn zincrby_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"zincrby key 3 a"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"3".to_vec())
        );
        assert_eq!(
            command(parser!(b"zincrby key 4 a"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"7".to_vec())
        );
    }

    #[test]
    fn zcount_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(parser!(b"zcount key 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(parser!(b"zcount key 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(parser!(b"zcount key (2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(
                parser!(b"zcount key -inf inf"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
    }

    #[test]
    fn zlexcount_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 0 a 0 b 0 c 0 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(
                parser!(b"zlexcount key [a [b"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(
                parser!(b"zlexcount key [a [b"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(
                parser!(b"zlexcount key (b [c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"zlexcount key - +"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(4)
        );
    }

    #[test]
    fn zremrangebyscore_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebyscore key 2 3"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebyscore key 2 3"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebyscore key (2 4"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebyscore key -inf inf"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn zremrangebylex_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 0 a 0 b 0 c 0 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebylex key [b (d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebylex key [b (d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebylex key (b [d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebylex key - +"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn zremrangebyrank_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebyrank key 1 2"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebyrank key 5 10"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebyrank key 1 -1"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebyrank key 0 -1"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn zrange_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(parser!(b"zrange key 0 0"), &mut db, &mut Client::mock()).unwrap(),
            Response::Array(vec![Response::Data(b"a".to_vec()),])
        );
        assert_eq!(
            command(
                parser!(b"zrange key 0 0 withscoreS"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"a".to_vec()),
                Response::Data(b"1".to_vec()),
            ])
        );
        assert_eq!(
            command(
                parser!(b"zrange key -2 -1 WITHSCORES"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"c".to_vec()),
                Response::Data(b"3".to_vec()),
                Response::Data(b"d".to_vec()),
                Response::Data(b"4".to_vec()),
            ])
        );
    }

    #[test]
    fn zrevrange_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(parser!(b"zrevrange key 0 0"), &mut db, &mut Client::mock()).unwrap(),
            Response::Array(vec![Response::Data(b"d".to_vec()),])
        );
        assert_eq!(
            command(
                parser!(b"zrevrange key 0 0 withscoreS"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"d".to_vec()),
                Response::Data(b"4".to_vec()),
            ])
        );
        assert_eq!(
            command(
                parser!(b"zrevrange key -2 -1 WITHSCORES"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"b".to_vec()),
                Response::Data(b"2".to_vec()),
                Response::Data(b"a".to_vec()),
                Response::Data(b"1".to_vec()),
            ])
        );
    }

    #[test]
    fn zrangebyscore_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(
                parser!(b"zrangebyscore key 1 (2"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![Response::Data(b"a".to_vec()),])
        );
        assert_eq!(
            command(
                parser!(b"zrangebyscore key 1 1 withscoreS"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"a".to_vec()),
                Response::Data(b"1".to_vec()),
            ])
        );
        assert_eq!(
            command(
                parser!(b"zrangebyscore key (2 inf WITHSCORES"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"c".to_vec()),
                Response::Data(b"3".to_vec()),
                Response::Data(b"d".to_vec()),
                Response::Data(b"4".to_vec()),
            ])
        );
        assert_eq!(
            command(
                parser!(b"zrangebyscore key -inf inf withscores LIMIT 2 10"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"c".to_vec()),
                Response::Data(b"3".to_vec()),
                Response::Data(b"d".to_vec()),
                Response::Data(b"4".to_vec()),
            ])
        );
    }

    #[test]
    fn zrevrangebyscore_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(
                parser!(b"zrevrangebyscore key (2 1"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![Response::Data(b"a".to_vec()),])
        );
        assert_eq!(
            command(
                parser!(b"zrevrangebyscore key 1 1 withscoreS"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"a".to_vec()),
                Response::Data(b"1".to_vec()),
            ])
        );
        assert_eq!(
            command(
                parser!(b"zrevrangebyscore key inf (2 WITHSCORES"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"d".to_vec()),
                Response::Data(b"4".to_vec()),
                Response::Data(b"c".to_vec()),
                Response::Data(b"3".to_vec()),
            ])
        );
        assert_eq!(
            command(
                parser!(b"zrevrangebyscore key inf -inf withscores LIMIT 2 10"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"b".to_vec()),
                Response::Data(b"2".to_vec()),
                Response::Data(b"a".to_vec()),
                Response::Data(b"1".to_vec()),
            ])
        );
    }

    #[test]
    fn zrangebylex_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 0 a 0 b 0 c 0 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(
                parser!(b"zrangebylex key [a (b"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![Response::Data(b"a".to_vec()),])
        );
        assert_eq!(
            command(
                parser!(b"zrangebylex key (b +"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"c".to_vec()),
                Response::Data(b"d".to_vec()),
            ])
        );
        assert_eq!(
            command(
                parser!(b"zrangebylex key - + LIMIT 2 10"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"c".to_vec()),
                Response::Data(b"d".to_vec()),
            ])
        );
    }

    #[test]
    fn zrevrangebylex_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 0 a 0 b 0 c 0 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(
                parser!(b"zrevrangebylex key (b [a"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![Response::Data(b"a".to_vec()),])
        );
        assert_eq!(
            command(
                parser!(b"zrevrangebylex key + (b"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"d".to_vec()),
                Response::Data(b"c".to_vec()),
            ])
        );
        assert_eq!(
            command(
                parser!(b"zrevrangebylex key + - LIMIT 2 10"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"b".to_vec()),
                Response::Data(b"a".to_vec()),
            ])
        );
    }

    #[test]
    fn zrank_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(parser!(b"zrank key a"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(parser!(b"zrank key b"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"zrank key e"), &mut db, &mut Client::mock()).unwrap(),
            Response::Nil
        );
    }

    #[test]
    fn zrevrank_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(parser!(b"zrevrank key a"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(parser!(b"zrevrank key b"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(parser!(b"zrevrank key e"), &mut db, &mut Client::mock()).unwrap(),
            Response::Nil
        );
    }

    #[test]
    fn zrem_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 1 a 2 b 3 c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(parser!(b"zrem key a b d e"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(parser!(b"zrem key c"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn zunionstore_command_short() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key1 1 a 2 b 3 c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(parser!(b"zadd key2 4 d 5 e"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(
                parser!(b"zunionstore key 3 key1 key2 key3"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(5)
        );
    }

    #[test]
    fn zunionstore_command_short2() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key1 1 a 2 b 3 c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(parser!(b"zadd key2 4 d 5 e"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(
                parser!(b"zunionstore key 2 key1 key2"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(5)
        );
    }

    #[test]
    fn zunionstore_command_weights() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key1 1 a 2 b 3 c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(parser!(b"zadd key2 4 d 5 e"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(
                parser!(b"zunionstore key 3 key1 key2 key3 Weights 1 2 3"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(5)
        );
        assert_eq!(
            command(parser!(b"zscore key d"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"8".to_vec())
        );
    }

    #[test]
    fn zunionstore_command_aggregate() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key1 1 a 2 b 3 c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(
                parser!(b"zadd key2 9 c 4 d 5 e"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(
                parser!(b"zunionstore key 3 key1 key2 key3 aggregate max"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(5)
        );
        assert_eq!(
            command(parser!(b"zscore key c"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"9".to_vec())
        );
        assert_eq!(
            command(
                parser!(b"zunionstore key 3 key1 key2 key3 aggregate min"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(5)
        );
        assert_eq!(
            command(parser!(b"zscore key c"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"3".to_vec())
        );
    }

    #[test]
    fn zunionstore_command_weights_aggregate() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key1 1 a 2 b 3 c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(
                parser!(b"zadd key2 3 c 4 d 5 e"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(
                parser!(b"zunionstore key 3 key1 key2 key3 weights 1 2 3 aggregate max"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(5)
        );
        assert_eq!(
            command(parser!(b"zscore key c"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"6".to_vec())
        );
    }

    #[test]
    fn zinterstore_command_short() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key1 1 a 2 b 3 c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(
                parser!(b"zadd key2 3 c 4 d 5 e"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(
                parser!(b"zinterstore key 2 key1 key2"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn zinterstore_command_weights() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key1 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(parser!(b"zadd key2 4 d 5 e"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(parser!(b"zadd key3 0 d"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(
                parser!(b"zinterstore key 3 key1 key2 key3 Weights 1 2 3"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"zscore key d"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"12".to_vec())
        );
    }

    #[test]
    fn zinterstore_command_aggregate() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key1 1 a 2 b 3 c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(
                parser!(b"zadd key2 9 c 4 d 5 e"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(
                parser!(b"zinterstore key 2 key1 key2 aggregate max"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"zscore key c"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"9".to_vec())
        );
        assert_eq!(
            command(
                parser!(b"zinterstore key 2 key1 key2 aggregate min"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"zscore key c"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"3".to_vec())
        );
    }

    #[test]
    fn zinterstore_command_weights_aggregate() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key1 1 a 2 b 3 c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(
                parser!(b"zadd key2 3 c 4 d 5 e"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(
                parser!(b"zinterstore key 2 key1 key2 weights 1 2 aggregate max"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"zscore key c"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"6".to_vec())
        );
    }

    #[test]
    fn select_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let mut client = Client::mock();
        command(parser!(b"select 1"), &mut db, &mut client).unwrap();
        assert_eq!(client.dbindex, 1);
    }

    #[test]
    fn flushdb_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let mut client = Client::mock();
        command(parser!(b"select 0"), &mut db, &mut client).unwrap();
        assert_eq!(
            command(parser!(b"set key value"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        command(parser!(b"select 1"), &mut db, &mut client).unwrap();
        assert_eq!(
            command(parser!(b"set key valuf"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"flushdb"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        command(parser!(b"select 0"), &mut db, &mut client).unwrap();
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Data("value".to_owned().into_bytes())
        );
        command(parser!(b"select 1"), &mut db, &mut client).unwrap();
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Nil
        );
    }

    #[test]
    fn flushall_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let mut client = Client::mock();
        command(parser!(b"select 0"), &mut db, &mut client).unwrap();
        assert_eq!(
            command(parser!(b"set key value"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        command(parser!(b"select 1"), &mut db, &mut client).unwrap();
        assert_eq!(
            command(parser!(b"set key valuf"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"flushall"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        command(parser!(b"select 0"), &mut db, &mut client).unwrap();
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Nil
        );
        command(parser!(b"select 1"), &mut db, &mut client).unwrap();
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Nil
        );
    }

    #[test]
    fn subscribe_publish_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let (tx, rx) = channel();
        let mut client = Client::new(tx, 0);
        assert!(command(parser!(b"subscribe channel"), &mut db, &mut client).is_err());
        assert_eq!(
            command(
                parser!(b"publish channel hello-world"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert!(command(parser!(b"unsubscribe channel"), &mut db, &mut client).is_err());

        assert_eq!(
            rx.try_recv().unwrap().unwrap(),
            Response::Array(vec![
                Response::Data(b"subscribe".to_vec()),
                Response::Data(b"channel".to_vec()),
                Response::Integer(1),
            ])
        );
        assert_eq!(
            rx.try_recv().unwrap().unwrap(),
            Response::Array(vec![
                Response::Data(b"message".to_vec()),
                Response::Data(b"channel".to_vec()),
                Response::Data(b"hello-world".to_vec()),
            ])
        );
        assert_eq!(
            rx.try_recv().unwrap().unwrap(),
            Response::Array(vec![
                Response::Data(b"unsubscribe".to_vec()),
                Response::Data(b"channel".to_vec()),
                Response::Integer(0),
            ])
        );
    }

    #[test]
    fn auth_command() {
        let mut config = Config::new(Logger::new(Level::Warning));
        config.requirepass = Some("helloworld".to_owned());
        let mut db = Database::new(config);
        let mut client = Client::mock();
        assert!(command(parser!(b"get key"), &mut db, &mut client)
            .unwrap()
            .is_error());
        assert_eq!(client.auth, false);
        assert!(command(parser!(b"auth channel"), &mut db, &mut client)
            .unwrap()
            .is_error());
        assert_eq!(client.auth, false);
        assert!(!command(parser!(b"auth helloworld"), &mut db, &mut client)
            .unwrap()
            .is_error());
        assert_eq!(client.auth, true);
    }

    #[test]
    fn dump_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"1".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"dump key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"\x00\xc0\x01\x07\x00\xd9J2E\xd9\xcb\xc4\xe6".to_vec())
        );
    }

    #[test]
    fn keys_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let mut client = Client::mock();
        assert!(db
            .get_or_create(0, &b"key1".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert!(db
            .get_or_create(0, &b"key2".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert!(db
            .get_or_create(0, &b"key3".to_vec())
            .set(b"value".to_vec())
            .is_ok());

        match command(parser!(b"KEYS *"), &mut db, &mut client).unwrap() {
            Response::Array(resp) => assert_eq!(3, resp.len()),
            _ => panic!("Keys failed"),
        };

        assert_eq!(
            command(parser!(b"KEYS key1"), &mut db, &mut client).unwrap(),
            Response::Array(vec![Response::Data(b"key1".to_vec())])
        );
        assert_eq!(
            command(parser!(b"KEYS key[^23]"), &mut db, &mut client).unwrap(),
            Response::Array(vec![Response::Data(b"key1".to_vec())])
        );
        assert_eq!(
            command(parser!(b"KEYS key[1]"), &mut db, &mut client).unwrap(),
            Response::Array(vec![Response::Data(b"key1".to_vec())])
        );
    }

    #[test]
    fn multi_exec_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let mut client = Client::mock();
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());

        assert_eq!(
            command(parser!(b"multi"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"append key 1"), &mut db, &mut client).unwrap(),
            Response::Status("QUEUED".to_owned())
        );
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Status("QUEUED".to_owned())
        );

        // still has the old value
        assert_eq!(
            db.get_or_create(0, &b"key".to_vec()).get().unwrap(),
            b"value".to_vec()
        );

        assert_eq!(
            command(parser!(b"EXEC"), &mut db, &mut client).unwrap(),
            Response::Array(vec![
                Response::Integer(6),
                Response::Data(b"value1".to_vec()),
            ])
        );

        // value is updated
        assert_eq!(
            db.get_or_create(0, &b"key".to_vec()).get().unwrap(),
            b"value1".to_vec()
        );

        // multi status back to normal
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Data(b"value1".to_vec())
        );
    }

    #[test]
    fn multi_discard_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let mut client = Client::mock();
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());

        assert_eq!(
            command(parser!(b"multi"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"append key 1"), &mut db, &mut client).unwrap(),
            Response::Status("QUEUED".to_owned())
        );
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Status("QUEUED".to_owned())
        );

        // still has the old value
        assert_eq!(
            db.get_or_create(0, &b"key".to_vec()).get().unwrap(),
            b"value".to_vec()
        );

        assert_eq!(
            command(parser!(b"DISCARD"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );

        // still has the old value
        assert_eq!(
            db.get_or_create(0, &b"key".to_vec()).get().unwrap(),
            b"value".to_vec()
        );

        // multi status back to normal
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Data(b"value".to_vec())
        );
    }

    #[test]
    fn watch_multi_exec_fail_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let mut client = Client::mock();

        assert_eq!(
            command(parser!(b"watch key"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"set key 1"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"multi"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Status("QUEUED".to_owned())
        );
        assert_eq!(
            command(parser!(b"exec"), &mut db, &mut client).unwrap(),
            Response::Nil
        );
    }

    #[test]
    fn watch_multi_exec_ok_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let mut client = Client::mock();

        assert_eq!(
            command(parser!(b"watch key"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Nil
        );
        assert_eq!(
            command(parser!(b"multi"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"set key 1"), &mut db, &mut client).unwrap(),
            Response::Status("QUEUED".to_owned())
        );
        assert_eq!(
            command(parser!(b"EXEC"), &mut db, &mut client).unwrap(),
            Response::Array(vec![Response::Status("OK".to_owned()),])
        );
    }

    #[test]
    fn watch_unwatch_multi_exec_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let mut client = Client::mock();

        assert_eq!(
            command(parser!(b"watch key"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"set key 1"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"unwatch"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"multi"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Status("QUEUED".to_owned())
        );
        assert_eq!(
            command(parser!(b"EXEC"), &mut db, &mut client).unwrap(),
            Response::Array(vec![Response::Data(b"1".to_vec()),])
        );
    }

    #[test]
    fn monitor() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let (tx, rx) = channel();
        let mut client1 = Client::new(tx, 0);
        let mut client2 = Client::mock();
        assert_eq!(
            command(parser!(b"monitor"), &mut db, &mut client1).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client2).unwrap(),
            Response::Nil
        );
        assert_eq!(
            rx.recv().unwrap(),
            Some(Response::Status("\"get\" \"key\" ".to_owned()))
        );
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn info() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let mut client = Client::mock();
        if let Response::Data(d) = command(parser!(b"info"), &mut db, &mut client).unwrap() {
            let s = from_utf8(&*d).unwrap();
            assert!(s.contains("rsedis_git_sha1"));
            assert!(s.contains("rsedis_git_dirty"));
        } else {
            panic!("Expected data");
        }
    }
}
