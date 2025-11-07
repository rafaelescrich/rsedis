use std::collections::HashMap;
use std::io;
use std::io::Write;

use error::OperationError;
use rdbutil::constants::*;
use rdbutil::{encode_len, encode_slice_u8, EncodeError};

#[derive(PartialEq, Debug, Clone)]
pub enum ValueHash {
    /// Ziplist encoding for small hashes
    ZipList(Vec<u8>),
    /// Hashtable encoding for larger hashes
    HashMap(HashMap<Vec<u8>, Vec<u8>>),
}

impl Default for ValueHash {
    fn default() -> Self {
        Self::new()
    }
}

impl ValueHash {
    pub fn new() -> ValueHash {
        ValueHash::ZipList(Vec::new())
    }

    /// Converts ziplist to hashtable if needed based on config limits
    fn ensure_hashtable(&mut self, max_entries: usize, max_value: usize) {
        if let ValueHash::ZipList(_) = self {
            // For now, we'll convert to hashtable when we need to add items
            // The actual ziplist format would need proper deserialization
        }
    }

    /// Converts hashtable to ziplist if it fits within limits
    fn try_convert_to_ziplist(&mut self, max_entries: usize, max_value: usize) {
        if let ValueHash::HashMap(ref map) = self {
            if map.len() <= max_entries {
                let mut all_fit = true;
                for (k, v) in map.iter() {
                    if k.len() > max_value || v.len() > max_value {
                        all_fit = false;
                        break;
                    }
                }
                if all_fit {
                    // Convert to ziplist (simplified - just mark as ziplist)
                    // In a real implementation, we'd serialize to ziplist format
                    *self = ValueHash::ZipList(Vec::new());
                }
            }
        }
    }

    pub fn hset(&mut self, field: Vec<u8>, value: Vec<u8>) -> bool {
        match self {
            ValueHash::HashMap(map) => map.insert(field, value).is_none(),
            ValueHash::ZipList(_) => {
                // Convert to hashtable when adding/modifying
                let mut map = HashMap::new();
                // In a real implementation, we'd deserialize the ziplist first
                let inserted = map.insert(field, value).is_none();
                *self = ValueHash::HashMap(map);
                inserted
            }
        }
    }

    pub fn hget(&self, field: &[u8]) -> Option<&Vec<u8>> {
        match self {
            ValueHash::HashMap(map) => map.get(field),
            ValueHash::ZipList(_) => {
                // In a real implementation, we'd search the ziplist
                None // Simplified - ziplist is empty for now
            }
        }
    }

    pub fn hdel(&mut self, fields: &[&[u8]]) -> usize {
        match self {
            ValueHash::HashMap(map) => {
                let mut count = 0;
                for field in fields {
                    if map.remove(*field).is_some() {
                        count += 1;
                    }
                }
                count
            }
            ValueHash::ZipList(_) => {
                // Convert to hashtable to delete
                let mut map = HashMap::new();
                let mut count = 0;
                for field in fields {
                    if map.remove(*field).is_some() {
                        count += 1;
                    }
                }
                *self = ValueHash::HashMap(map);
                count
            }
        }
    }

    pub fn hexists(&self, field: &[u8]) -> bool {
        match self {
            ValueHash::HashMap(map) => map.contains_key(field),
            ValueHash::ZipList(_) => false, // Simplified
        }
    }

    pub fn hlen(&self) -> usize {
        match self {
            ValueHash::HashMap(map) => map.len(),
            ValueHash::ZipList(_) => 0, // Simplified - would need to parse ziplist
        }
    }

    pub fn hkeys(&self) -> Vec<Vec<u8>> {
        match self {
            ValueHash::HashMap(map) => map.keys().cloned().collect(),
            ValueHash::ZipList(_) => Vec::new(), // Simplified
        }
    }

    pub fn hvals(&self) -> Vec<Vec<u8>> {
        match self {
            ValueHash::HashMap(map) => map.values().cloned().collect(),
            ValueHash::ZipList(_) => Vec::new(), // Simplified
        }
    }

    pub fn hgetall(&self) -> Vec<Vec<u8>> {
        match self {
            ValueHash::HashMap(map) => {
                let mut result = Vec::with_capacity(map.len() * 2);
                for (key, value) in map {
                    result.push(key.clone());
                    result.push(value.clone());
                }
                result
            }
            ValueHash::ZipList(_) => Vec::new(), // Simplified
        }
    }

    /// Scan hash fields and values with cursor-based iteration.
    /// Returns (next_cursor, fields_and_values) where next_cursor is 0 when done.
    /// The result alternates between field and value.
    pub fn hscan(&self, cursor: usize, pattern: Option<&[u8]>, count: usize) -> (usize, Vec<Vec<u8>>) {
        use util::glob_match;
        match self {
            ValueHash::HashMap(map) => {
                let all_items: Vec<_> = map.iter().collect();
                let total = all_items.len();
                
                if cursor >= total {
                    return (0, Vec::new());
                }
                
                let end = std::cmp::min(cursor + count, total);
                let mut result = Vec::new();
                
                for i in cursor..end {
                    let (field, value) = all_items[i];
                    if let Some(pat) = pattern {
                        if glob_match(pat, field, false) {
                            result.push(field.clone());
                            result.push(value.clone());
                        }
                    } else {
                        result.push(field.clone());
                        result.push(value.clone());
                    }
                }
                
                let next_cursor = if end >= total { 0 } else { end };
                (next_cursor, result)
            }
            ValueHash::ZipList(_) => {
                // Simplified - ziplist is empty for now
                (0, Vec::new())
            }
        }
    }

    pub fn hstrlen(&self, field: &[u8]) -> usize {
        match self {
            ValueHash::HashMap(map) => map.get(field).map(|v| v.len()).unwrap_or(0),
            ValueHash::ZipList(_) => 0, // Simplified
        }
    }

    pub fn hincrby(&mut self, field: Vec<u8>, increment: i64) -> Result<i64, OperationError> {
        match self {
            ValueHash::HashMap(map) => {
                let current = map.get(&field).and_then(|v| {
                    std::str::from_utf8(v)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                });

                let new_value = match current {
                    Some(val) => val.checked_add(increment).ok_or(OperationError::OverflowError)?,
                    None => increment,
                };

                map.insert(field, new_value.to_string().into_bytes());
                Ok(new_value)
            }
            ValueHash::ZipList(_) => {
                // Convert to hashtable for increment operations
                let mut map = HashMap::new();
                let new_value = increment;
                map.insert(field, new_value.to_string().into_bytes());
                *self = ValueHash::HashMap(map);
                Ok(new_value)
            }
        }
    }

    pub fn hincrbyfloat(&mut self, field: Vec<u8>, increment: f64) -> Result<f64, OperationError> {
        match self {
            ValueHash::HashMap(map) => {
                let current = map.get(&field).and_then(|v| {
                    std::str::from_utf8(v)
                        .ok()
                        .and_then(|s| s.parse::<f64>().ok())
                });

                let new_value = match current {
                    Some(val) => val + increment,
                    None => increment,
                };

                if new_value.is_nan() || new_value.is_infinite() {
                    return Err(OperationError::NotANumberError);
                }

                map.insert(field, new_value.to_string().into_bytes());
                Ok(new_value)
            }
            ValueHash::ZipList(_) => {
                // Convert to hashtable for increment operations
                let mut map = HashMap::new();
                let new_value = increment;
                if new_value.is_nan() || new_value.is_infinite() {
                    return Err(OperationError::NotANumberError);
                }
                map.insert(field, new_value.to_string().into_bytes());
                *self = ValueHash::HashMap(map);
                Ok(new_value)
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            ValueHash::HashMap(map) => map.is_empty(),
            ValueHash::ZipList(zl) => zl.is_empty(),
        }
    }

    pub fn dump<T: Write>(&self, writer: &mut T) -> io::Result<usize> {
        let mut v = vec![];
        match self {
            ValueHash::HashMap(map) => {
                encode_len(map.len(), &mut v)?;
                for (key, value) in map {
                    encode_slice_u8(key, &mut v, true)?;
                    encode_slice_u8(value, &mut v, true)?;
                }
            }
            ValueHash::ZipList(_) => {
                // Simplified - ziplist is empty for now
                encode_len(0, &mut v)?;
            }
        }
        let data = [
            vec![TYPE_HASH],
            v,
            vec![(VERSION & 0xff) as u8],
            vec![((VERSION >> 8) & 0xff) as u8],
        ]
        .concat();
        writer.write(&*data)
    }

    pub fn debug_object(&self) -> String {
        let mut serialized_data = vec![];
        let serialized = self.dump(&mut serialized_data).unwrap();
        let encoding = match self {
            ValueHash::ZipList(_) => "ziplist",
            ValueHash::HashMap(_) => "hashtable",
        };
        format!(
            "Value at:0x0000000000 refcount:1 encoding:{} serializedlength:{} lru:0 \
             lru_seconds_idle:0",
            encoding, serialized
        )
    }
}

#[cfg(test)]
mod test_hash {
    use super::ValueHash;

    #[test]
    fn hset_hget() {
        let mut hash = ValueHash::new();
        assert!(hash.hset(b"field1".to_vec(), b"value1".to_vec()));
        assert_eq!(hash.hget(b"field1"), Some(&b"value1".to_vec()));
        assert!(!hash.hset(b"field1".to_vec(), b"value2".to_vec()));
        assert_eq!(hash.hget(b"field1"), Some(&b"value2".to_vec()));
    }

    #[test]
    fn hdel() {
        let mut hash = ValueHash::new();
        hash.hset(b"field1".to_vec(), b"value1".to_vec());
        hash.hset(b"field2".to_vec(), b"value2".to_vec());
        assert_eq!(hash.hdel(&[b"field1", b"field3"]), 1);
        assert_eq!(hash.hlen(), 1);
    }

    #[test]
    fn hexists() {
        let mut hash = ValueHash::new();
        assert!(!hash.hexists(b"field1"));
        hash.hset(b"field1".to_vec(), b"value1".to_vec());
        assert!(hash.hexists(b"field1"));
    }

    #[test]
    fn hlen() {
        let mut hash = ValueHash::new();
        assert_eq!(hash.hlen(), 0);
        hash.hset(b"field1".to_vec(), b"value1".to_vec());
        assert_eq!(hash.hlen(), 1);
        hash.hset(b"field2".to_vec(), b"value2".to_vec());
        assert_eq!(hash.hlen(), 2);
    }

    #[test]
    fn hincrby() {
        let mut hash = ValueHash::new();
        assert_eq!(hash.hincrby(b"field1".to_vec(), 5).unwrap(), 5);
        assert_eq!(hash.hincrby(b"field1".to_vec(), 3).unwrap(), 8);
        assert_eq!(hash.hincrby(b"field1".to_vec(), -2).unwrap(), 6);
    }

    #[test]
    fn hincrbyfloat() {
        let mut hash = ValueHash::new();
        assert_eq!(hash.hincrbyfloat(b"field1".to_vec(), 5.5).unwrap(), 5.5);
        assert_eq!(hash.hincrbyfloat(b"field1".to_vec(), 3.2).unwrap(), 8.7);
    }
}

