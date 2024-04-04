use anyhow::{bail, Result};
use std::collections::HashMap;

struct Data<'a> {
    key: &'a str,
    value: &'a [u8],
}

impl<'a> Data<'a> {
    fn new(key: &'a str, value: &'a [u8]) -> Result<Self> {
        if key.len() >= 1 << 10 || value.len() >= 1 << 20 {
            anyhow::bail!("key or data length exceeded")
        }

        Ok(Self { key, value })
    }

    fn len(&self) -> usize {
        4 + self.key.len() + self.value.len()
    }

    fn write(&self, buffer: &mut [u8]) -> usize {
        assert!(self.len() <= buffer.len());

        let (s, e) = (0, 4);
        buffer[s..e].copy_from_slice(&u32::to_le_bytes(self.pack_size()));
        let (s, e) = (e, e + self.key.len());
        buffer[s..e].copy_from_slice(self.key.as_bytes());
        let (s, e) = (e, e + self.value.len());
        buffer[s..e].copy_from_slice(self.value);

        self.len()
    }

    fn read(buffer: &'a [u8]) -> Self {
        let (str_size, data_size) = Self::unpack_size(buffer[0..4].try_into().unwrap());
        Self {
            key: std::str::from_utf8(&buffer[4..4 + str_size]).unwrap(),
            value: &buffer[4 + str_size..4 + str_size + data_size],
        }
    }

    fn pack_size(&self) -> u32 {
        ((self.key.len() << 20) | self.value.len()) as u32
    }

    fn unpack_size(data: &[u8; 4]) -> (usize, usize) {
        let b = u32::from_le_bytes(*data);
        ((b >> 20) as usize, (b & ((1 << 20) - 1)) as usize)
    }
}

struct Chunk {
    pos: usize,
    data: Vec<u8>,
}

impl Chunk {
    fn new(bytes: usize) -> Self {
        Self {
            pos: 0,
            data: vec![0u8; bytes],
        }
    }
    fn remain(&self) -> usize {
        self.data.len() - self.pos
    }

    fn push<'a>(&'a mut self, data: Data<'_>) -> Data<'a> {
        assert!(data.len() <= self.remain());

        let wrote = data.write(&mut self.data[self.pos..]);
        let data = Data::read(&self.data[self.pos..]);
        self.pos += wrote;

        data
    }

    fn iter(&self) -> ChunkIter {
        ChunkIter {
            cur: 0,
            data: &self.data[..],
            end: self.pos,
        }
    }
}

struct ChunkIter<'a> {
    cur: usize,
    data: &'a [u8],
    end: usize,
}

impl<'a> Iterator for ChunkIter<'a> {
    type Item = Data<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur == self.end {
            return None;
        }
        let data = &self.data[self.cur..];
        let data = Data::read(data);
        self.cur += data.len();
        Some(data)
    }
}

pub struct InmemoryCache<'a> {
    bytes_per_chunk: usize,
    target_chunk: usize,
    entries: HashMap<&'a str, &'a [u8]>,
    chunks: Vec<Chunk>,
}

impl<'a> InmemoryCache<'a> {
    pub fn new(num_chunks: usize, bytes_per_chunk: usize) -> Self {
        Self {
            bytes_per_chunk,
            target_chunk: 0,
            entries: HashMap::new(),
            chunks: (0..num_chunks)
                .map(|_| Chunk::new(bytes_per_chunk))
                .collect(),
        }
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let entry = self.entries.get(key)?;
        Some(entry.to_vec())
    }

    pub fn set<'b>(&mut self, key: &'b str, value: &'b [u8]) -> Result<()> {
        let data = Data::new(key, value)?;

        if data.len() > self.bytes_per_chunk {
            bail!("key and value too large")
        }

        let chunk = if self.chunks[self.target_chunk].remain() < data.len() {
            self.target_chunk = (self.target_chunk + 1) % self.chunks.len();
            let chunk = &mut self.chunks[self.target_chunk];
            for data in chunk.iter() {
                self.entries.remove(data.key);
            }

            chunk
        } else {
            &mut self.chunks[self.target_chunk]
        };

        unsafe {
            // unsafeにせざるを得ない...
            let wrote = std::mem::transmute::<Data<'_>, Data<'a>>(chunk.push(data));
            self.entries.insert(wrote.key, wrote.value);
        }

        Ok(())
    }

    pub fn del(&mut self, key: &str) -> bool {
        self.entries.remove(key).is_some()
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use super::*;
    use anyhow::{anyhow, Result};
    use rand::{rngs::StdRng, Rng as _, SeedableRng as _};

    #[test]
    fn test_data() -> Result<()> {
        let key = "some-key";
        let value = [0u8, 1u8, 2u8, 3u8, 4u8];
        let data = Data::new(key, &value[..])?;

        let mut bytes = [0u8; 128];
        let wrote = data.write(&mut bytes[..]);

        assert_eq!(wrote, 4 + 8 + 5);

        let archived = Data::read(&bytes[..]);
        assert_eq!(archived.key, key);
        assert!(vec_equal(archived.value, &value));

        Ok(())
    }

    #[test]
    fn test_cache() -> Result<()> {
        let mut cache = Pin::new(Box::new(InmemoryCache::new(1, 1024)));

        (*cache).set("hello", "world".as_bytes())?;

        let key = String::from("hello");
        let data = (*cache).get(&key).ok_or(anyhow!("not found"))?;

        assert!(vec_equal(&data, "world".as_bytes()));

        Ok(())
    }

    fn rand_vec(bytes: usize) -> Vec<u8> {
        let mut data = vec![0u8; bytes];
        let mut rng = StdRng::from_entropy();
        rng.fill(&mut data[..]);
        data
    }

    fn vec_equal(va: &[u8], vb: &[u8]) -> bool {
        (va.len() == vb.len()) &&  // zip stops at the shortest
     va.iter()
       .zip(vb)
       .all(|(a,b)| *a == *b)
    }
}
