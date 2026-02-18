use std::io;
use std::io::{Read, Write};

pub trait Serialize {
    fn serialize<W: Write>(&self, w: W) -> io::Result<usize>;
}

pub trait Deserialize: Sized {
    fn deserialize<R: Read>(r: R) -> io::Result<Self>;
}

macro_rules! impl_serialization {
    ($($t:ty),*) => {
        $(
            impl Serialize for $t {
                fn serialize<W: Write>(&self, mut w: W) -> io::Result<usize> {
                    let bytes = self.to_le_bytes();
                    w.write_all(&bytes)?;
                    Ok(bytes.len())
                }
            }

            impl Deserialize for $t {
                fn deserialize<R: Read>(mut r: R) -> io::Result<Self> {
                    let mut buf = [0u8; std::mem::size_of::<$t>()];
                    r.read_exact(&mut buf)?;
                    Ok(Self::from_le_bytes(buf))
                }
            }
        )*
    };
}

impl_serialization!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64);

impl Serialize for bool {
    fn serialize<W: Write>(&self, w: W) -> io::Result<usize> {
        (*self as u8).serialize(w)
    }
}

impl Deserialize for bool {
    fn deserialize<R: Read>(r: R) -> io::Result<Self> {
        let val = u8::deserialize(r)?;
        match val {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid bool value")),
        }
    }
}

impl Serialize for String {
    fn serialize<W: Write>(&self, mut w: W) -> io::Result<usize> {
        let bytes = self.as_bytes();
        let len = bytes.len() as u32;

        let mut written = len.serialize(&mut w)?;
        w.write_all(bytes)?;
        written += bytes.len();

        Ok(written)
    }
}

impl Deserialize for String {
    fn deserialize<R: Read>(mut r: R) -> io::Result<Self> {
        let len = u32::deserialize(&mut r)? as usize;
        let mut buf = vec![0u8; len];
        r.read_exact(&mut buf)?;

        String::from_utf8(buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

impl<T: Serialize> Serialize for Option<T> {
    fn serialize<W: Write>(&self, mut w: W) -> io::Result<usize> {
        match self {
            Some(val) => {
                let mut written = 1u8.serialize(&mut w)?;
                written += val.serialize(w)?;
                Ok(written)
            }
            None => {
                0u8.serialize(w)
            }
        }
    }
}

impl<T: Deserialize> Deserialize for Option<T> {
    fn deserialize<R: Read>(mut r: R) -> io::Result<Self> {
        let tag = u8::deserialize(&mut r)?;
        match tag {
            0 => Ok(None),
            1 => Ok(Some(T::deserialize(r)?)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid Option tag")),
        }
    }
}

impl<T: Serialize> Serialize for Vec<T> {
    fn serialize<W: Write>(&self, mut w: W) -> io::Result<usize> {
        (self.len() as u64).serialize(&mut w)?;
        let mut total = 8;
        for i in self {
            total += i.serialize(&mut w)?;
        }
        Ok(total)
    }
}

impl<T: Deserialize> Deserialize for Vec<T> {
    fn deserialize<R: Read>(mut r: R) -> io::Result<Self> {
        let len = u64::deserialize(&mut r)? as usize;
        let mut out = Vec::with_capacity(len);
        for _ in 0..len {
            out.push(T::deserialize(&mut r)?);
        }
        Ok(out)
    }
}

impl<T: Serialize + std::fmt::Debug, E: Serialize + std::fmt::Debug> Serialize for Result<T, E> {
    fn serialize<W: Write>(&self, mut w: W) -> io::Result<usize> {
        let mut total = self.is_ok().serialize(&mut w)?;

        total += match self {
            Ok(x) => x.serialize(&mut w)?,
            Err(e) => e.serialize(&mut w)?
        };

        Ok(total)
    }
}

impl<T: Deserialize + std::fmt::Debug, E: Deserialize + std::fmt::Debug> Deserialize for Result<T, E> {
    fn deserialize<R: Read>(mut r: R) -> io::Result<Self> {
        if bool::deserialize(&mut r)? {
            Ok(Ok(T::deserialize(&mut r)?))
        } else {
            Ok(Err(E::deserialize(&mut r)?))
        }
    }
}