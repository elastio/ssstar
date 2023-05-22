//! Subset of the source code of the `tar` crate, copy-pasted here so that we can access some
//! internal implementation logic that isn't part of the public API.
//!
//! It's very regretable that we must resort to such hacks, but we need to be able to compute where
//! in a tar stream the data for each object is located, and that requires some internal
//! implementation details that aren't exposed in the API.
//!
//! This module is a mix of private functions from `builder.rs` and `header.rs` in the `tar` crate
//! source repo.  Copied from the 0.4.38 version of the crate, although this code concerns the
//! format of the `tar` crate so there shouldn't be an issue with this code even when we upgrade to
//! a newer version of the `tar` crate
use std::borrow::Cow;
use std::io;
use std::io::prelude::*;
#[cfg(unix)]
use std::os::unix::prelude::*;
use std::path::Path;
use std::str;
use tar::{EntryType, Header};

/// Calculate the size of the header for a particular file in the tar archive
///
/// (This is not part of the `tar` codebase, it's part of `ssstar`, located in this module so it
/// can have access to the private functions copy-pasted below).
///
/// Based on `prepare_header_path` in `builder.rs`, modified to not actually write to a `Write`
/// impl.
pub(super) fn calculate_header_size(mut header: tar::Header, path: &Path) -> io::Result<usize> {
    // Try to encode the path directly in the header, but if it ends up not
    // working (probably because it's too long) then try to use the GNU-specific
    // long name extension by emitting an entry which indicates that it's the
    // filename.
    if let Err(e) = header.set_path(path) {
        let data = path2bytes(path)?;
        let max = header.as_old().name.len();
        // Since `e` isn't specific enough to let us know the path is indeed too
        // long, verify it first before using the extension.
        if data.len() < max {
            return Err(e);
        }
        let header2 = prepare_header(data.len() as u64, b'L');
        // null-terminated string
        let mut data2 = data.chain(io::repeat(0).take(1));
        let mut dst = vec![];
        append(&mut dst, &header2, &mut data2)?;

        // Truncate the path to store in the header we're about to emit to
        // ensure we've got something at least mentioned. Note that we use
        // `str`-encoding to be compatible with Windows, but in general the
        // entry in the header itself shouldn't matter too much since extraction
        // doesn't look at it.
        let truncated = match str::from_utf8(&data[..max]) {
            Ok(s) => s,
            Err(e) => str::from_utf8(&data[..e.valid_up_to()]).unwrap(),
        };
        header.set_path(truncated)?;

        // Size is the size of the separate header/body combination for the long name,
        // plus the size of this header.
        Ok(dst.len() + header.as_bytes().len())
    } else {
        // path is not too long, so the header size is quite simple
        Ok(header.as_bytes().len())
    }
}

#[cfg(any(windows, target_arch = "wasm32"))]
pub fn path2bytes(p: &Path) -> io::Result<Cow<[u8]>> {
    p.as_os_str()
        .to_str()
        .map(|s| s.as_bytes())
        .ok_or_else(|| other(&format!("path {} was not valid Unicode", p.display())))
        .map(|bytes| {
            if bytes.contains(&b'\\') {
                // Normalize to Unix-style path separators
                let mut bytes = bytes.to_owned();
                for b in &mut bytes {
                    if *b == b'\\' {
                        *b = b'/';
                    }
                }
                Cow::Owned(bytes)
            } else {
                Cow::Borrowed(bytes)
            }
        })
}

#[cfg(unix)]
/// On unix this will never fail
pub fn path2bytes(p: &Path) -> io::Result<Cow<[u8]>> {
    Ok(p.as_os_str().as_bytes()).map(Cow::Borrowed)
}

#[cfg(target_arch = "wasm32")]
fn invalid_utf8<T>(_: T) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "Invalid utf-8")
}

fn append(mut dst: &mut dyn Write, header: &Header, mut data: &mut dyn Read) -> io::Result<()> {
    dst.write_all(header.as_bytes())?;
    let len = io::copy(&mut data, &mut dst)?;

    // Pad with zeros if necessary.
    let buf = [0; 512];
    let remaining = 512 - (len % 512);
    if remaining < 512 {
        dst.write_all(&buf[..remaining as usize])?;
    }

    Ok(())
}

fn prepare_header(size: u64, entry_type: u8) -> Header {
    let mut header = Header::new_gnu();
    let name = b"././@LongLink";
    header.as_gnu_mut().unwrap().name[..name.len()].clone_from_slice(&name[..]);
    header.set_mode(0o644);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    // + 1 to be compliant with GNU tar
    header.set_size(size + 1);
    header.set_entry_type(EntryType::new(entry_type));
    header.set_cksum();
    header
}

#[cfg(any(windows, target_arch = "wasm32"))]
fn other(msg: &str) -> Error {
    Error::new(ErrorKind::Other, msg)
}

