//! Types which bridge between async `Stream` and `AsyncWrite` and synchronous `Read` and `Write`
//! types.
//!
//! This is a tricky problem to solve without impacting performance, but it's very important
//! because we download from and upload to S3 with parallelized, async code, but the `tar` crate
//! operates on synchronous `Read` and `Write` traits.

use crate::Result;
use bytes::{buf::Reader, Buf, Bytes};
use futures::{Stream, StreamExt};
use std::{io::Read, pin::Pin};
use tokio::io::AsyncWrite;

/// Given a [`Stream`] impl that yields vecs of bytes, produce a [`Read`] implementation that will
/// expose those very same bytes for blocking reads.
///
/// NOTE: The resulting [`std::io::Read`] implementation will panic if any of its `read_*` methods
/// are called from an async context, like in block of async code.  Blocking reads must be
/// performed in a blocking worker thread, using [`tokio::task::spawn_blocking`].
pub(crate) fn stream_as_reader<S>(stream: S) -> impl Read
where
    S: Stream<Item = Result<Bytes>> + Send + 'static,
{
    let handle = tokio::runtime::Handle::current();

    TryStreamReader {
        buffer: None,
        stream: Box::pin(stream),
        handle,
    }
}

/// Given an async [`tokio::io::AsyncWrite`] implementation, wrap it in a synchronous
/// [`std::io::Write`] impl which will pass writes to the async implementation internally.
///
/// As with [`stream_as_reader`], we need this to connect our completely async code with the `tar`
/// crate which operations only on blocking I/O.
///
/// Also like `stream_as_reader`, the internal implementation mechanism means that attempting to
/// perform blocking I/O on the returned `Write` impl from within an async context will panic.  You
/// should never do blocking I/O in an async context anyway.
pub(crate) fn async_write_as_writer<W>(writer: W) -> tokio_util::io::SyncIoBridge<W>
where
    W: AsyncWrite + Unpin + 'static,
{
    tokio_util::io::SyncIoBridge::new(writer)
}

struct TryStreamReader {
    buffer: Option<Reader<Bytes>>,
    stream: Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>,
    handle: tokio::runtime::Handle,
}

impl Read for TryStreamReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // If there's an existing buffer of data left over from a prior read, try to satisfy the
        // read request that way
        if let Some(mut buffer) = self.buffer.take() {
            if buffer.get_ref().remaining() > 0 {
                // Satisfy the read request using this buffer.
                //
                // Note it's possible that `buf` is bigger than the available bytes in the buffer,
                // so we probably should try to fill the remaining space in `buf` with another read
                // from the stream, but the spec for `Read` doesn't require that and this keeps the
                // code simpler.
                let bytes_read = buffer.read(buf)?;

                // If there's anything left in the buffer, put it back for use the next time
                if buffer.get_ref().remaining() > 0 {
                    self.buffer = Some(buffer);
                }

                return Ok(bytes_read);
            }
        }

        // No existing buffer, so pull the next one from the stream
        match self.handle.block_on(async { self.stream.next().await }) {
            None => {
                // The end of the stream.  That means EOF as far as the reader is concerned
                Ok(0)
            }
            Some(Err(e)) => {
                // The async reader that is filling the stream reported an error result, so this
                // needs to be reported back to the caller
                // Unfortunately the `std::io` error type is not that flexible so this is going to
                // be ugly
                Err(std::io::Error::new(std::io::ErrorKind::Other, e))
            }
            Some(Ok(bytes)) => {
                // Got the next buffer full of data.  Satisfy this read from it, and if there's
                // anything left over after that, store it for the next read
                let mut buffer = bytes.reader();

                let bytes_read = buffer.read(buf)?;

                if buffer.get_ref().remaining() > 0 {
                    self.buffer = Some(buffer);
                }

                Ok(bytes_read)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::*;
    use std::io::Cursor;

    /// Exercise the `TryStreamReader` type using some in-memory buffers
    ///
    /// We'll generate a big random buffer, split it up into randomly-sized pieces, and yield those
    /// in a stream.  The test will read them back from the `Read` impl in also randomly-sized
    /// buffers, gradually reconstructing the big random buffer in another Vec.  At the end, both
    /// the input and the output buffer should be the same.
    #[tokio::test]
    async fn read_from_stream() {
        const TEST_DATA_SIZE: usize = 10_000_000;
        const MAX_READ_SIZE: usize = TEST_DATA_SIZE / 10;
        const MIN_READ_SIZE: usize = 1;

        let mut test_data = vec![0u8; TEST_DATA_SIZE];
        let mut rand = rand::thread_rng();

        rand.fill(&mut test_data[..]);

        // Break up this data data into randomly sized smaller bits which the stream can yield
        let mut chunks = Vec::new();

        let mut cursor = Cursor::new(test_data.clone());

        while cursor.position() < TEST_DATA_SIZE as u64 {
            let mut chunk = vec![0u8; rand.gen_range(MIN_READ_SIZE..MAX_READ_SIZE)];

            let bytes_read = cursor.read(&mut chunk[..]).unwrap();

            chunk.truncate(bytes_read);

            chunks.push(Bytes::from(chunk));
        }

        // Make a stream that yields these chunks one at a time
        let stream = futures::stream::iter(chunks.into_iter().map(Result::<_>::Ok));

        // Wrap our reader around the stream
        let mut reader = stream_as_reader(stream);

        // The reading from the `Read` impl is blocking, which means it should not be run in an
        // async worker thread.  In our case since we use `Handle::block_on`, it will actually
        // panic if you try to read from an async task.  That's a nice side-effect and ensures that
        // we won't accidentally do blocking reads in an async task
        let read_data = tokio::task::spawn_blocking(move || {
            let mut rand = rand::thread_rng();

            // Read the data back from the Read implementation in random chunks
            let mut read_data = Vec::with_capacity(TEST_DATA_SIZE);

            while read_data.len() < TEST_DATA_SIZE {
                let mut chunk = vec![0u8; rand.gen_range(MIN_READ_SIZE..MAX_READ_SIZE)];

                let bytes_read = reader.read(&mut chunk[..]).unwrap();

                assert!(bytes_read > 0);

                read_data.extend_from_slice(&chunk[0..bytes_read]);
            }

            // Now all of the data is read, so the next attempt to read should yield zero bytes
            let mut dummy = vec![0u8; 100];
            assert_eq!(0, reader.read(&mut dummy[..]).unwrap());

            read_data
        })
        .await
        .unwrap();

        assert_eq!(test_data, read_data);
    }
}
