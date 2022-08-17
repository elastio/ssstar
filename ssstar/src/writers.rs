//! Some helpers which construct [`tokio::io::AsyncWrite`] impls that collect the written bytes
//! into a [`MultipartChunk`] struct and then yield them on a stream for some other async worker to
//! consume.
//!
//! There are two forms, one [`multipart`] breaks up the written data into multiple chunks
//! of a certain size for later uploading to object storage via the multipart upload APIs, and the
//! other [`unipart`] buffers the entire data written into memory and yields a single chunk
//! containing all the data.

use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, DuplexStream},
    sync::{mpsc, oneshot},
};
use tracing::{debug, warn};

pub(crate) struct MultipartChunk {
    /// The part number of the chunk starting from 0.
    pub part_number: usize,

    /// The contents of this chunk
    pub data: Bytes,
}

/// Construct a special kind of [`tokio::io::AsyncWrite`] implementation which internally buffers
/// written data into the multipart chunk size before sending it to a channel where some worker
/// task will presumably be waiting to operate on it.
pub(crate) fn multipart(
    multipart_chunk_size: usize,
    chunks_channel_depth: usize,
) -> (DuplexStream, mpsc::Receiver<MultipartChunk>) {
    let (bytes_sender, mut bytes_receiver) = tokio::io::duplex(multipart_chunk_size);
    let (chunks_sender, chunks_receiver) = mpsc::channel(chunks_channel_depth);

    // Spawn an async task whose job it is to take bytes written to the duplex, build them up into
    // MultipartChunk's equal to the multipart chunk size (except potentially for the last chunk
    // before the duplex is closed), and send them onto the mpsc channel for chunks.
    let mut part_number = 0usize;

    tokio::spawn(async move {
        loop {
            let mut buffer = bytes::BytesMut::with_capacity(multipart_chunk_size);

            // Becuse this receiver is part of a duplex, we know that reads are infallible.
            // If the sender is dropped, it just means that EOF is signaled
            // Read until the buffer is full, or until a 0 byte read indicates that this is the end
            // of the data coming over the duplex
            while buffer.len() < multipart_chunk_size {
                let bytes_read = bytes_receiver
                    .read_buf(&mut buffer)
                    .await
                    .expect("BUG: Reads from DuplexStream are infallible");

                if bytes_read == 0 {
                    // EOF.  Whatever we've read in `buffer` up to this point will be the final
                    // chunk
                    break;
                }
            }

            if !buffer.is_empty() {
                // Make this into a multi-part chunk
                let chunk = MultipartChunk {
                    part_number,
                    data: Bytes::from(buffer),
                };

                part_number += 1;

                // If sending on the channel fails, it can only mean the receiver was dropped,
                // probably because of some error sending the multipart chunks wherever they
                // need to go.  In any case all we can do it abort.
                if chunks_sender.send(chunk).await.is_err() {
                    warn!("chunks receiver was dropped; aborting the worker task");
                    break;
                }
            } else {
                // 0 bytes read; we're done here
                debug!("encountered end of duplex stream; worker task exiting");
                break;
            }
        }
    });

    (bytes_sender, chunks_receiver)
}

/// Handle a special case of [`multipart`] when the expected data isn't big enough to
/// justify multi-part chunking.
///
/// This behaves almost identical to `multipart`, except that only one chunk is produced,
/// consisting of the entirety of the bytes written to the `DuplexStream`.  Obviously this is only
/// intended for relatively small objects.
///
/// `max_bytes` should be greater than or equal to the amount of bytes ultimately written to the
/// `DuplexStream`.  If this is wrong and more data are written, no errors will ocurr but
/// performance will be reduced.
pub(crate) fn unipart(max_bytes: usize) -> (DuplexStream, oneshot::Receiver<Bytes>) {
    let (bytes_sender, mut bytes_receiver) = tokio::io::duplex(max_bytes);
    let (chunks_sender, chunks_receiver) = oneshot::channel();

    tokio::spawn(async move {
        // Assume the object will be smaller than the multipart threshold, otherwise why are we
        // using a unipart writer?
        let mut buffer = Vec::with_capacity(max_bytes);

        // Becuse this receiver is part of a duplex, we know that reads are infallible.
        // If the sender is dropped, it just means that EOF is signaled
        bytes_receiver
            .read_to_end(&mut buffer)
            .await
            .expect("BUG: Reads from DuplexStream are infallible");

        // In this case it doesn't matter if zero bytes were written, we're still yielding a
        // single chunk
        //
        // If sending on the channel fails, it can only mean the receiver was dropped,
        // probably because of some error sending the multipart chunks wherever they
        // need to go.  In any case all we can do it abort.
        if chunks_sender.send(Bytes::from(buffer)).is_err() {
            warn!("chunks receiver was dropped; unipart chunk is lost");
        }
    });

    (bytes_sender, chunks_receiver)
}
