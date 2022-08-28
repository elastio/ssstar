//! Types that help with reading from and writing to tar archives
use crate::Result;
use crate::{create, extract};
use futures::FutureExt;
use snafu::{IntoError, ResultExt};
use std::io::Write;
use std::{
    future::Future,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tracing::{error, warn};

/// Wraps a [`tar::Builder`] and ensures it never attempts blocking write operations in an async
/// context.
///
/// We use `tar::Builder` with a `Write` implementation which internally uses `tokio::block_on` to
/// perform async write operations in a sync context.  This will panic if any write operations are
/// performed while in an async context.  Unfortunately, `tar` has made the dubious decision that
/// when the builder is dropped, it should flush to the writer, meaning the write can happen in an
/// async context after all.
///
/// This wrapper implements `Drop` itself and makes sure to perform the operation in a synchronous
/// context.
///
/// It stores the builder as `Option` not because there might not be a builder, but because in
/// order to implement `drop` correctly we need to transfer ownership of the builder to a sync
/// context.
pub(crate) struct TarBuilderWrapper<W: std::io::Write + Send + 'static> {
    builder: Option<Arc<Mutex<tar::Builder<CountingWriter<W>>>>>,
}

impl<W: std::io::Write + Send + 'static> Clone for TarBuilderWrapper<W> {
    fn clone(&self) -> Self {
        Self {
            builder: self.builder.clone(),
        }
    }
}

impl<W: std::io::Write + Send + 'static> Drop for TarBuilderWrapper<W> {
    fn drop(&mut self) {
        if let Some(builder) = self.builder.take() {
            // This type gets cloned frequently so it can be used in multiple async tasks in
            // paralell.  The drop logic we want to apply to the tar builder requires exclusive
            // ownership.  So don't proceed unless this is the last instance
            if let Some(builder) = Arc::try_unwrap(builder)
                .ok()
                .map(|mutex| mutex.into_inner().unwrap())
            {
                // Exclusive access to the builder, so this is the last clone
                // Move the dropping to a blocking thread where it's safe
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    warn!("tar builder dropped without being finished, probably due to an error.  spawning cleanup on blocking thread.");

                    // Run the drop in another async task.  This isn't ideal, it would be much better
                    // if we could block waitingfor the drop to happen, but alas async drop is a bit of
                    // an unsolved problem in Rust at the moment.  This code path should only be
                    // executed if something else failed and we're returning an error before finalizing
                    // the tar builder, so it's unlikely that builder has constructed a completed tar
                    // archive anyway.
                    handle.spawn(Self::drop_builder_async(builder));
                } else {
                    // XXX: I'm pretty sure this is impossible in our code.  Handling it anyway just in
                    // case
                    warn!("tar builder is being dropped outside of async context");
                    if let Err(e) = Self::drop_builder(builder) {
                        error!(?e, "sync drop of builder failed");
                    }
                }
            }
        }
    }
}

impl<W: std::io::Write + Send + 'static> TarBuilderWrapper<W> {
    /// Wrap a [`tar::Builder`] so it can be safely used in an async context
    pub fn new(writer: W, progress: Arc<dyn create::CreateProgressCallback>) -> Self {
        // Wrap this writer in our own writer which counts bytes written and reports progress
        let writer = CountingWriter::new(writer, progress);

        Self {
            builder: Some(Arc::new(Mutex::new(tar::Builder::new(writer)))),
        }
    }

    /// Spawn a blocking task to append data to the tar archive, but don't wait for the task to
    /// finish
    pub fn spawn_append_data(
        &self,
        mut header: tar::Header,
        path: impl Into<PathBuf>,
        data: impl std::io::Read + Send + 'static,
    ) -> impl Future<Output = Result<()>> {
        let path = path.into();

        let append_fut = self.with_builder_mut(move |builder| {
            builder
                .append_data(&mut header, path, data)
                .with_context(|_| crate::error::TarAppendDataSnafu {})
        });

        async move { append_fut.await }
    }

    /// Run a closure in a sync context, providing a mut ref to `tar::Builder`.
    ///
    /// By limiting access to the builder in this way, we can ensure it's never operated on in an
    /// async context.
    pub fn with_builder_mut<F, T>(&self, f: F) -> impl Future<Output = Result<T>>
    where
        F: FnOnce(&mut tar::Builder<CountingWriter<W>>) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let builder = self.builder();

        tokio::task::spawn_blocking(move || {
            let mut guard = builder.lock().unwrap();
            f(&mut guard)
        })
        .map(|result| result.with_context(|_| crate::error::SpawnBlockingSnafu {})?)
    }

    /// Tell the builder that we're finished writing to it, flush the underlying writer, and drop
    /// in sync context to avoid panics.
    ///
    /// On success, returns the total number of bytes written to the underlying `Write` impl
    pub async fn finish_and_close(mut self) -> Result<u64> {
        // This is handled by the `Drop` impl.  We provide an explicit method for it just for
        // clarity in the code, since code in Drop feels like spooky action at a distance.
        if let Some(builder) = self.builder.take() {
            // It's a bug to call this when there are any other clones of this type floating around
            let builder = Arc::try_unwrap(builder)
                .unwrap_or_else(|_| panic!("BUG: all tar builder clones should be dropped by now"))
                .into_inner()
                .unwrap();
            Self::drop_builder_async(builder).await
        } else {
            unreachable!("BUG: builder already dropped before call to finish_and_close")
        }
    }

    fn builder(&self) -> Arc<Mutex<tar::Builder<CountingWriter<W>>>> {
        self.builder.clone().expect("BUG: already dropped")
    }

    /// Do the same work as [`Self::drop_builder`], but from an async context
    async fn drop_builder_async(builder: tar::Builder<CountingWriter<W>>) -> Result<u64> {
        // Dropping whilst in an async context.
        let fut = tokio::task::spawn_blocking(move || Self::drop_builder(builder));

        match fut.await {
            Err(e) => {
                error!(
                    ?e,
                    "blocking task to drop tar builder panicked or was canceleed"
                );
                Err(crate::error::SpawnBlockingSnafu {}.into_error(e))
            }
            Ok(Err(e)) => {
                error!(?e, "finalizing the tar builder reported an error");

                Err(e)
            }
            Ok(Ok(bytes_written)) => Ok(bytes_written),
        }
    }

    /// Destroy the `tar::Builder` in a guaranteed synchronous context so blocking calls to the
    /// writer are fine.
    fn drop_builder(builder: tar::Builder<CountingWriter<W>>) -> Result<u64> {
        let mut blocking_writer = builder
            .into_inner()
            .with_context(|_| crate::error::FlushSnafu {})?;

        // the Builder docs are not explicit about whether the writer is flushed as part of
        // `into_inner`.  So to be on the safe side, we need to flush it here
        blocking_writer
            .flush()
            .with_context(|_| crate::error::FlushSnafu {})?;

        Ok(blocking_writer.total_bytes_written)
    }
}

/// A wrapper around an arbitrary [`std::io::Write`] which counts how many bytes are written to the
/// underlying writer and reports them to the [`crate::CreateProgressCallback`] callback method
pub(crate) struct CountingWriter<W: std::io::Write + Send + 'static> {
    inner: W,
    progress: Arc<dyn create::CreateProgressCallback>,
    total_bytes_written: u64,
}

impl<W: std::io::Write + Send + 'static> CountingWriter<W> {
    fn new(writer: W, progress: Arc<dyn create::CreateProgressCallback>) -> Self {
        Self {
            inner: writer,
            progress,
            total_bytes_written: 0,
        }
    }
}

impl<W: std::io::Write + Send + 'static> std::io::Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let bytes_written = self.inner.write(buf)?;

        self.progress.tar_archive_bytes_written(bytes_written);
        self.total_bytes_written += bytes_written as u64;

        Ok(bytes_written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// A wrapper around an arbitrary [`std::io::Read`] which counts how many bytes are written to the
/// underlying reader and reports them to the [`create::ExtractProgressCallback`] callback method
pub(crate) struct CountingReader<R: std::io::Read + Send + 'static> {
    inner: R,
    progress: Arc<dyn extract::ExtractProgressCallback>,
    total_bytes_read: u64,
}

impl<R: std::io::Read + Send + 'static> CountingReader<R> {
    pub(crate) fn new(reader: R, progress: Arc<dyn extract::ExtractProgressCallback>) -> Self {
        Self {
            inner: reader,
            progress,
            total_bytes_read: 0,
        }
    }

    pub fn total_bytes_read(&self) -> u64 {
        self.total_bytes_read
    }
}

impl<R: std::io::Read + Send + 'static> std::io::Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes_read = self.inner.read(buf)?;

        self.progress.extract_archive_part_read(bytes_read);

        self.total_bytes_read += bytes_read as u64;

        Ok(bytes_read)
    }
}
