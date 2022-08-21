//! Logging in tests is important for troubleshooting, but works very differently then in
//! production.
use crate::Result;
use once_cell::sync::Lazy;
use std::{
    cell::RefCell,
    future::Future,
    io::Write,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    time::Duration,
};
use tracing_subscriber::fmt::MakeWriter;

/// An implementation of `MakeWriter` that captures all log events for a single test
#[derive(Clone)]
struct TestWriter {
    log_events: Arc<Mutex<Vec<u8>>>,
}

impl TestWriter {
    fn new() -> Self {
        Self {
            log_events: Arc::new(Mutex::new(Vec::<u8>::new())),
        }
    }

    /// Clear the writer's buffer, returning the current contents as a string
    /// Panics of non-UTF8 text has been written to the buffer
    fn take_string(&self) -> String {
        let mut guard = self.log_events.lock().unwrap();

        let buffer: Vec<u8> = std::mem::take(&mut guard);

        String::from_utf8(buffer).unwrap()
    }
}

impl<'a> Write for &'a TestWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut guard = self.log_events.lock().unwrap();

        // Vec already implements Write
        guard.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // no flush needed when you write to a fix
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for TestWriter {
    type Writer = &'a Self;

    fn make_writer(&'a self) -> Self::Writer {
        self
    }
}

/// Run a test with logging enabled.
///
/// This takes the place of `tokio::test` because it needs to initialize the tokio runtime in some
/// more customized way to ensure logging is done correctly.
///
/// It initializes a single `tracing` Dispatch object for this test only, as well as a dedicated
/// tokio runtime.  It configures `tracing` to log to a buffer, and dumps this buffer to the
/// console at the end of the test or in the event of a panic.  Importantly, and unlike the default
/// behavior you get when initializing tracing, this automatically initializes all threads in the
/// tokio runtime for this test, to use that same logging config.  So this will pick up all log
/// events in tokio async tasks as well, but only those for this specific test.
///
/// This makes the log output for each test much more actionable because it's not interspersed with
/// log events from other tests.
pub fn test_with_logging(test: impl Future<Output = Result<()>>) -> Result<()> {
    // All log events for this test will be stored in this vec.
    let test_writer = TestWriter::new();

    let dispatch = {
        use tracing_subscriber::prelude::*;
        use tracing_subscriber::{fmt, EnvFilter};

        // Note the use of `TestWriter` here which writes the log events to stdout in a way that
        // Rust unit tests are able to capture (at least on the main thread)
        let format = fmt::layer()
            .with_level(true) // include level in output
            .with_target(true) // targets aren't that useful but filters operate on targets so they're important to know
            .with_thread_ids(true) // thread IDs are helpful when multiple parallel tests are running at the same time
            .with_thread_names(false) // but thread names are pretty shit
            .with_writer(test_writer.clone());

        // Get the log filter from the RUST_LOG env var, or if not set use a reasonable default
        let filter = EnvFilter::try_from_default_env()
            .or_else(|_| EnvFilter::try_new("h2=warn,hyper=info,rustls=info,aws=info,debug"))
            .unwrap();

        // Create a `fmt` subscriber that uses our custom event format, and set it
        // as the default.
        let subscriber = tracing_subscriber::registry().with(filter).with(format);

        tracing::Dispatch::new(subscriber)
    };

    let dispatch = Arc::new(dispatch);

    // This dispatch contains the logging config for this particular test.  It needs to be made the
    // default dispatcher in each thread as well.

    tracing::dispatcher::with_default(&dispatch, || {
        std::thread_local! {
            static THREAD_DISPATCHER_GUARD: RefCell<Option<tracing::subscriber::DefaultGuard>> = RefCell::new(None);
        }

        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.enable_all();
        {
            let dispatch = dispatch.clone();
            builder.on_thread_start(move || {
                let dispatch = dispatch.clone();

                THREAD_DISPATCHER_GUARD.with(|cell| {
                    cell.replace(Some(tracing::dispatcher::set_default(&dispatch)));
                })
            });
        }

        builder.on_thread_stop(|| {
            // Drop the dispatcher guard so it's no longer the thread-local default
            THREAD_DISPATCHER_GUARD.with(|cell| cell.replace(None));
        });

        let runtime = builder.build()?;

        // I'm pretty sure Tokio `Runtime` structs are safe to pass across unwind boundaries, but I
        // don't know for sure
        // It's not realistic to require all test futures to be explicitly unwind safe, so I'll
        // just assume they're safe too
        let result = std::panic::catch_unwind(AssertUnwindSafe(move || {
            let result = runtime.block_on(test);
            runtime.shutdown_timeout(Duration::from_secs(10));

            result
        }));

        // Test has run, maybe succeed maybe failed maybe panicked
        // Print all of the log events now
        let log_events = test_writer.take_string();

        println!("Log events from this test: \n{}", log_events);

        match result {
            Ok(result) => {
                // Test did not panic, so return the result the test did complete with
                result
            }
            Err(err) => {
                // Test panicked.  Just re-throw the panic now that we've written the log output
                std::panic::resume_unwind(err)
            }
        }
    })?;

    Ok(())
}
