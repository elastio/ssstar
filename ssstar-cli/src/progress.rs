//! Implementations of progress callbacks that render progress bars
use ssstar::Result;
use std::{
    borrow::Cow,
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

/// Display a spinner while some long-running but unmeasurable task is running, then hide the
/// spinner when it finishes
pub(crate) async fn with_spinner<S, F, T>(globals: &super::Globals, message: S, task: F) -> T
where
    S: Into<Cow<'static, str>>,
    F: Future<Output = T>,
{
    let spinner = if !hide_progress(globals) {
        indicatif::ProgressBar::new_spinner()
    } else {
        indicatif::ProgressBar::hidden()
    };

    spinner.set_style(
        indicatif::ProgressStyle::with_template("{spinner:.blue} {msg}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
    );

    spinner.enable_steady_tick(Duration::from_millis(120));

    spinner.set_message(message);

    let result = task.await;

    spinner.finish_and_clear();

    result
}

/// Run the specified archive creation job, with progress bars for extra pretty-ness
pub(crate) async fn run_create_job(
    globals: &super::Globals,
    job: ssstar::CreateArchiveJob,
) -> Result<()> {
    let progress = CreateProgressReport::new(hide_progress(globals), &job);

    // TODO: implement abort functionality
    job.run(futures::future::pending(), progress).await
}

/// Run the specified archive extraction job, with progress bars for extra pretty-ness
pub(crate) async fn run_extract_job(
    globals: &super::Globals,
    job: ssstar::ExtractArchiveJob,
) -> Result<()> {
    let progress = ExtractProgressReport::new(hide_progress(globals), &job);

    // TODO: implement abort functionality
    job.run(futures::future::pending(), progress).await
}

/// Progress should be hidden for either of verbose mode (because there will be a flurry of log
/// messages and the progress bar rendering will be all messed up), or quiet mode (because
/// progress bars are not quiet).
fn hide_progress(globals: &super::Globals) -> bool {
    globals.verbose || globals.quiet
}

/// Progress reporting for the create operation, which receives progress updates from the lib crate
/// and renders progress bars accordingly
///
/// The create operation renders several progress bars, probaly more than anyone but the developer
/// of the code is interested in.
#[derive(Clone)]
struct CreateProgressReport {
    /// Aggregate which groups all of the below progress bars together
    #[allow(dead_code)] // Unused but needs to stay in scope
    multi: indicatif::MultiProgress,

    /// The raw input object bytes downloaded, regardless of order.
    ///
    /// Because we run multiple parts downloads in parallel, it will often happen that later parts
    /// will finish downloading before earlier parts.  This progress bar counts the part downloads
    /// immediately as they complete, without regard to ordering.  Thus it will sometimes be ahead
    /// of `ordered_bytes_downloaded`
    raw_bytes_downloaded: indicatif::ProgressBar,

    /// The number of input object bytes downloaded and placed in the queue for writing to the tar
    /// archive.
    ///
    /// Because we must write the input object bytes to the archive in the order in which those
    /// bytes appear, we must wait until the next part finishes downloading, even if subsequent
    /// parts have already downloaded.  Thus this will often be behind `raw_bytes_downloaded`
    ordered_bytes_downloaded: indicatif::ProgressBar,

    /// The number of input object bytes taken off of the queue from the transfer stage and written
    /// to the `tar` crate.  It presumably writes those to the underling writer, although not
    /// necessarily immediately due to buffering
    input_bytes_written_to_archive: indicatif::ProgressBar,

    /// The total number of bytes written by the `tar` crate to the underlying writer.  This will
    /// always be slightly more than `input_bytes_written_to_archive` because this counter also
    /// includes the `tar` format overhead
    total_bytes_written_to_archive: indicatif::ProgressBar,

    /// The number of tar archive bytes uploaded to object storage.  Unless the target is object
    /// storage, this progress bar will not be updated or rendered.  The upload will keep running
    /// even after `total_bytes_written_to_archive` has reached completion, because the uploader
    /// has a queue of work to do even after the `tar` crate has flushed its underlying writer and
    /// finished its work.
    archive_bytes_uploaded: indicatif::ProgressBar,
}

impl CreateProgressReport {
    fn new(hide_progress: bool, job: &ssstar::CreateArchiveJob) -> Self {
        fn standard_style() -> indicatif::ProgressStyle {
            indicatif::ProgressStyle::with_template("{spinner:.green} {prefix}: {msg:<55!} [{bar:20.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec})")
        .unwrap()
        .progress_chars("#>-")
        }

        /// The template syntax for right alignment seems not to work as I expected, and it does
        /// not pad the prefix string to the left with spaces.  So we have to do that ourselves
        fn pad_prefix(prefix: &'static str) -> String {
            const PREFIX_MAX_LEN: usize = 25;

            assert!(
                prefix.len() <= PREFIX_MAX_LEN,
                "Prefix '{prefix}' is too long"
            );

            format!("{prefix:>25}")
        }

        let multi = if !hide_progress {
            indicatif::MultiProgress::new()
        } else {
            indicatif::MultiProgress::with_draw_target(indicatif::ProgressDrawTarget::hidden())
        };

        let total_bytes = job.total_bytes();

        let raw_bytes_downloaded = multi.add(indicatif::ProgressBar::new(total_bytes));
        raw_bytes_downloaded.set_prefix(pad_prefix("D/L parts (all)"));
        raw_bytes_downloaded.set_style(standard_style());

        let ordered_bytes_downloaded = multi.add(indicatif::ProgressBar::new(total_bytes));
        ordered_bytes_downloaded.set_style(standard_style());
        ordered_bytes_downloaded.set_prefix(pad_prefix("D/L parts (ordered)"));

        let input_bytes_written_to_archive = multi.add(indicatif::ProgressBar::new(total_bytes));
        input_bytes_written_to_archive.set_style(standard_style());
        input_bytes_written_to_archive.set_prefix(pad_prefix("Write parts to tar"));

        // XXX: Technically `total_bytes` isn't the correct length here, because there's some tar
        // overhead as well.  This doesn't matter though, setting the lengths here is just to avoid
        // odd flicker when the progress bar length jumps dramatically.  All of the bars here will
        // have there lengths set explicitly as the progress events fire, and `total_bytes` is
        // close enough to the final archive size for the fraction of a second during which these
        // bars will be visible but not yet have the estimated archive size set.
        let total_bytes_written_to_archive = multi.add(indicatif::ProgressBar::new(total_bytes));
        total_bytes_written_to_archive.set_style(standard_style());
        total_bytes_written_to_archive.set_prefix(pad_prefix("Tar bytes written"));

        let archive_bytes_uploaded = multi.add(indicatif::ProgressBar::new(total_bytes));
        archive_bytes_uploaded.set_style(standard_style());
        archive_bytes_uploaded.set_prefix(pad_prefix("Tar bytes uploaded to S3"));

        Self {
            multi,
            raw_bytes_downloaded,
            ordered_bytes_downloaded,
            input_bytes_written_to_archive,
            total_bytes_written_to_archive,
            archive_bytes_uploaded,
        }
    }
}

#[allow(unused_variables)] // so we can keep the unused progress methods with their comments
impl ssstar::CreateProgressCallback for CreateProgressReport {
    fn input_objects_download_starting(&self, total_objects: usize, total_bytes: u64) {
        // Initialize the progress bars that related to downloading of input objects
        self.raw_bytes_downloaded.set_length(total_bytes);
        self.raw_bytes_downloaded
            .set_message("Starting download...");
        self.ordered_bytes_downloaded.set_length(total_bytes);
        self.ordered_bytes_downloaded
            .set_message("Starting download...");
        self.input_bytes_written_to_archive.set_length(total_bytes);
        self.input_bytes_written_to_archive
            .set_message("Starting download...");
    }

    fn input_object_download_started(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        size: u64,
    ) {
        // Nothing to do here; we dont' have a separate progress bar for downloads started and not
        // yet completed (maybe we should...)
    }

    fn input_part_unordered_downloaded(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        part_number: usize,
        part_size: usize,
    ) {
        self.raw_bytes_downloaded.inc(part_size as u64);
        self.raw_bytes_downloaded
            .set_message(format!("{} (part {})", key, part_number));
    }

    fn input_part_downloaded(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        part_number: usize,
        part_size: usize,
    ) {
        self.ordered_bytes_downloaded.inc(part_size as u64);
        self.ordered_bytes_downloaded
            .set_message(format!("{} (part {})", key, part_number));
    }

    fn input_object_download_completed(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        size: u64,
    ) {
        // Progress bars report part-by-part so there's no need to update a progress bar here
    }

    fn input_objects_download_completed(&self, total_bytes: u64, duration: Duration) {
        // If everything is working right, the progress bars for download are already exactly at
        // 100% done from prior updates but let's mark them as officially "done" now.
        let bytes_per_second = (total_bytes as f64 / duration.as_secs_f64()) as u64;
        let bytes_per_second = indicatif::BinaryBytes(bytes_per_second);
        let total_bytes = indicatif::BinaryBytes(total_bytes);
        let duration = indicatif::HumanDuration(duration);
        let message =
            format!("Download completed ({total_bytes} in {duration}, {bytes_per_second}/s)");

        self.multi.println(message).unwrap();
        self.raw_bytes_downloaded
            .finish_with_message(format!("Done ({total_bytes}, {bytes_per_second})"));
        self.ordered_bytes_downloaded
            .finish_with_message(format!("Done ({total_bytes}, {bytes_per_second})"));
    }

    fn tar_archive_initialized(
        &self,
        total_objects: usize,
        total_bytes: u64,
        estimated_archive_size: u64,
    ) {
        // Initialize the progress bars tracking tar archive writes
        self.total_bytes_written_to_archive
            .set_length(estimated_archive_size);
        self.total_bytes_written_to_archive
            .set_message("Archive initializing...");

        // there might or might not be an upload happening depending on the configured tar archive
        // target, but if there is it will have the same length
        self.archive_bytes_uploaded
            .set_length(estimated_archive_size);
    }

    fn tar_archive_part_written(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        part_number: usize,
        part_size: usize,
    ) {
        self.input_bytes_written_to_archive.inc(part_size as u64);
        self.input_bytes_written_to_archive
            .set_message(format!("{} (part {})", key, part_number));
    }

    fn tar_archive_object_written(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        byte_offset: u64,
        size: u64,
    ) {
        // Nothing to report
    }

    fn tar_archive_bytes_written(&self, bytes_written: usize) {
        self.total_bytes_written_to_archive
            .inc(bytes_written as u64);
        self.total_bytes_written_to_archive.set_message("Writing")
    }

    fn tar_archive_writes_completed(&self, total_bytes_written: u64) {
        self.input_bytes_written_to_archive
            .finish_with_message("All input objects written");
        self.total_bytes_written_to_archive
            .finish_with_message("Archive writes completed");
    }

    fn tar_archive_bytes_uploaded(&self, bytes_uploaded: usize) {
        self.archive_bytes_uploaded.inc(bytes_uploaded as u64);
        self.archive_bytes_uploaded
            .set_message("Upload in progress");
    }

    fn tar_archive_upload_completed(&self, size: u64, duration: Duration) {
        let bytes_per_second = (size as f64 / duration.as_secs_f64()) as u64;
        let bytes_per_second = indicatif::BinaryBytes(bytes_per_second);
        let duration = indicatif::HumanDuration(duration);
        let message = format!("Archive upload completed ({duration}, {bytes_per_second}/s)");

        self.multi.println(message).unwrap();
        self.archive_bytes_uploaded
            .finish_with_message("Archive upload completed");
    }
}

/// Progress reporting for the extract operation, which receives progress updates from the lib crate
/// and renders progress bars accordingly
///
/// The extract operation renders several progress bars, probaly more than anyone but the developer
/// of the code is interested in.
#[derive(Clone)]
struct ExtractProgressReport {
    /// Aggregate which groups all of the below progress bars together
    #[allow(dead_code)] // Unused but needs to stay in scope
    multi: indicatif::MultiProgress,

    /// The raw bytes read from the underlying tar archive
    raw_bytes_read: indicatif::ProgressBar,

    /// The progress extracting the object currently being extracted
    extract_object: indicatif::ProgressBar,

    /// The progress uploading the object currently being uploaded.
    /// Might or might not be the object being extracted, depends on the queue depth and whether
    /// the last extracted object has finished uploading
    upload_object: indicatif::ProgressBar,

    /// The current upload progress for objects whose upload has started but not yet completed.
    /// Additional detail needed to render meaningful progress information in the progress bar
    object_upload_progress: Arc<Mutex<HashMap<String, ObjectUploadProgress>>>,
}

/// To render progress meaningfully, the progress reporter needs to keep track of the progress so
/// far of each object being uploaded, since the progress callback methods themselves don't provide
/// all of the necessary context needed to render the progress bars meaningfully
struct ObjectUploadProgress {
    total_size: u64,
    total_bytes_uploaded: u64,
}

impl ExtractProgressReport {
    fn new(hide_progress: bool, job: &ssstar::ExtractArchiveJob) -> Self {
        fn standard_style() -> indicatif::ProgressStyle {
            indicatif::ProgressStyle::with_template("{spinner:.green} {prefix}: {msg:<55!} [{bar:20.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec})")
        .unwrap()
        .progress_chars("#>-")
        }

        /// The template syntax for right alignment seems not to work as I expected, and it does
        /// not pad the prefix string to the left with spaces.  So we have to do that ourselves
        fn pad_prefix(prefix: &'static str) -> String {
            const PREFIX_MAX_LEN: usize = 25;

            assert!(
                prefix.len() <= PREFIX_MAX_LEN,
                "Prefix '{prefix}' is too long"
            );

            format!("{prefix:>25}")
        }

        let multi = if !hide_progress {
            indicatif::MultiProgress::new()
        } else {
            indicatif::MultiProgress::with_draw_target(indicatif::ProgressDrawTarget::hidden())
        };

        let archive_size = job.archive_size();

        let raw_bytes_read = multi.add(indicatif::ProgressBar::new(
            archive_size.unwrap_or_default(),
        ));
        raw_bytes_read.set_prefix(pad_prefix("Read from tar"));
        raw_bytes_read.set_style(standard_style());

        let extract_object = multi.add(indicatif::ProgressBar::new(0));
        extract_object.set_style(standard_style());
        extract_object.set_prefix(pad_prefix("Extract file from tar"));

        let upload_object = multi.add(indicatif::ProgressBar::new(0));
        upload_object.set_style(standard_style());
        upload_object.set_prefix(pad_prefix("Upload object to S3"));

        Self {
            multi,
            raw_bytes_read,
            extract_object,
            upload_object,
            object_upload_progress: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[allow(unused_variables)] // so we can keep the unused progress methods with their comments
impl ssstar::ExtractProgressCallback for ExtractProgressReport {
    fn extract_starting(&self, archive_size: Option<u64>) {
        // Bar should already be set with the right length but just in case
        self.raw_bytes_read
            .set_length(archive_size.unwrap_or_default());
        self.raw_bytes_read.set_message("Extracting files");
    }

    fn extract_archive_part_read(&self, bytes: usize) {
        self.raw_bytes_read.inc(bytes as u64);
    }

    fn extract_object_skipped(&self, key: &str, size: u64) {
        self.extract_object.set_length(size);
        self.extract_object.set_position(size);
        self.extract_object.set_message(format!("Skipped '{key}'"));
    }

    fn extract_object_starting(&self, key: &str, size: u64) {
        self.extract_object.set_position(0);
        self.extract_object.set_length(size);
        self.extract_object.set_message(key.to_string());
    }

    fn extract_object_part_read(&self, key: &str, bytes: usize) {
        self.extract_object.inc(bytes as u64);
    }

    fn extract_object_finished(&self, key: &str, size: u64) {
        self.extract_object.set_position(size);
    }

    fn extract_finished(
        &self,
        extracted_objects: usize,
        extracted_object_bytes: u64,
        skipped_objects: usize,
        skipped_object_bytes: u64,
        total_bytes: u64,
        duration: Duration,
    ) {
        let bytes_per_second = (total_bytes as f64 / duration.as_secs_f64()) as u64;
        let bytes_per_second = indicatif::BinaryBytes(bytes_per_second);
        let total_objects = indicatif::HumanCount((extracted_objects + skipped_objects) as u64);
        let extracted_objects = indicatif::HumanCount(extracted_objects as u64);
        let extracted_object_bytes = indicatif::BinaryBytes(extracted_object_bytes);
        let skipped_objects = indicatif::HumanCount(skipped_objects as u64);
        let skipped_object_bytes = indicatif::BinaryBytes(skipped_object_bytes);
        let total_bytes = indicatif::BinaryBytes(total_bytes);
        let duration = indicatif::HumanDuration(duration);

        self.multi.println(
            format!("Extraction complete!  Read {total_objects} objects ({total_bytes}) from archive in {duration} ({bytes_per_second}/s)")).unwrap();
        self.multi
            .println(format!(
                "Extracted {extracted_objects} objects ({extracted_object_bytes})"
            ))
            .unwrap();
        self.multi
            .println(format!(
                "Skipped {skipped_objects} objects ({skipped_object_bytes})"
            ))
            .unwrap();

        self.raw_bytes_read.finish_and_clear();
        self.extract_object.finish_and_clear();
    }

    fn object_upload_starting(&self, key: &str, size: u64) {
        self.upload_object.set_position(0);
        self.upload_object.set_length(size);
        self.upload_object.set_message(key.to_string());

        let mut guard = self.object_upload_progress.lock().unwrap();
        guard.insert(
            key.to_string(),
            ObjectUploadProgress {
                total_size: size,
                total_bytes_uploaded: 0,
            },
        );
    }

    fn object_part_uploaded(&self, key: &str, bytes: usize) {
        let mut guard = self.object_upload_progress.lock().unwrap();
        let mut progress = guard
            .get_mut(key)
            .unwrap_or_else(|| panic!("BUG: Object key '{key}' part uploaded event received before upload starting or after object uploaded"));
        progress.total_bytes_uploaded += bytes as u64;

        self.upload_object.set_length(progress.total_size);
        self.upload_object
            .set_position(progress.total_bytes_uploaded);
        self.upload_object.set_message(key.to_string());
    }

    fn object_uploaded(&self, key: &str, size: u64) {
        self.upload_object.set_length(size);
        self.upload_object.set_position(size);
        self.upload_object.set_message(key.to_string());

        // This object has finished uploading, so there should be no more `object_part_uploaded`
        // messages
        let mut guard = self.object_upload_progress.lock().unwrap();
        guard.remove(key);
    }

    fn objects_uploaded(&self, total_objects: usize, total_object_bytes: u64, duration: Duration) {
        let bytes_per_second = (total_object_bytes as f64 / duration.as_secs_f64()) as u64;
        let bytes_per_second = indicatif::BinaryBytes(bytes_per_second);
        let total_objects = indicatif::HumanCount(total_objects as u64);
        let total_object_bytes = indicatif::BinaryBytes(total_object_bytes);
        let duration = indicatif::HumanDuration(duration);

        self.multi
            .println(format!(
                "Upload complete!  Uploaded {total_objects} objects ({total_object_bytes}) in {duration} ({bytes_per_second}/s)"
            ))
            .unwrap();
        self.upload_object.finish_and_clear();
    }
}
