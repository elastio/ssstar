//! Test helper that implements [`ssstar::CreateProgressCallback`] which keeps a record of every progress
//! update in order so we can write tests that verify behavior or progress reporting functionality.
use more_asserts::*;
use ssstar::CreateProgressCallback;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, strum::EnumDiscriminants)]
#[allow(dead_code)] // Not all of these are used in tests but we want to capture all fields for all events
pub(crate) enum CreateProgressEvent {
    InputObjectDownloadStarted {
        bucket: String,
        key: String,
        version_id: Option<String>,
        size: u64,
    },

    InputPartUnorderedDownloaded {
        bucket: String,
        key: String,
        version_id: Option<String>,
        part_number: usize,
        part_size: usize,
    },

    InputPartDownloaded {
        bucket: String,
        key: String,
        version_id: Option<String>,
        part_number: usize,
        part_size: usize,
    },

    InputObjectDownloadCompleted {
        bucket: String,
        key: String,
        version_id: Option<String>,
        size: u64,
    },

    InputObjectsDownloadCompleted {
        total_bytes: u64,
    },

    TarArchiveInitialized {
        total_objects: usize,
        total_bytes: u64,
        estimated_archive_size: u64,
    },

    TarArchivePartWritten {
        bucket: String,
        key: String,
        version_id: Option<String>,
        part_number: usize,
        part_size: usize,
    },

    TarArchiveObjectWritten {
        bucket: String,
        key: String,
        version_id: Option<String>,
        size: u64,
    },

    TarArchiveBytesWritten {
        bytes_written: u64,
    },

    TarArchiveWritesCompleted {
        total_bytes_written: u64,
    },

    TarArchiveBytesUploaded {
        bytes_uploaded: u64,
    },

    TarArchiveUploadCompleted {
        size: u64,
    },
}

#[derive(Clone)]
pub(crate) struct TestCreateProgressCallback {
    events: Arc<Mutex<Vec<CreateProgressEvent>>>,
}

// Helper macro to reduce boilerplate when matching on specific events
macro_rules! with_match {
    ($var:ident, $matches:pat, $block:block) => {
        if let $matches = $var {
            $block
        } else {
            unreachable!(
                "{}",
                concat!(
                    stringify!($var),
                    " does not match expression ",
                    stringify!($matches)
                )
            )
        }
    };
}

impl TestCreateProgressCallback {
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Review all updates after a job has run to successful completion, validating that the
    /// updates are all sane and match expected invariants.
    ///
    /// If the create job didn't finish successfully then this check should not be applied.
    pub fn sanity_check_updates(&self) {
        // The object download started and object download completed events should match in
        // quantity and total bytes
        assert_eq!(
            self.input_object_download_started(),
            self.input_object_download_completed()
        );

        // Same thing for the unordered part download, and the in-order part download events
        assert_eq!(
            self.input_part_unordered_downloaded(),
            self.input_part_downloaded()
        );

        // The total size of objects downloaded, parts downloaded, and the final event with total
        // bytes downloaded should all be the same
        let (input_objects_downloaded, input_object_bytes_downloaded) =
            self.input_object_download_completed();
        let (input_parts_downloaded, input_part_bytes_downloaded) = self.input_part_downloaded();
        let total_input_objects_bytes_downloaded = self.input_objects_download_completed();
        assert_eq!(
            total_input_objects_bytes_downloaded,
            input_part_bytes_downloaded
        );
        assert_eq!(
            total_input_objects_bytes_downloaded,
            input_object_bytes_downloaded
        );

        // There must have been at least one input object, otherwise the create would fail
        assert_gt!(input_objects_downloaded, 0);

        // The tar archive initialized event should have the same total bytes and total objects as
        // the input objects download events
        // The estimated archive size should be bigger than the total input bytes
        let (archive_total_objects, archive_total_bytes, estimated_archive_size) =
            self.tar_archive_initialized();
        assert_eq!(archive_total_objects, input_objects_downloaded);
        assert_eq!(archive_total_bytes, total_input_objects_bytes_downloaded);
        assert_gt!(estimated_archive_size, archive_total_bytes);

        // The number and size of tar objects written should match the number and size of input
        // objects downloaded
        let (archive_objects_written, archive_object_bytes_written) =
            self.tar_archive_object_written();
        assert_eq!(archive_objects_written, input_objects_downloaded);
        assert_eq!(archive_object_bytes_written, input_object_bytes_downloaded);

        // Similarly the input parts downloaded should match the parts written to the tar archive
        let (archive_parts_written, archive_part_bytes_written) = self.tar_archive_part_written();
        assert_eq!(archive_parts_written, input_parts_downloaded);
        assert_eq!(archive_part_bytes_written, input_part_bytes_downloaded);

        // The bytes written to the tar archive are expected to be at least slightly larger than
        // the sum of the sizes of the inputs
        let (archive_writes, archive_bytes_written) = self.tar_archive_bytes_written();
        assert_gt!(archive_writes, 0);
        assert_gt!(archive_bytes_written, input_object_bytes_downloaded);

        // The total number of bytes written should equal the sum of the individual write events
        assert_eq!(archive_bytes_written, self.tar_archive_writes_completed());

        // If there are archive bytes uploaded, they should add up to the completed total,
        // otherwise neither should be present
        let (archive_uploads, sum_archive_bytes_uploaded) = self.tar_archive_bytes_uploaded();
        if archive_uploads > 0 {
            let archive_upload_completed_bytes = self.tar_archive_upload_completed();

            assert_eq!(sum_archive_bytes_uploaded, archive_upload_completed_bytes);
        }
    }

    /// The number of object download started events, and the total size of all of them combined
    pub fn input_object_download_started(&self) -> (usize, u64) {
        let events =
            self.filter_events(CreateProgressEventDiscriminants::InputObjectDownloadStarted);
        let count = events.len();
        let sum = events
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    CreateProgressEvent::InputObjectDownloadStarted { size, .. },
                    { size }
                )
            })
            .sum();

        (count, sum)
    }

    /// The number of object part downloaded completed events, and the total size of all of them combined
    pub fn input_part_unordered_downloaded(&self) -> (usize, u64) {
        let events =
            self.filter_events(CreateProgressEventDiscriminants::InputPartUnorderedDownloaded);
        let count = events.len();
        let sum = events
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    CreateProgressEvent::InputPartUnorderedDownloaded { part_size, .. },
                    { part_size as u64 }
                )
            })
            .sum();

        (count, sum)
    }

    /// The number of object part downloaded completed events, and the total size of all of them combined
    pub fn input_part_downloaded(&self) -> (usize, u64) {
        let events = self.filter_events(CreateProgressEventDiscriminants::InputPartDownloaded);
        let count = events.len();
        let sum = events
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    CreateProgressEvent::InputPartDownloaded { part_size, .. },
                    { part_size as u64 }
                )
            })
            .sum();

        (count, sum)
    }

    /// The number of object download completed events, and the total size of all of them combined
    pub fn input_object_download_completed(&self) -> (usize, u64) {
        let events =
            self.filter_events(CreateProgressEventDiscriminants::InputObjectDownloadCompleted);
        let count = events.len();
        let sum = events
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    CreateProgressEvent::InputObjectDownloadCompleted { size, .. },
                    { size }
                )
            })
            .sum();

        (count, sum)
    }

    /// The total bytes downloaded as reported by the `input_objects_download_completed` event
    pub fn input_objects_download_completed(&self) -> u64 {
        let event = self
            .filter_single_event(CreateProgressEventDiscriminants::InputObjectsDownloadCompleted)
            .unwrap();
        with_match!(
            event,
            CreateProgressEvent::InputObjectsDownloadCompleted { total_bytes, .. },
            { total_bytes }
        )
    }

    /// The object count, total input bytes, and estimated archive size from the tar archive
    /// initialized event
    pub fn tar_archive_initialized(&self) -> (usize, u64, u64) {
        let event = self
            .filter_single_event(CreateProgressEventDiscriminants::TarArchiveInitialized)
            .unwrap();
        with_match!(
            event,
            CreateProgressEvent::TarArchiveInitialized {
                total_objects,
                total_bytes,
                estimated_archive_size
            },
            { (total_objects, total_bytes, estimated_archive_size) }
        )
    }

    /// The number of archive part written events, and the total size of all of them combined
    pub fn tar_archive_part_written(&self) -> (usize, u64) {
        let events = self.filter_events(CreateProgressEventDiscriminants::TarArchivePartWritten);
        let count = events.len();
        let sum = events
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    CreateProgressEvent::TarArchivePartWritten { part_size, .. },
                    { part_size as u64 }
                )
            })
            .sum();

        (count, sum)
    }

    /// The number of archive object written events, and the total size of all of them combined
    pub fn tar_archive_object_written(&self) -> (usize, u64) {
        let events = self.filter_events(CreateProgressEventDiscriminants::TarArchiveObjectWritten);
        let count = events.len();
        let sum = events
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    CreateProgressEvent::TarArchiveObjectWritten { size, .. },
                    { size as u64 }
                )
            })
            .sum();

        (count, sum)
    }

    /// The number of archive bytes written events, and the total size of all of them combined
    pub fn tar_archive_bytes_written(&self) -> (usize, u64) {
        let events = self.filter_events(CreateProgressEventDiscriminants::TarArchiveBytesWritten);
        let count = events.len();
        let sum = events
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    CreateProgressEvent::TarArchiveBytesWritten { bytes_written, .. },
                    { bytes_written }
                )
            })
            .sum();

        (count, sum)
    }

    /// The number of total bytes reported to be written at the end of the tar archive writing
    /// process
    pub fn tar_archive_writes_completed(&self) -> u64 {
        let event = self
            .filter_single_event(CreateProgressEventDiscriminants::TarArchiveWritesCompleted)
            .unwrap();
        with_match!(
            event,
            CreateProgressEvent::TarArchiveWritesCompleted {
                total_bytes_written
            },
            { total_bytes_written }
        )
    }

    /// The number of archive bytes uploaded events, and the total size of all of them combined
    pub fn tar_archive_bytes_uploaded(&self) -> (usize, u64) {
        let events = self.filter_events(CreateProgressEventDiscriminants::TarArchiveBytesUploaded);
        let count = events.len();
        let sum = events
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    CreateProgressEvent::TarArchiveBytesUploaded { bytes_uploaded, .. },
                    { bytes_uploaded }
                )
            })
            .sum();

        (count, sum)
    }

    /// The number of total bytes reported to be uploaded to object storage at the end of the tar archive upload
    /// process
    pub fn tar_archive_upload_completed(&self) -> u64 {
        let event = self
            .filter_single_event(CreateProgressEventDiscriminants::TarArchiveUploadCompleted)
            .unwrap();
        with_match!(
            event,
            CreateProgressEvent::TarArchiveUploadCompleted { size },
            { size }
        )
    }

    /// Iterate over all events of a certain type
    pub fn filter_events(&self, typ: CreateProgressEventDiscriminants) -> Vec<CreateProgressEvent> {
        let events = self.events.lock().unwrap();

        events
            .iter()
            .filter(|event| {
                let event_typ: CreateProgressEventDiscriminants = (*event).into();

                event_typ == typ
            })
            .cloned()
            .collect::<Vec<_>>()
    }

    /// Get the single ocurrence of an event, if it can only appear 0 or 1 times.  If it appears
    /// more than this an assert is fired
    pub fn filter_single_event(
        &self,
        typ: CreateProgressEventDiscriminants,
    ) -> Option<CreateProgressEvent> {
        let mut events = self.filter_events(typ);

        assert!(
            events.len() <= 1,
            "Expected 0 or 1 instances of {:?}, but found {}",
            typ,
            events.len()
        );

        events.pop()
    }

    fn report_event(&self, event: CreateProgressEvent) {
        let mut events = self.events.lock().unwrap();

        events.push(event)
    }
}

impl std::fmt::Debug for TestCreateProgressCallback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Just use the innter `Vec`'s debug repr
        let events = self.events.lock().unwrap();
        events.fmt(f)
    }
}

impl CreateProgressCallback for TestCreateProgressCallback {
    fn input_object_download_started(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        size: u64,
    ) {
        self.report_event(CreateProgressEvent::InputObjectDownloadStarted {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: version_id.map(|id| id.to_string()),
            size,
        });
    }

    fn input_part_unordered_downloaded(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        part_number: usize,
        part_size: usize,
    ) {
        self.report_event(CreateProgressEvent::InputPartUnorderedDownloaded {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: version_id.map(|id| id.to_string()),
            part_number,
            part_size,
        });
    }

    fn input_part_downloaded(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        part_number: usize,
        part_size: usize,
    ) {
        self.report_event(CreateProgressEvent::InputPartDownloaded {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: version_id.map(|id| id.to_string()),
            part_number,
            part_size,
        });
    }

    fn input_object_download_completed(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        size: u64,
    ) {
        self.report_event(CreateProgressEvent::InputObjectDownloadCompleted {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: version_id.map(|id| id.to_string()),
            size,
        });
    }

    fn input_objects_download_completed(&self, total_bytes: u64) {
        self.report_event(CreateProgressEvent::InputObjectsDownloadCompleted { total_bytes });
    }

    fn tar_archive_initialized(
        &self,
        total_objects: usize,
        total_bytes: u64,
        estimated_archive_size: u64,
    ) {
        self.report_event(CreateProgressEvent::TarArchiveInitialized {
            total_objects,
            total_bytes,
            estimated_archive_size,
        });
    }

    fn tar_archive_part_written(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        part_number: usize,
        part_size: usize,
    ) {
        self.report_event(CreateProgressEvent::TarArchivePartWritten {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: version_id.map(|id| id.to_string()),
            part_number,
            part_size,
        });
    }

    fn tar_archive_object_written(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        size: u64,
    ) {
        self.report_event(CreateProgressEvent::TarArchiveObjectWritten {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: version_id.map(|id| id.to_string()),
            size,
        });
    }

    fn tar_archive_bytes_written(&self, bytes_written: u64) {
        self.report_event(CreateProgressEvent::TarArchiveBytesWritten { bytes_written });
    }

    fn tar_archive_writes_completed(&self, total_bytes_written: u64) {
        self.report_event(CreateProgressEvent::TarArchiveWritesCompleted {
            total_bytes_written,
        });
    }

    fn tar_archive_bytes_uploaded(&self, bytes_uploaded: u64) {
        self.report_event(CreateProgressEvent::TarArchiveBytesUploaded { bytes_uploaded });
    }

    fn tar_archive_upload_completed(&self, size: u64) {
        self.report_event(CreateProgressEvent::TarArchiveUploadCompleted { size });
    }
}
