//! Test helper that implements [`ssstar::CreateProgressCallback`] which keeps a record of every progress
//! update in order so we can write tests that verify behavior or progress reporting functionality.
use more_asserts::*;
use ssstar::{CreateProgressCallback, ExtractProgressCallback};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Debug, strum::EnumDiscriminants)]
#[allow(dead_code)] // Not all of these are used in tests but we want to capture all fields for all events
pub(crate) enum CreateProgressEvent {
    InputObjectsDownloadStarting {
        total_objects: usize,
        total_bytes: u64,
    },

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

    ArchiveInitialized {
        total_objects: usize,
        total_bytes: u64,
        estimated_archive_size: u64,
    },

    ArchivePartWritten {
        bucket: String,
        key: String,
        version_id: Option<String>,
        part_number: usize,
        part_size: usize,
    },

    ArchiveObjectWritten {
        bucket: String,
        key: String,
        version_id: Option<String>,
        byte_offset: u64,
        size: u64,
    },

    ArchiveBytesWritten {
        bytes_written: usize,
    },

    ArchiveWritesCompleted {
        total_bytes_written: u64,
    },

    ArchiveBytesUploaded {
        bytes_uploaded: usize,
    },

    ArchiveUploadCompleted {
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

        // The totals reported at the start of the download should match the conclusion
        let (input_objects_downloading, input_object_bytes_downloading) =
            self.input_objects_download_starting();

        assert_eq!(
            (input_objects_downloading, input_object_bytes_downloading),
            self.input_object_download_started()
        );

        // The total size of objects downloaded, parts downloaded, and the final event with total
        // bytes downloaded should all be the same
        let (input_objects_downloaded, input_object_bytes_downloaded) =
            self.input_object_download_completed();
        let (input_parts_downloaded, input_part_bytes_downloaded) = self.input_part_downloaded();
        let total_input_objects_bytes_downloaded = self.input_objects_download_completed();
        assert_eq!(input_objects_downloaded, input_objects_downloading);
        assert_eq!(
            input_object_bytes_downloaded,
            input_object_bytes_downloading
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

    /// The values passed to the one and only download starting event
    pub fn input_objects_download_starting(&self) -> (usize, u64) {
        let event = self
            .filter_single_event(CreateProgressEventDiscriminants::InputObjectsDownloadStarting)
            .unwrap();
        with_match!(
            event,
            CreateProgressEvent::InputObjectsDownloadStarting {
                total_objects,
                total_bytes
            },
            { (total_objects, total_bytes) }
        )
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
            .filter_single_event(CreateProgressEventDiscriminants::ArchiveInitialized)
            .unwrap();
        with_match!(
            event,
            CreateProgressEvent::ArchiveInitialized {
                total_objects,
                total_bytes,
                estimated_archive_size
            },
            { (total_objects, total_bytes, estimated_archive_size) }
        )
    }

    /// The number of archive part written events, and the total size of all of them combined
    pub fn tar_archive_part_written(&self) -> (usize, u64) {
        let events = self.filter_events(CreateProgressEventDiscriminants::ArchivePartWritten);
        let count = events.len();
        let sum = events
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    CreateProgressEvent::ArchivePartWritten { part_size, .. },
                    { part_size as u64 }
                )
            })
            .sum();

        (count, sum)
    }

    /// The number of archive object written events, and the total size of all of them combined
    pub fn tar_archive_object_written(&self) -> (usize, u64) {
        let events = self.filter_events(CreateProgressEventDiscriminants::ArchiveObjectWritten);
        let count = events.len();
        let sum = events
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    CreateProgressEvent::ArchiveObjectWritten { size, .. },
                    { size }
                )
            })
            .sum();

        (count, sum)
    }

    /// The number of archive bytes written events, and the total size of all of them combined
    pub fn tar_archive_bytes_written(&self) -> (usize, u64) {
        let events = self.filter_events(CreateProgressEventDiscriminants::ArchiveBytesWritten);
        let count = events.len();
        let sum = events
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    CreateProgressEvent::ArchiveBytesWritten { bytes_written, .. },
                    { bytes_written as u64 }
                )
            })
            .sum();

        (count, sum)
    }

    /// The number of total bytes reported to be written at the end of the tar archive writing
    /// process
    pub fn tar_archive_writes_completed(&self) -> u64 {
        let event = self
            .filter_single_event(CreateProgressEventDiscriminants::ArchiveWritesCompleted)
            .unwrap();
        with_match!(
            event,
            CreateProgressEvent::ArchiveWritesCompleted {
                total_bytes_written
            },
            { total_bytes_written }
        )
    }

    /// The number of archive bytes uploaded events, and the total size of all of them combined
    pub fn tar_archive_bytes_uploaded(&self) -> (usize, u64) {
        let events = self.filter_events(CreateProgressEventDiscriminants::ArchiveBytesUploaded);
        let count = events.len();
        let sum = events
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    CreateProgressEvent::ArchiveBytesUploaded { bytes_uploaded, .. },
                    { bytes_uploaded as u64 }
                )
            })
            .sum();

        (count, sum)
    }

    /// The number of total bytes reported to be uploaded to object storage at the end of the tar archive upload
    /// process
    pub fn tar_archive_upload_completed(&self) -> u64 {
        let event = self
            .filter_single_event(CreateProgressEventDiscriminants::ArchiveUploadCompleted)
            .unwrap();
        with_match!(
            event,
            CreateProgressEvent::ArchiveUploadCompleted { size },
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
    fn input_objects_download_starting(&self, total_objects: usize, total_bytes: u64) {
        self.report_event(CreateProgressEvent::InputObjectsDownloadStarting {
            total_objects,
            total_bytes,
        });
    }

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

    fn input_objects_download_completed(&self, total_bytes: u64, _duration: Duration) {
        self.report_event(CreateProgressEvent::InputObjectsDownloadCompleted { total_bytes });
    }

    fn archive_initialized(
        &self,
        total_objects: usize,
        total_bytes: u64,
        estimated_archive_size: u64,
    ) {
        self.report_event(CreateProgressEvent::ArchiveInitialized {
            total_objects,
            total_bytes,
            estimated_archive_size,
        });
    }

    fn archive_part_written(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        part_number: usize,
        part_size: usize,
    ) {
        self.report_event(CreateProgressEvent::ArchivePartWritten {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: version_id.map(|id| id.to_string()),
            part_number,
            part_size,
        });
    }

    fn archive_object_written(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        byte_offset: u64,
        size: u64,
    ) {
        self.report_event(CreateProgressEvent::ArchiveObjectWritten {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: version_id.map(|id| id.to_string()),
            byte_offset,
            size,
        });
    }

    fn archive_bytes_written(&self, bytes_written: usize) {
        self.report_event(CreateProgressEvent::ArchiveBytesWritten { bytes_written });
    }

    fn archive_writes_completed(&self, total_bytes_written: u64) {
        self.report_event(CreateProgressEvent::ArchiveWritesCompleted {
            total_bytes_written,
        });
    }

    fn archive_bytes_uploaded(&self, bytes_uploaded: usize) {
        self.report_event(CreateProgressEvent::ArchiveBytesUploaded { bytes_uploaded });
    }

    fn archive_upload_completed(&self, size: u64, _duration: Duration) {
        self.report_event(CreateProgressEvent::ArchiveUploadCompleted { size });
    }
}

#[derive(Clone, Debug, strum::EnumDiscriminants)]
#[allow(dead_code)] // Not all of these are used in tests but we want to capture all fields for all events
pub(crate) enum ExtractProgressEvent {
    ExtractStarting {
        archive_size: Option<u64>,
    },

    ExtractArchivePartRead {
        bytes: usize,
    },

    ExtractObjectSkipped {
        key: String,
        size: u64,
    },

    ExtractObjectStarting {
        key: String,
        size: u64,
    },
    ExtractObjectPartRead {
        key: String,
        bytes: usize,
    },
    ExtractObjectFinished {
        key: String,
        size: u64,
    },

    ExtractFinished {
        extracted_objects: usize,
        extracted_object_bytes: u64,
        skipped_objects: usize,
        skipped_object_bytes: u64,
        total_bytes: u64,
    },

    ObjectUploadStarting {
        key: String,
        size: u64,
    },

    ObjectPartUploaded {
        key: String,
        bytes: usize,
    },

    ObjectUploaded {
        key: String,
        size: u64,
    },

    ObjectsUploaded {
        total_objects: usize,
        total_object_bytes: u64,
    },
}

#[derive(Clone)]
pub(crate) struct TestExtractProgressCallback {
    events: Arc<Mutex<Vec<ExtractProgressEvent>>>,
}

impl TestExtractProgressCallback {
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Review all updates after a job has run to successful completion, validating that the
    /// updates are all sane and match expected invariants.
    ///
    /// If the extract job didn't finish successfully then this check should not be applied.
    pub fn sanity_check_updates(&self) {
        // There must have been an extract starting event, and if there was an archive size it
        // should match the total read
        let archive_size = self.extract_starting();

        if let Some(archive_size) = archive_size {
            let (_, archive_bytes_read) = self.extract_archive_part_read();

            // The tar format has some kind of footer at the end which apparently doesnt need to be
            // read
            assert_eq!(archive_size, archive_bytes_read + 512);
        }

        // The total archive bytes read at the end of the extraction process should match the sum
        // of all bytes read during the extraction
        let (_, archive_part_bytes_read) = self.extract_archive_part_read();
        let (_, _, _, _, total_archive_bytes_read) = self.extract_finished();
        assert_eq!(archive_part_bytes_read, total_archive_bytes_read);

        // The number and total size of extracted and skipped objects should match the final number
        let (objects_skipped, object_bytes_skipped) = self.extract_object_skipped();
        let (objects_extracted, object_bytes_extracted) = self.extract_object_finished();

        let (
            total_objects_extracted,
            total_object_bytes_extracted,
            total_objects_skipped,
            total_object_bytes_skipped,
            _,
        ) = self.extract_finished();

        assert_eq!(objects_skipped, total_objects_skipped);
        assert_eq!(object_bytes_skipped, total_object_bytes_skipped);
        assert_eq!(objects_extracted, total_objects_extracted);
        assert_eq!(object_bytes_extracted, total_object_bytes_extracted);

        // The sum of all of the sizes of all object parts should match the total object bytes
        // extracted
        let (_, object_part_bytes_read) = self.extract_object_part_read();

        assert_eq!(total_object_bytes_extracted, object_part_bytes_read);

        // The size and count of the object starting and object finished events should match
        assert_eq!(
            self.extract_object_starting(),
            self.extract_object_finished()
        );

        // Since we only extract objects which should be uploaded, the extract starting and
        // finished totals should exactly match the upload starting and uploaded totals.
        assert_eq!(
            self.extract_object_starting(),
            self.object_upload_starting()
        );
        assert_eq!(self.extract_object_finished(), self.object_uploaded());

        let object_upload_starting_events = self
            .filter_events(ExtractProgressEventDiscriminants::ObjectUploadStarting)
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    ExtractProgressEvent::ObjectUploadStarting { key, size, .. },
                    { (key, size) }
                )
            });

        // Make a hash table where the key is the object key and the value is a vec of all
        // object_part_uploaded events in the order in which they occurred for that particular
        // object key
        let mut object_part_uploaded_events: HashMap<String, Vec<usize>> = HashMap::new();

        for (key, bytes) in self
            .filter_events(ExtractProgressEventDiscriminants::ObjectPartUploaded)
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    ExtractProgressEvent::ObjectPartUploaded { key, bytes, .. },
                    { (key, bytes) }
                )
            })
        {
            object_part_uploaded_events
                .entry(key)
                .or_default()
                .push(bytes)
        }

        // Validate that object parts progress starts from 0 and increases smoothly to the total
        // size.
        //
        // This is complex because we need to associate the object upload starting events with the corresponding part events
        for (object_key, object_size) in object_upload_starting_events {
            // Iterate over all of the object parts that pertain to this key
            let mut object_part_total_bytes = 0u64;

            for part_bytes in object_part_uploaded_events
                .get(&object_key)
                .unwrap_or_else(|| panic!("Object {object_key} has no object_part_uploaded events"))
            {
                assert_le!(
                    object_part_total_bytes + (*part_bytes as u64),
                    object_size,
                    "Object '{object_key}'"
                );
                object_part_total_bytes += *part_bytes as u64;
            }

            assert_eq!(
                object_part_total_bytes, object_size,
                "Object '{object_key}'"
            );
        }

        // The number and size of objects uploaded should match the final message
        assert_eq!(self.objects_uploaded(), self.object_uploaded());

        // The sum of all object part uploaded byte lengths should match the total amount of object
        // data uploaded
        let (_, object_part_bytes_uploaded) = self.object_part_uploaded();
        let (_, total_object_bytes_uploaded) = self.objects_uploaded();
        assert_eq!(object_part_bytes_uploaded, total_object_bytes_uploaded);
    }

    pub fn extract_starting(&self) -> Option<u64> {
        let event = self
            .filter_single_event(ExtractProgressEventDiscriminants::ExtractStarting)
            .unwrap();
        with_match!(
            event,
            ExtractProgressEvent::ExtractStarting { archive_size },
            { archive_size }
        )
    }

    /// The number of extract archive part read events, and the total size of all of them combined
    pub fn extract_archive_part_read(&self) -> (usize, u64) {
        let events = self.filter_events(ExtractProgressEventDiscriminants::ExtractArchivePartRead);
        let count = events.len();
        let sum = events
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    ExtractProgressEvent::ExtractArchivePartRead { bytes, .. },
                    { bytes as u64 }
                )
            })
            .sum();

        (count, sum)
    }

    /// The number of extract object skipped, and the total size of all of them combined
    pub fn extract_object_skipped(&self) -> (usize, u64) {
        let events = self.filter_events(ExtractProgressEventDiscriminants::ExtractObjectSkipped);
        let count = events.len();
        let sum = events
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    ExtractProgressEvent::ExtractObjectSkipped { size, .. },
                    { size }
                )
            })
            .sum();

        (count, sum)
    }

    /// The number of extract object starting events, and the total size of all of them combined
    pub fn extract_object_starting(&self) -> (usize, u64) {
        let events = self.filter_events(ExtractProgressEventDiscriminants::ExtractObjectStarting);
        let count = events.len();
        let sum = events
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    ExtractProgressEvent::ExtractObjectStarting { size, .. },
                    { size }
                )
            })
            .sum();

        (count, sum)
    }

    /// The number of extract object part read events, and the total size of all of them combined
    pub fn extract_object_part_read(&self) -> (usize, u64) {
        let events = self.filter_events(ExtractProgressEventDiscriminants::ExtractObjectPartRead);
        let count = events.len();
        let sum = events
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    ExtractProgressEvent::ExtractObjectPartRead { bytes, .. },
                    { bytes as u64 }
                )
            })
            .sum();

        (count, sum)
    }

    /// The number of extract object finished events, and the total size of all of them combined
    pub fn extract_object_finished(&self) -> (usize, u64) {
        let events = self.filter_events(ExtractProgressEventDiscriminants::ExtractObjectFinished);
        let count = events.len();
        let sum = events
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    ExtractProgressEvent::ExtractObjectFinished { size, .. },
                    { size }
                )
            })
            .sum();

        (count, sum)
    }

    /// The totals reported in the `extract_finished` event
    ///
    /// They are:
    /// - `expected_objects`
    /// - `extracted_object_bytes`
    /// - `skipped_objects`
    /// - `skipped_object_bytes`
    /// - `total_bytes`
    pub fn extract_finished(&self) -> (usize, u64, usize, u64, u64) {
        let event = self
            .filter_single_event(ExtractProgressEventDiscriminants::ExtractFinished)
            .unwrap();
        with_match!(
            event,
            ExtractProgressEvent::ExtractFinished {
                extracted_objects,
                extracted_object_bytes,
                skipped_objects,
                skipped_object_bytes,
                total_bytes
            },
            {
                (
                    extracted_objects,
                    extracted_object_bytes,
                    skipped_objects,
                    skipped_object_bytes,
                    total_bytes,
                )
            }
        )
    }

    /// The number of object upload starting events, and the total size of all of them combined
    pub fn object_upload_starting(&self) -> (usize, u64) {
        let events = self.filter_events(ExtractProgressEventDiscriminants::ObjectUploadStarting);
        let count = events.len();
        let sum = events
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    ExtractProgressEvent::ObjectUploadStarting { size, .. },
                    { size }
                )
            })
            .sum();

        (count, sum)
    }

    /// The number of object part uploaded events, and the total size of all of them combined
    pub fn object_part_uploaded(&self) -> (usize, u64) {
        let events = self.filter_events(ExtractProgressEventDiscriminants::ObjectPartUploaded);
        let count = events.len();
        let sum = events
            .into_iter()
            .map(|event| {
                with_match!(
                    event,
                    ExtractProgressEvent::ObjectPartUploaded { bytes, .. },
                    { bytes as u64 }
                )
            })
            .sum();

        (count, sum)
    }

    /// The number of object uploaded events, and the total size of all of them combined
    pub fn object_uploaded(&self) -> (usize, u64) {
        let events = self.filter_events(ExtractProgressEventDiscriminants::ObjectUploaded);
        let count = events.len();
        let sum = events
            .into_iter()
            .map(|event| {
                with_match!(event, ExtractProgressEvent::ObjectUploaded { size, .. }, {
                    size
                })
            })
            .sum();

        (count, sum)
    }

    /// The number of objects and bytes reported to be uploaded at the end of the upload process
    pub fn objects_uploaded(&self) -> (usize, u64) {
        let event = self
            .filter_single_event(ExtractProgressEventDiscriminants::ObjectsUploaded)
            .unwrap();
        with_match!(
            event,
            ExtractProgressEvent::ObjectsUploaded {
                total_objects,
                total_object_bytes
            },
            { (total_objects, total_object_bytes) }
        )
    }

    /// Iterate over all events of a certain type
    pub fn filter_events(
        &self,
        typ: ExtractProgressEventDiscriminants,
    ) -> Vec<ExtractProgressEvent> {
        let events = self.events.lock().unwrap();

        events
            .iter()
            .filter(|event| {
                let event_typ: ExtractProgressEventDiscriminants = (*event).into();

                event_typ == typ
            })
            .cloned()
            .collect::<Vec<_>>()
    }

    /// Get the single ocurrence of an event, if it can only appear 0 or 1 times.  If it appears
    /// more than this an assert is fired
    pub fn filter_single_event(
        &self,
        typ: ExtractProgressEventDiscriminants,
    ) -> Option<ExtractProgressEvent> {
        let mut events = self.filter_events(typ);

        assert!(
            events.len() <= 1,
            "Expected 0 or 1 instances of {:?}, but found {}",
            typ,
            events.len()
        );

        events.pop()
    }

    fn report_event(&self, event: ExtractProgressEvent) {
        let mut events = self.events.lock().unwrap();

        events.push(event)
    }
}

impl std::fmt::Debug for TestExtractProgressCallback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Just use the innter `Vec`'s debug repr
        let events = self.events.lock().unwrap();
        events.fmt(f)
    }
}

impl ExtractProgressCallback for TestExtractProgressCallback {
    fn extract_starting(&self, archive_size: Option<u64>) {
        self.report_event(ExtractProgressEvent::ExtractStarting { archive_size });
    }

    fn extract_archive_part_read(&self, bytes: usize) {
        self.report_event(ExtractProgressEvent::ExtractArchivePartRead { bytes });
    }

    fn extract_object_skipped(&self, key: &str, size: u64) {
        self.report_event(ExtractProgressEvent::ExtractObjectSkipped {
            key: key.to_string(),
            size,
        });
    }

    fn extract_object_starting(&self, key: &str, size: u64) {
        self.report_event(ExtractProgressEvent::ExtractObjectStarting {
            key: key.to_string(),
            size,
        });
    }

    fn extract_object_part_read(&self, key: &str, bytes: usize) {
        self.report_event(ExtractProgressEvent::ExtractObjectPartRead {
            key: key.to_string(),
            bytes,
        });
    }

    fn extract_object_finished(&self, key: &str, size: u64) {
        self.report_event(ExtractProgressEvent::ExtractObjectFinished {
            key: key.to_string(),
            size,
        });
    }

    fn extract_finished(
        &self,
        extracted_objects: usize,
        extracted_object_bytes: u64,
        skipped_objects: usize,
        skipped_object_bytes: u64,
        total_bytes: u64,
        _duration: Duration,
    ) {
        self.report_event(ExtractProgressEvent::ExtractFinished {
            extracted_objects,
            extracted_object_bytes,
            skipped_objects,
            skipped_object_bytes,
            total_bytes,
        });
    }

    fn object_upload_starting(&self, key: &str, size: u64) {
        self.report_event(ExtractProgressEvent::ObjectUploadStarting {
            key: key.to_string(),
            size,
        });
    }

    fn object_part_uploaded(&self, key: &str, bytes: usize) {
        self.report_event(ExtractProgressEvent::ObjectPartUploaded {
            key: key.to_string(),
            bytes,
        });
    }

    fn object_uploaded(&self, key: &str, size: u64) {
        self.report_event(ExtractProgressEvent::ObjectUploaded {
            key: key.to_string(),
            size,
        });
    }

    fn objects_uploaded(&self, total_objects: usize, total_object_bytes: u64, _duration: Duration) {
        self.report_event(ExtractProgressEvent::ObjectsUploaded {
            total_objects,
            total_object_bytes,
        });
    }
}
