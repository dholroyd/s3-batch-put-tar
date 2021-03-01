use log::*;
use rusoto_core::RusotoError;
use rusoto_s3;
use serde_derive::Serialize;
use std::io;
use std::io::{Read, Seek};
use std::sync::mpsc::TrySendError;
use tar;

struct Batch<W: io::Write + io::Seek> {
    start_time_ms: u64,
    output: tar::Builder<W>,
}
impl<W: io::Write + io::Seek> Batch<W> {
    pub fn new(start_time_ms: u64, output: W) -> Batch<W> {
        Batch {
            start_time_ms,
            output: tar::Builder::new(output),
        }
    }
    pub fn put_object(&mut self, write: QueuedWrite) -> io::Result<()> {
        let pos = self
            .output
            .get_mut()
            .seek(io::SeekFrom::Current(0))
            .unwrap();
        assert_eq!(pos, write.put_ref.offset_bytes as u64);
        let mut header = tar::Header::new_old();
        let h = &mut header;
        h.set_path(write.input.name).expect("set_path()");
        h.set_mtime(write.write_time.as_secs());
        h.set_size(write.input.body.len() as _);
        // its nice if the extracted files are readable + writable
        h.set_mode(0b0110000000);
        // after filling header fields; calculate header checksum
        h.set_cksum();
        self.output.append(&header, &write.input.body[..])?;
        let pos = self
            .output
            .get_mut()
            .seek(io::SeekFrom::Current(0))
            .unwrap();
        assert!(pos >= (write.put_ref.offset_bytes + write.put_ref.size_bytes) as u64);
        Ok(())
    }

    pub fn into_inner(self) -> io::Result<W> {
        self.output.into_inner()
    }
}

#[derive(Debug)]
pub enum BatchPutObjectError {
    NameTooLong {
        length: usize,
    },
    /// No more put-object operations could be submitted because the submission queue is already
    /// full.
    QueueFull,
    /// No more put-object operations could be submitted because the object writer has been shut
    /// down.
    WriterClosed,
    /// The body given in the BatchPutObjectRequest must produce Some(usize) from its size_hint()
    /// method so that the correct space can be allocated for the object within the batch without
    /// having to read the whole stream into memory.
    UnsizedBody,
}

struct BatchProcessor<Client: rusoto_s3::S3> {
    rx: std::sync::mpsc::Receiver<QueuedWrite>,
    batch_duration_ms: u128,
    client: Client,
    /// Name of the target S3 bucket
    bucket: String,
    key_prefix: String,
    rt: tokio::runtime::Handle,
    storage_class: Option<String>,
}
impl<Client: rusoto_s3::S3> BatchProcessor<Client> {
    pub fn new(
        rx: std::sync::mpsc::Receiver<QueuedWrite>,
        batch_duration_ms: u128,
        client: Client,
        bucket: String,
        key_prefix: String,
        rt: tokio::runtime::Handle,
        storage_class: Option<String>,
    ) -> Self {
        BatchProcessor {
            rx,
            batch_duration_ms,
            client,
            bucket,
            rt,
            key_prefix,
            storage_class,
        }
    }
    pub fn process(self) {
        self.rt.block_on(self.process_async())
    }
    async fn process_async(&self) {
        let mut current_batch = None;
        let timeout = std::time::Duration::from_millis(self.batch_duration_ms as _);
        loop {
            match self.rx.recv_timeout(timeout) {
                Ok(req) => {
                    if current_batch.is_none() {
                        let tmp = match tempfile::tempfile() {
                            Ok(t) => t,
                            Err(e) => {
                                log::error!("tempfile() failed: {:?}.  BatchProcessor exiting", e);
                                break;
                            }
                        };
                        let batch_start = self.batch_start_ms(req.write_time);
                        current_batch = Some(Batch::new(batch_start, tmp));
                    } else {
                        let current_start = current_batch.as_ref().unwrap().start_time_ms;
                        let next_start = req.put_ref.batch_id;
                        if current_start != next_start {
                            let last = current_batch.take().unwrap();
                            if let Err(e) = self.finalise(last).await {
                                error!("finalise() failed: {:?}", e);
                            }
                            let tmp = match tempfile::tempfile() {
                                Ok(t) => t,
                                Err(e) => {
                                    error!("tempfile() failed: {:?}.  BatchProcessor exiting", e);
                                    break;
                                }
                            };
                            current_batch = Some(Batch::new(next_start, tmp));
                        }
                    };
                    let mut batch = current_batch.take().unwrap();
                    let (res, batch) = self
                        .rt
                        .spawn_blocking(move || (batch.put_object(req), batch))
                        .await
                        .unwrap();
                    if let Err(e) = res {
                        error!("failure to write batch-object: {:?}", e);
                        // presume this batch is in some way compromised (e.g. out of disk space)
                        // so we free it tp prevent any further attempts to write, in hopes that
                        // any batches crated later may fare better once the underlying problem is
                        // resolved
                    } else {
                        current_batch = Some(batch);
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    if current_batch.is_some() {
                        let now = std::time::SystemTime::now();
                        let utc = now
                            .duration_since(std::time::UNIX_EPOCH)
                            .expect("duration_since()");
                        let batch_start = self.batch_start_ms(utc);
                        if batch_start > current_batch.as_ref().unwrap().start_time_ms {
                            let last = current_batch.take().unwrap();
                            if let Err(e) = self.finalise(last).await {
                                error!("finalise() failed: {:?}", e);
                            }
                        }
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    async fn finalise(&self, batch: Batch<std::fs::File>) -> io::Result<()> {
        let start_time_ms = batch.start_time_ms;
        let mut file = batch.into_inner()?;
        let mut attempt = 0;
        const MAX_ATTEMPTS: u32 = 3;
        loop {
            attempt += 1;
            file.seek(std::io::SeekFrom::Start(0))?;
            let mut all = vec![];
            file.read_to_end(&mut all)?; // FIXME: stream the file rather than loading into memory - https://stackoverflow.com/questions/57810173/streamed-upload-to-s3-with-rusoto
            let mut req = rusoto_s3::PutObjectRequest::default();
            req.body = Some(rusoto_s3::StreamingBody::from(all));
            req.bucket = self.bucket.clone();
            req.key = format!("{}{:}.tar", self.key_prefix, start_time_ms);
            req.storage_class = self.storage_class.clone();
            match self.client.put_object(req).await {
                Ok(_output) => return Ok(()),
                Err(e) => {
                    if attempt <= MAX_ATTEMPTS {
                        if let RusotoError::HttpDispatch(err) = e {
                            error!("put_object() failed on attempt {}: {:?}", attempt, err);
                        }
                    } else {
                        return Err(io::Error::new(io::ErrorKind::Other, e));
                    }
                }
            }
            // wait for 100ms, 200ms
            tokio::time::delay_for(std::time::Duration::from_millis(100 * (2_u64.pow(attempt))))
                .await;
        }
    }

    fn batch_start_ms(&self, utc: std::time::Duration) -> u64 {
        (utc.as_millis() - (utc.as_millis() % self.batch_duration_ms)) as u64
    }
}

struct QueuedWrite {
    // This object tracks the BatchObjectRef we handed back to the caller when we accepted this
    // in order that when we really come to insert the item into the tar file we can assert that
    // the *actual* file layout matches the byte-offset values we had predicted
    put_ref: BatchObjectRef,
    input: BatchPutObjectRequest,
    write_time: std::time::Duration,
}

struct Coalesce {
    last_batch_id: Option<u64>,
    batch_duration_ms: u64,
    next_write_offset_bytes: usize,
    tx: std::sync::mpsc::SyncSender<QueuedWrite>,
}
impl Coalesce {
    const TAR_BLOCK_SIZE: usize = 512;

    pub fn new(batch_duration_ms: u64, tx: std::sync::mpsc::SyncSender<QueuedWrite>) -> Self {
        Coalesce {
            last_batch_id: None,
            batch_duration_ms,
            next_write_offset_bytes: 0,
            tx,
        }
    }
    pub fn put_object(
        &mut self,
        input: BatchPutObjectRequest,
    ) -> Result<BatchObjectRef, BatchPutObjectError> {
        let now = std::time::SystemTime::now();
        let utc = now
            .duration_since(std::time::UNIX_EPOCH)
            .expect("duration_since()");
        let this_batch_id = self.batch_start_ms(utc);

        if self.last_batch_id.is_none() {
            self.last_batch_id = Some(this_batch_id);
        } else {
            if *self.last_batch_id.as_ref().unwrap() < this_batch_id {
                self.next_write_offset_bytes = 0;
                self.last_batch_id = Some(this_batch_id);
            }
        }
        let size = input.body.len();
        let put_ref = BatchObjectRef {
            batch_id: *self.last_batch_id.as_ref().unwrap(),
            offset_bytes: self.next_write_offset_bytes,
            size_bytes: size,
        };
        let tar_space_required = Self::TAR_BLOCK_SIZE
            + size
            + if size % Self::TAR_BLOCK_SIZE == 0 {
                0
            } else {
                Self::TAR_BLOCK_SIZE - size % Self::TAR_BLOCK_SIZE
            };
        self.next_write_offset_bytes += tar_space_required;

        let write = QueuedWrite {
            put_ref: put_ref.clone(),
            write_time: utc,
            input,
        };
        match self.tx.try_send(write) {
            Ok(_) => Ok(put_ref),
            Err(e) => {
                // undo our update to the shared state made above, since we didn't write the data
                self.next_write_offset_bytes -= tar_space_required;
                Err(match e {
                    TrySendError::Full(_) => BatchPutObjectError::QueueFull,
                    TrySendError::Disconnected(_) => BatchPutObjectError::WriterClosed,
                })
            }
        }
    }

    fn batch_start_ms(&self, utc: std::time::Duration) -> u64 {
        let utc = utc.as_millis() as u64;
        utc - utc % self.batch_duration_ms
    }
}

pub struct S3BatchPutClient {
    inner: std::sync::Arc<std::sync::Mutex<Coalesce>>,
}
impl S3BatchPutClient {
    /// Queue the given object to be written into a batch.
    ///
    /// Batches are written do disk before being uploaded to S3.  The writing to disk and eventual
    /// upload to S3 will be performed on another thread, therefore this method does not block the
    /// caller or need to be `await`ed.
    pub fn put_object(
        &mut self,
        input: BatchPutObjectRequest,
    ) -> Result<BatchObjectRef, BatchPutObjectError> {
        self.inner.lock().unwrap().put_object(input)
    }
}

pub struct BatchPutObjectRequest {
    pub name: String,
    pub body: Vec<u8>,
}

#[derive(Serialize, Debug, Clone)]
pub struct BatchObjectRef {
    pub batch_id: u64,
    pub offset_bytes: usize,
    pub size_bytes: usize,
}

pub struct ClientBuilder<Client: rusoto_s3::S3> {
    batch_duration: Option<u64>,
    bucket: Option<String>,
    key_prefix: Option<String>,
    client: Option<Client>,
    rt: Option<tokio::runtime::Handle>,
    storage_class: Option<String>,
}
impl<Client: rusoto_s3::S3> Default for ClientBuilder<Client> {
    fn default() -> Self {
        ClientBuilder {
            batch_duration: None,
            bucket: None,
            client: None,
            key_prefix: None,
            rt: None,
            storage_class: None,
        }
    }
}
impl<Client: rusoto_s3::S3 + Send + 'static> ClientBuilder<Client> {
    pub fn batch_duration(mut self, batch_duration: u64) -> Self {
        self.batch_duration = Some(batch_duration);
        self
    }

    pub fn bucket<S: ToString>(mut self, name: S) -> Self {
        self.bucket = Some(name.to_string());
        self
    }

    pub fn key_prefix<S: ToString>(mut self, prefix: S) -> Self {
        self.key_prefix = Some(prefix.to_string());
        self
    }

    pub fn s3_client(mut self, s3_client: Client) -> Self {
        self.client = Some(s3_client);
        self
    }

    pub fn handle(mut self, rt: tokio::runtime::Handle) -> Self {
        self.rt = Some(rt);
        self
    }

    /// Optional S3 storage class to request when storing resulting tar files
    pub fn storage_class(mut self, storage_class: String) -> Self {
        self.storage_class = Some(storage_class);
        self
    }

    pub fn build(self) -> Result<S3BatchPutClient, BuildError> {
        let (tx, rx) = std::sync::mpsc::sync_channel(30);
        let proc = BatchProcessor::new(
            rx,
            self.batch_duration
                .ok_or_else(|| BuildError::MissingBatchDuration)? as _,
            self.client.ok_or_else(|| BuildError::MissingS3Client)?,
            self.bucket.ok_or_else(|| BuildError::MissingBucket)?,
            self.key_prefix.unwrap_or("".to_string()),
            self.rt.ok_or_else(|| BuildError::MissingRuntime)?,
            self.storage_class,
        );
        std::thread::spawn(|| proc.process());
        let client = S3BatchPutClient {
            inner: std::sync::Arc::new(std::sync::Mutex::new(Coalesce::new(
                self.batch_duration
                    .ok_or_else(|| BuildError::MissingBatchDuration)? as _,
                tx,
            ))),
        };
        Ok(client)
    }
}

#[derive(Debug)]
pub enum BuildError {
    MissingBucket,
    MissingBatchDuration,
    MissingS3Client,
    MissingRuntime,
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rusoto_core::RusotoError;
    use rusoto_s3::{
        AbortMultipartUploadError, AbortMultipartUploadOutput, AbortMultipartUploadRequest,
        CompleteMultipartUploadError, CompleteMultipartUploadOutput,
        CompleteMultipartUploadRequest, CopyObjectError, CopyObjectOutput, CopyObjectRequest,
        CreateBucketError, CreateBucketOutput, CreateBucketRequest, CreateMultipartUploadError,
        CreateMultipartUploadOutput, CreateMultipartUploadRequest,
        DeleteBucketAnalyticsConfigurationError, DeleteBucketAnalyticsConfigurationRequest,
        DeleteBucketCorsError, DeleteBucketCorsRequest, DeleteBucketEncryptionError,
        DeleteBucketEncryptionRequest, DeleteBucketError, DeleteBucketInventoryConfigurationError,
        DeleteBucketInventoryConfigurationRequest, DeleteBucketLifecycleError,
        DeleteBucketLifecycleRequest, DeleteBucketMetricsConfigurationError,
        DeleteBucketMetricsConfigurationRequest, DeleteBucketPolicyError,
        DeleteBucketPolicyRequest, DeleteBucketReplicationError, DeleteBucketReplicationRequest,
        DeleteBucketRequest, DeleteBucketTaggingError, DeleteBucketTaggingRequest,
        DeleteBucketWebsiteError, DeleteBucketWebsiteRequest, DeleteObjectError,
        DeleteObjectOutput, DeleteObjectRequest, DeleteObjectTaggingError,
        DeleteObjectTaggingOutput, DeleteObjectTaggingRequest, DeleteObjectsError,
        DeleteObjectsOutput, DeleteObjectsRequest, DeletePublicAccessBlockError,
        DeletePublicAccessBlockRequest, GetBucketAccelerateConfigurationError,
        GetBucketAccelerateConfigurationOutput, GetBucketAccelerateConfigurationRequest,
        GetBucketAclError, GetBucketAclOutput, GetBucketAclRequest,
        GetBucketAnalyticsConfigurationError, GetBucketAnalyticsConfigurationOutput,
        GetBucketAnalyticsConfigurationRequest, GetBucketCorsError, GetBucketCorsOutput,
        GetBucketCorsRequest, GetBucketEncryptionError, GetBucketEncryptionOutput,
        GetBucketEncryptionRequest, GetBucketInventoryConfigurationError,
        GetBucketInventoryConfigurationOutput, GetBucketInventoryConfigurationRequest,
        GetBucketLifecycleConfigurationError, GetBucketLifecycleConfigurationOutput,
        GetBucketLifecycleConfigurationRequest, GetBucketLifecycleError, GetBucketLifecycleOutput,
        GetBucketLifecycleRequest, GetBucketLocationError, GetBucketLocationOutput,
        GetBucketLocationRequest, GetBucketLoggingError, GetBucketLoggingOutput,
        GetBucketLoggingRequest, GetBucketMetricsConfigurationError,
        GetBucketMetricsConfigurationOutput, GetBucketMetricsConfigurationRequest,
        GetBucketNotificationConfigurationError, GetBucketNotificationConfigurationRequest,
        GetBucketNotificationError, GetBucketPolicyError, GetBucketPolicyOutput,
        GetBucketPolicyRequest, GetBucketPolicyStatusError, GetBucketPolicyStatusOutput,
        GetBucketPolicyStatusRequest, GetBucketReplicationError, GetBucketReplicationOutput,
        GetBucketReplicationRequest, GetBucketRequestPaymentError, GetBucketRequestPaymentOutput,
        GetBucketRequestPaymentRequest, GetBucketTaggingError, GetBucketTaggingOutput,
        GetBucketTaggingRequest, GetBucketVersioningError, GetBucketVersioningOutput,
        GetBucketVersioningRequest, GetBucketWebsiteError, GetBucketWebsiteOutput,
        GetBucketWebsiteRequest, GetObjectAclError, GetObjectAclOutput, GetObjectAclRequest,
        GetObjectError, GetObjectLegalHoldError, GetObjectLegalHoldOutput,
        GetObjectLegalHoldRequest, GetObjectLockConfigurationError,
        GetObjectLockConfigurationOutput, GetObjectLockConfigurationRequest, GetObjectOutput,
        GetObjectRequest, GetObjectRetentionError, GetObjectRetentionOutput,
        GetObjectRetentionRequest, GetObjectTaggingError, GetObjectTaggingOutput,
        GetObjectTaggingRequest, GetObjectTorrentError, GetObjectTorrentOutput,
        GetObjectTorrentRequest, GetPublicAccessBlockError, GetPublicAccessBlockOutput,
        GetPublicAccessBlockRequest, HeadBucketError, HeadBucketRequest, HeadObjectError,
        HeadObjectOutput, HeadObjectRequest, ListBucketAnalyticsConfigurationsError,
        ListBucketAnalyticsConfigurationsOutput, ListBucketAnalyticsConfigurationsRequest,
        ListBucketInventoryConfigurationsError, ListBucketInventoryConfigurationsOutput,
        ListBucketInventoryConfigurationsRequest, ListBucketMetricsConfigurationsError,
        ListBucketMetricsConfigurationsOutput, ListBucketMetricsConfigurationsRequest,
        ListBucketsError, ListBucketsOutput, ListMultipartUploadsError, ListMultipartUploadsOutput,
        ListMultipartUploadsRequest, ListObjectVersionsError, ListObjectVersionsOutput,
        ListObjectVersionsRequest, ListObjectsError, ListObjectsOutput, ListObjectsRequest,
        ListObjectsV2Error, ListObjectsV2Output, ListObjectsV2Request, ListPartsError,
        ListPartsOutput, ListPartsRequest, NotificationConfiguration,
        NotificationConfigurationDeprecated, PutBucketAccelerateConfigurationError,
        PutBucketAccelerateConfigurationRequest, PutBucketAclError, PutBucketAclRequest,
        PutBucketAnalyticsConfigurationError, PutBucketAnalyticsConfigurationRequest,
        PutBucketCorsError, PutBucketCorsRequest, PutBucketEncryptionError,
        PutBucketEncryptionRequest, PutBucketInventoryConfigurationError,
        PutBucketInventoryConfigurationRequest, PutBucketLifecycleConfigurationError,
        PutBucketLifecycleConfigurationRequest, PutBucketLifecycleError, PutBucketLifecycleRequest,
        PutBucketLoggingError, PutBucketLoggingRequest, PutBucketMetricsConfigurationError,
        PutBucketMetricsConfigurationRequest, PutBucketNotificationConfigurationError,
        PutBucketNotificationConfigurationRequest, PutBucketNotificationError,
        PutBucketNotificationRequest, PutBucketPolicyError, PutBucketPolicyRequest,
        PutBucketReplicationError, PutBucketReplicationRequest, PutBucketRequestPaymentError,
        PutBucketRequestPaymentRequest, PutBucketTaggingError, PutBucketTaggingRequest,
        PutBucketVersioningError, PutBucketVersioningRequest, PutBucketWebsiteError,
        PutBucketWebsiteRequest, PutObjectAclError, PutObjectAclOutput, PutObjectAclRequest,
        PutObjectError, PutObjectLegalHoldError, PutObjectLegalHoldOutput,
        PutObjectLegalHoldRequest, PutObjectLockConfigurationError,
        PutObjectLockConfigurationOutput, PutObjectLockConfigurationRequest, PutObjectOutput,
        PutObjectRequest, PutObjectRetentionError, PutObjectRetentionOutput,
        PutObjectRetentionRequest, PutObjectTaggingError, PutObjectTaggingOutput,
        PutObjectTaggingRequest, PutPublicAccessBlockError, PutPublicAccessBlockRequest,
        RestoreObjectError, RestoreObjectOutput, RestoreObjectRequest, SelectObjectContentError,
        SelectObjectContentOutput, SelectObjectContentRequest, UploadPartCopyError,
        UploadPartCopyOutput, UploadPartCopyRequest, UploadPartError, UploadPartOutput,
        UploadPartRequest,
    };

    #[derive(Default)]
    struct Mock {
        puts: std::sync::Arc<std::sync::Mutex<std::cell::RefCell<Vec<PutObjectRequest>>>>,
    }
    #[async_trait]
    impl rusoto_s3::S3 for Mock {
        async fn abort_multipart_upload(
            &self,
            _input: AbortMultipartUploadRequest,
        ) -> Result<AbortMultipartUploadOutput, RusotoError<AbortMultipartUploadError>> {
            unimplemented!()
        }

        async fn complete_multipart_upload(
            &self,
            _input: CompleteMultipartUploadRequest,
        ) -> Result<CompleteMultipartUploadOutput, RusotoError<CompleteMultipartUploadError>>
        {
            unimplemented!()
        }

        async fn copy_object(
            &self,
            _input: CopyObjectRequest,
        ) -> Result<CopyObjectOutput, RusotoError<CopyObjectError>> {
            unimplemented!()
        }

        async fn create_bucket(
            &self,
            _input: CreateBucketRequest,
        ) -> Result<CreateBucketOutput, RusotoError<CreateBucketError>> {
            unimplemented!()
        }

        async fn create_multipart_upload(
            &self,
            _input: CreateMultipartUploadRequest,
        ) -> Result<CreateMultipartUploadOutput, RusotoError<CreateMultipartUploadError>> {
            unimplemented!()
        }

        async fn delete_bucket(
            &self,
            _input: DeleteBucketRequest,
        ) -> Result<(), RusotoError<DeleteBucketError>> {
            unimplemented!()
        }

        async fn delete_bucket_analytics_configuration(
            &self,
            _input: DeleteBucketAnalyticsConfigurationRequest,
        ) -> Result<(), RusotoError<DeleteBucketAnalyticsConfigurationError>> {
            unimplemented!()
        }

        async fn delete_bucket_cors(
            &self,
            _input: DeleteBucketCorsRequest,
        ) -> Result<(), RusotoError<DeleteBucketCorsError>> {
            unimplemented!()
        }

        async fn delete_bucket_encryption(
            &self,
            _input: DeleteBucketEncryptionRequest,
        ) -> Result<(), RusotoError<DeleteBucketEncryptionError>> {
            unimplemented!()
        }

        async fn delete_bucket_inventory_configuration(
            &self,
            _input: DeleteBucketInventoryConfigurationRequest,
        ) -> Result<(), RusotoError<DeleteBucketInventoryConfigurationError>> {
            unimplemented!()
        }

        async fn delete_bucket_lifecycle(
            &self,
            _input: DeleteBucketLifecycleRequest,
        ) -> Result<(), RusotoError<DeleteBucketLifecycleError>> {
            unimplemented!()
        }

        async fn delete_bucket_metrics_configuration(
            &self,
            _input: DeleteBucketMetricsConfigurationRequest,
        ) -> Result<(), RusotoError<DeleteBucketMetricsConfigurationError>> {
            unimplemented!()
        }

        async fn delete_bucket_policy(
            &self,
            _input: DeleteBucketPolicyRequest,
        ) -> Result<(), RusotoError<DeleteBucketPolicyError>> {
            unimplemented!()
        }

        async fn delete_bucket_replication(
            &self,
            _input: DeleteBucketReplicationRequest,
        ) -> Result<(), RusotoError<DeleteBucketReplicationError>> {
            unimplemented!()
        }

        async fn delete_bucket_tagging(
            &self,
            _input: DeleteBucketTaggingRequest,
        ) -> Result<(), RusotoError<DeleteBucketTaggingError>> {
            unimplemented!()
        }

        async fn delete_bucket_website(
            &self,
            _input: DeleteBucketWebsiteRequest,
        ) -> Result<(), RusotoError<DeleteBucketWebsiteError>> {
            unimplemented!()
        }

        async fn delete_object(
            &self,
            _input: DeleteObjectRequest,
        ) -> Result<DeleteObjectOutput, RusotoError<DeleteObjectError>> {
            unimplemented!()
        }

        async fn delete_object_tagging(
            &self,
            _input: DeleteObjectTaggingRequest,
        ) -> Result<DeleteObjectTaggingOutput, RusotoError<DeleteObjectTaggingError>> {
            unimplemented!()
        }

        async fn delete_objects(
            &self,
            _input: DeleteObjectsRequest,
        ) -> Result<DeleteObjectsOutput, RusotoError<DeleteObjectsError>> {
            unimplemented!()
        }

        async fn delete_public_access_block(
            &self,
            _input: DeletePublicAccessBlockRequest,
        ) -> Result<(), RusotoError<DeletePublicAccessBlockError>> {
            unimplemented!()
        }

        async fn get_bucket_accelerate_configuration(
            &self,
            _input: GetBucketAccelerateConfigurationRequest,
        ) -> Result<
            GetBucketAccelerateConfigurationOutput,
            RusotoError<GetBucketAccelerateConfigurationError>,
        > {
            unimplemented!()
        }

        async fn get_bucket_acl(
            &self,
            _input: GetBucketAclRequest,
        ) -> Result<GetBucketAclOutput, RusotoError<GetBucketAclError>> {
            unimplemented!()
        }

        async fn get_bucket_analytics_configuration(
            &self,
            _input: GetBucketAnalyticsConfigurationRequest,
        ) -> Result<
            GetBucketAnalyticsConfigurationOutput,
            RusotoError<GetBucketAnalyticsConfigurationError>,
        > {
            unimplemented!()
        }

        async fn get_bucket_cors(
            &self,
            _input: GetBucketCorsRequest,
        ) -> Result<GetBucketCorsOutput, RusotoError<GetBucketCorsError>> {
            unimplemented!()
        }

        async fn get_bucket_encryption(
            &self,
            _input: GetBucketEncryptionRequest,
        ) -> Result<GetBucketEncryptionOutput, RusotoError<GetBucketEncryptionError>> {
            unimplemented!()
        }

        async fn get_bucket_inventory_configuration(
            &self,
            _input: GetBucketInventoryConfigurationRequest,
        ) -> Result<
            GetBucketInventoryConfigurationOutput,
            RusotoError<GetBucketInventoryConfigurationError>,
        > {
            unimplemented!()
        }

        async fn get_bucket_lifecycle(
            &self,
            _input: GetBucketLifecycleRequest,
        ) -> Result<GetBucketLifecycleOutput, RusotoError<GetBucketLifecycleError>> {
            unimplemented!()
        }

        async fn get_bucket_lifecycle_configuration(
            &self,
            _input: GetBucketLifecycleConfigurationRequest,
        ) -> Result<
            GetBucketLifecycleConfigurationOutput,
            RusotoError<GetBucketLifecycleConfigurationError>,
        > {
            unimplemented!()
        }

        async fn get_bucket_location(
            &self,
            _input: GetBucketLocationRequest,
        ) -> Result<GetBucketLocationOutput, RusotoError<GetBucketLocationError>> {
            unimplemented!()
        }

        async fn get_bucket_logging(
            &self,
            _input: GetBucketLoggingRequest,
        ) -> Result<GetBucketLoggingOutput, RusotoError<GetBucketLoggingError>> {
            unimplemented!()
        }

        async fn get_bucket_metrics_configuration(
            &self,
            _input: GetBucketMetricsConfigurationRequest,
        ) -> Result<
            GetBucketMetricsConfigurationOutput,
            RusotoError<GetBucketMetricsConfigurationError>,
        > {
            unimplemented!()
        }

        async fn get_bucket_notification(
            &self,
            _input: GetBucketNotificationConfigurationRequest,
        ) -> Result<NotificationConfigurationDeprecated, RusotoError<GetBucketNotificationError>>
        {
            unimplemented!()
        }

        async fn get_bucket_notification_configuration(
            &self,
            _input: GetBucketNotificationConfigurationRequest,
        ) -> Result<NotificationConfiguration, RusotoError<GetBucketNotificationConfigurationError>>
        {
            unimplemented!()
        }

        async fn get_bucket_policy(
            &self,
            _input: GetBucketPolicyRequest,
        ) -> Result<GetBucketPolicyOutput, RusotoError<GetBucketPolicyError>> {
            unimplemented!()
        }

        async fn get_bucket_policy_status(
            &self,
            _input: GetBucketPolicyStatusRequest,
        ) -> Result<GetBucketPolicyStatusOutput, RusotoError<GetBucketPolicyStatusError>> {
            unimplemented!()
        }

        async fn get_bucket_replication(
            &self,
            _input: GetBucketReplicationRequest,
        ) -> Result<GetBucketReplicationOutput, RusotoError<GetBucketReplicationError>> {
            unimplemented!()
        }

        async fn get_bucket_request_payment(
            &self,
            _input: GetBucketRequestPaymentRequest,
        ) -> Result<GetBucketRequestPaymentOutput, RusotoError<GetBucketRequestPaymentError>>
        {
            unimplemented!()
        }

        async fn get_bucket_tagging(
            &self,
            _input: GetBucketTaggingRequest,
        ) -> Result<GetBucketTaggingOutput, RusotoError<GetBucketTaggingError>> {
            unimplemented!()
        }

        async fn get_bucket_versioning(
            &self,
            _input: GetBucketVersioningRequest,
        ) -> Result<GetBucketVersioningOutput, RusotoError<GetBucketVersioningError>> {
            unimplemented!()
        }

        async fn get_bucket_website(
            &self,
            _input: GetBucketWebsiteRequest,
        ) -> Result<GetBucketWebsiteOutput, RusotoError<GetBucketWebsiteError>> {
            unimplemented!()
        }

        async fn get_object(
            &self,
            _input: GetObjectRequest,
        ) -> Result<GetObjectOutput, RusotoError<GetObjectError>> {
            unimplemented!()
        }

        async fn get_object_acl(
            &self,
            _input: GetObjectAclRequest,
        ) -> Result<GetObjectAclOutput, RusotoError<GetObjectAclError>> {
            unimplemented!()
        }

        async fn get_object_legal_hold(
            &self,
            _input: GetObjectLegalHoldRequest,
        ) -> Result<GetObjectLegalHoldOutput, RusotoError<GetObjectLegalHoldError>> {
            unimplemented!()
        }

        async fn get_object_lock_configuration(
            &self,
            _input: GetObjectLockConfigurationRequest,
        ) -> Result<GetObjectLockConfigurationOutput, RusotoError<GetObjectLockConfigurationError>>
        {
            unimplemented!()
        }

        async fn get_object_retention(
            &self,
            _input: GetObjectRetentionRequest,
        ) -> Result<GetObjectRetentionOutput, RusotoError<GetObjectRetentionError>> {
            unimplemented!()
        }

        async fn get_object_tagging(
            &self,
            _input: GetObjectTaggingRequest,
        ) -> Result<GetObjectTaggingOutput, RusotoError<GetObjectTaggingError>> {
            unimplemented!()
        }

        async fn get_object_torrent(
            &self,
            _input: GetObjectTorrentRequest,
        ) -> Result<GetObjectTorrentOutput, RusotoError<GetObjectTorrentError>> {
            unimplemented!()
        }

        async fn get_public_access_block(
            &self,
            _input: GetPublicAccessBlockRequest,
        ) -> Result<GetPublicAccessBlockOutput, RusotoError<GetPublicAccessBlockError>> {
            unimplemented!()
        }

        async fn head_bucket(
            &self,
            _input: HeadBucketRequest,
        ) -> Result<(), RusotoError<HeadBucketError>> {
            unimplemented!()
        }

        async fn head_object(
            &self,
            _input: HeadObjectRequest,
        ) -> Result<HeadObjectOutput, RusotoError<HeadObjectError>> {
            unimplemented!()
        }

        async fn list_bucket_analytics_configurations(
            &self,
            _input: ListBucketAnalyticsConfigurationsRequest,
        ) -> Result<
            ListBucketAnalyticsConfigurationsOutput,
            RusotoError<ListBucketAnalyticsConfigurationsError>,
        > {
            unimplemented!()
        }

        async fn list_bucket_inventory_configurations(
            &self,
            _input: ListBucketInventoryConfigurationsRequest,
        ) -> Result<
            ListBucketInventoryConfigurationsOutput,
            RusotoError<ListBucketInventoryConfigurationsError>,
        > {
            unimplemented!()
        }

        async fn list_bucket_metrics_configurations(
            &self,
            _input: ListBucketMetricsConfigurationsRequest,
        ) -> Result<
            ListBucketMetricsConfigurationsOutput,
            RusotoError<ListBucketMetricsConfigurationsError>,
        > {
            unimplemented!()
        }

        async fn list_buckets(&self) -> Result<ListBucketsOutput, RusotoError<ListBucketsError>> {
            unimplemented!()
        }

        async fn list_multipart_uploads(
            &self,
            _input: ListMultipartUploadsRequest,
        ) -> Result<ListMultipartUploadsOutput, RusotoError<ListMultipartUploadsError>> {
            unimplemented!()
        }

        async fn list_object_versions(
            &self,
            _input: ListObjectVersionsRequest,
        ) -> Result<ListObjectVersionsOutput, RusotoError<ListObjectVersionsError>> {
            unimplemented!()
        }

        async fn list_objects(
            &self,
            _input: ListObjectsRequest,
        ) -> Result<ListObjectsOutput, RusotoError<ListObjectsError>> {
            unimplemented!()
        }

        async fn list_objects_v2(
            &self,
            _input: ListObjectsV2Request,
        ) -> Result<ListObjectsV2Output, RusotoError<ListObjectsV2Error>> {
            unimplemented!()
        }

        async fn list_parts(
            &self,
            _input: ListPartsRequest,
        ) -> Result<ListPartsOutput, RusotoError<ListPartsError>> {
            unimplemented!()
        }

        async fn put_bucket_accelerate_configuration(
            &self,
            _input: PutBucketAccelerateConfigurationRequest,
        ) -> Result<(), RusotoError<PutBucketAccelerateConfigurationError>> {
            unimplemented!()
        }

        async fn put_bucket_acl(
            &self,
            _input: PutBucketAclRequest,
        ) -> Result<(), RusotoError<PutBucketAclError>> {
            unimplemented!()
        }

        async fn put_bucket_analytics_configuration(
            &self,
            _input: PutBucketAnalyticsConfigurationRequest,
        ) -> Result<(), RusotoError<PutBucketAnalyticsConfigurationError>> {
            unimplemented!()
        }

        async fn put_bucket_cors(
            &self,
            _input: PutBucketCorsRequest,
        ) -> Result<(), RusotoError<PutBucketCorsError>> {
            unimplemented!()
        }

        async fn put_bucket_encryption(
            &self,
            _input: PutBucketEncryptionRequest,
        ) -> Result<(), RusotoError<PutBucketEncryptionError>> {
            unimplemented!()
        }

        async fn put_bucket_inventory_configuration(
            &self,
            _input: PutBucketInventoryConfigurationRequest,
        ) -> Result<(), RusotoError<PutBucketInventoryConfigurationError>> {
            unimplemented!()
        }

        async fn put_bucket_lifecycle(
            &self,
            _input: PutBucketLifecycleRequest,
        ) -> Result<(), RusotoError<PutBucketLifecycleError>> {
            unimplemented!()
        }

        async fn put_bucket_lifecycle_configuration(
            &self,
            _input: PutBucketLifecycleConfigurationRequest,
        ) -> Result<(), RusotoError<PutBucketLifecycleConfigurationError>> {
            unimplemented!()
        }

        async fn put_bucket_logging(
            &self,
            _input: PutBucketLoggingRequest,
        ) -> Result<(), RusotoError<PutBucketLoggingError>> {
            unimplemented!()
        }

        async fn put_bucket_metrics_configuration(
            &self,
            _input: PutBucketMetricsConfigurationRequest,
        ) -> Result<(), RusotoError<PutBucketMetricsConfigurationError>> {
            unimplemented!()
        }

        async fn put_bucket_notification(
            &self,
            _input: PutBucketNotificationRequest,
        ) -> Result<(), RusotoError<PutBucketNotificationError>> {
            unimplemented!()
        }

        async fn put_bucket_notification_configuration(
            &self,
            _input: PutBucketNotificationConfigurationRequest,
        ) -> Result<(), RusotoError<PutBucketNotificationConfigurationError>> {
            unimplemented!()
        }

        async fn put_bucket_policy(
            &self,
            _input: PutBucketPolicyRequest,
        ) -> Result<(), RusotoError<PutBucketPolicyError>> {
            unimplemented!()
        }

        async fn put_bucket_replication(
            &self,
            _input: PutBucketReplicationRequest,
        ) -> Result<(), RusotoError<PutBucketReplicationError>> {
            unimplemented!()
        }

        async fn put_bucket_request_payment(
            &self,
            _input: PutBucketRequestPaymentRequest,
        ) -> Result<(), RusotoError<PutBucketRequestPaymentError>> {
            unimplemented!()
        }

        async fn put_bucket_tagging(
            &self,
            _input: PutBucketTaggingRequest,
        ) -> Result<(), RusotoError<PutBucketTaggingError>> {
            unimplemented!()
        }

        async fn put_bucket_versioning(
            &self,
            _input: PutBucketVersioningRequest,
        ) -> Result<(), RusotoError<PutBucketVersioningError>> {
            unimplemented!()
        }

        async fn put_bucket_website(
            &self,
            _input: PutBucketWebsiteRequest,
        ) -> Result<(), RusotoError<PutBucketWebsiteError>> {
            unimplemented!()
        }

        async fn put_object(
            &self,
            input: PutObjectRequest,
        ) -> Result<PutObjectOutput, RusotoError<PutObjectError>> {
            self.puts.lock().unwrap().borrow_mut().push(input);
            Ok(PutObjectOutput::default())
        }

        async fn put_object_acl(
            &self,
            _input: PutObjectAclRequest,
        ) -> Result<PutObjectAclOutput, RusotoError<PutObjectAclError>> {
            unimplemented!()
        }

        async fn put_object_legal_hold(
            &self,
            _input: PutObjectLegalHoldRequest,
        ) -> Result<PutObjectLegalHoldOutput, RusotoError<PutObjectLegalHoldError>> {
            unimplemented!()
        }

        async fn put_object_lock_configuration(
            &self,
            _input: PutObjectLockConfigurationRequest,
        ) -> Result<PutObjectLockConfigurationOutput, RusotoError<PutObjectLockConfigurationError>>
        {
            unimplemented!()
        }

        async fn put_object_retention(
            &self,
            _input: PutObjectRetentionRequest,
        ) -> Result<PutObjectRetentionOutput, RusotoError<PutObjectRetentionError>> {
            unimplemented!()
        }

        async fn put_object_tagging(
            &self,
            _input: PutObjectTaggingRequest,
        ) -> Result<PutObjectTaggingOutput, RusotoError<PutObjectTaggingError>> {
            unimplemented!()
        }

        async fn put_public_access_block(
            &self,
            _input: PutPublicAccessBlockRequest,
        ) -> Result<(), RusotoError<PutPublicAccessBlockError>> {
            unimplemented!()
        }

        async fn restore_object(
            &self,
            _input: RestoreObjectRequest,
        ) -> Result<RestoreObjectOutput, RusotoError<RestoreObjectError>> {
            unimplemented!()
        }

        async fn select_object_content(
            &self,
            _input: SelectObjectContentRequest,
        ) -> Result<SelectObjectContentOutput, RusotoError<SelectObjectContentError>> {
            unimplemented!()
        }

        async fn upload_part(
            &self,
            _input: UploadPartRequest,
        ) -> Result<UploadPartOutput, RusotoError<UploadPartError>> {
            unimplemented!()
        }

        async fn upload_part_copy(
            &self,
            _input: UploadPartCopyRequest,
        ) -> Result<UploadPartCopyOutput, RusotoError<UploadPartCopyError>> {
            unimplemented!()
        }
    }

    #[test]
    fn it_works() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.enter(|| {
            let s3_client = Mock::default();
            let puts = s3_client.puts.clone();
            let mut client = ClientBuilder::default()
                .batch_duration(300)
                .bucket("mybucky")
                .s3_client(s3_client)
                .handle(rt.handle().clone())
                .build()
                .unwrap();

            let data = ("12345678".to_owned().repeat(1024)).into_bytes();
            let len = data.len();
            let req = BatchPutObjectRequest {
                name: "test.txt".to_string(),
                body: data,
            };
            let put_ref = client.put_object(req).unwrap();
            assert_eq!(0, put_ref.offset_bytes);
            assert_eq!(len, put_ref.size_bytes);

            let data = "data-driven beard".to_owned().into_bytes();
            let len = data.len();
            let req = BatchPutObjectRequest {
                name: "data.log".to_string(),
                body: data,
            };
            let put_ref = client.put_object(req).unwrap();
            assert_eq!(8704, put_ref.offset_bytes);
            assert_eq!(len, put_ref.size_bytes);

            std::thread::sleep(std::time::Duration::from_millis(600));

            let guard = puts.lock().unwrap();
            let mut result = guard.borrow_mut();
            let first = result.pop().unwrap();
            assert_eq!(first.bucket, "mybucky");
            assert!(!first.key.is_empty());
            let mut arch = tar::Archive::new(first.body.unwrap().into_blocking_read());
            let mut iter = arch.entries().unwrap();

            let first = iter.next().unwrap().unwrap();
            assert_eq!(first.path().unwrap().to_str().unwrap(), "test.txt");

            let first = iter.next().unwrap().unwrap();
            assert_eq!(first.path().unwrap().to_str().unwrap(), "data.log");
        });
    }
}
