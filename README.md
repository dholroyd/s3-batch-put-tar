# s3-batch-put-object

Gather many objects into a smaller number of s3:PutObject calls

## Overview

A workload that creates numerous PutObject calls may be cost-inefficient due to the transactional cost of using the
S3 PutObject API.

The crate provides the calling application with a similar interface to [rusoto_s3](https://crates.io/crates/rusoto_s3),
and instead of writing objects to S3 immediately, it spools them to a local
[tar file](https://en.wikipedia.org/wiki/Tar_(computing)).  The calling application can control how often the tar file
containing the last batch of objects will be written to S3.

The `S3BatchPutClient` is safe to share across threads, and can coalesce writes from multiple concurrent threads into
a single batch.

## Efficient retrieval

The resulting tar files may simply be downloaded to consume each batch as a whole.

The caller of `S3BatchPutClient::put_object()` will also receive metadata about the location of the given object
within the tar file.  Once the batch has been written to S3, this metadata enables efficient access to individual
objects within the batch, by allowing you to make an S3 byte-range request that retrieves a single object without needing
to download the whole tar file.

## Target workload

This create might help if your workload has the following attributes:

 - The cost of `s3:PutObject` operations is a significant and needs to be reduced
 - Can tolerate some extra delay before objects become available to read (the delay introduced by the batching together of multiple writes)
 - Preventing writes from happening concurrently is generally not a problem (the batch serialises writes, by design)
 - The time between the workload creating one object and the next will generally be less than the batching-duration
   (batches will contain multiple items often enough to pay for the cost of creating the batches)
 - Does not need to be able to supply S3 metadata for individual objects (the scope for including metadata for objects
   in the tar file holding the batch will be very limited)
 - Does not need readers to be able to access individual objects via the `s3:GetObject` API (i.e. readers are happy to
   either download a whole batch of objects as a tar file, or to make byte-range requests to retrieve individual
   objects)