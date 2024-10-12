use std::task::Poll;
use std::pin::Pin;
use tokio::io::ReadBuf;
use std::task::Context;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::Client;
use std::future::Future;

pub struct S3StreamDownload
{
    bucket: String,
    key: String,
    client: Client,
    downloaded: i64,
    to_download: i64,
    get_object_future: Option<Pin<Box<dyn Future<Output = Result<GetObjectOutput, SdkError<GetObjectError, HttpResponse>>>>>>,
    get_object_output: Option<GetObjectOutput>,
    chunk_size: i64,
    retries_allowed: usize,
    retries_done: usize,
}

impl S3StreamDownload
{
    pub async fn new(client: Client, bucket: String, key: String, chunk_size: i64, retries: usize) -> Result<S3StreamDownload, String> {
        let content_length = client
            .head_object()
            .bucket(bucket.clone())
            .key(key.clone())
            .send()
            .await
            .map_err(|err| format!("{:?}", err))?
            .content_length;

        if content_length.is_none() {
            return Err(String::from("Empty content length"));
        }

        Ok(S3StreamDownload{
            client: client,
            bucket: bucket,
            key: key,
            get_object_future: None,
            get_object_output: None,
            downloaded: 0,
            to_download: content_length.unwrap(),
            chunk_size: chunk_size,
            retries_allowed: retries,
            retries_done: 0,
        })
    }
}

impl tokio::io::AsyncRead for S3StreamDownload
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>
    ) -> Poll<tokio::io::Result<()>> {
        let s3_stream = self.get_mut();

        if s3_stream.get_object_output.is_some() {
            let mut get_object_output = s3_stream.get_object_output.take().unwrap();
            let poll_result = Pin::new(&mut get_object_output.body).poll_next(cx);
            match poll_result {
                Poll::Pending => {
                    s3_stream.get_object_output.replace(get_object_output);
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }

                Poll::Ready(None) => {
                    //nothing.
                    //stream of the current part is exhausted
                    //carry on with downloading of the next part
                }

                Poll::Ready(Some(Ok(bytes))) => {
                    buf.put_slice(bytes.as_ref());
                    s3_stream.get_object_output.replace(get_object_output);
                    s3_stream.downloaded += bytes.len() as i64;
                    cx.waker().wake_by_ref();

                    return Poll::Ready(Ok(()));
                }

                Poll::Ready(Some(Err(err))) => {
                    if s3_stream.retries_allowed > s3_stream.retries_done {
                        s3_stream.retries_done = s3_stream.retries_done + 1;

                        return Poll::Pending;
                    }

                    let tokio_err = tokio::io::Error::new(tokio::io::ErrorKind::Other, format!("{err:?}"));
                    return Poll::Ready(Err(tokio_err));
                }
            }

        }

        if s3_stream.get_object_future.is_some() {
            let mut get_object_future = s3_stream.get_object_future.take().unwrap();
            let poll_result = get_object_future.as_mut().poll(cx);
            match poll_result {
                Poll::Pending => {
                    s3_stream.get_object_future.replace(get_object_future);
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }

                Poll::Ready(Ok(get_object_output)) => {
                    s3_stream.get_object_output.replace(get_object_output);
                    s3_stream.get_object_future.take();

                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(Err(aws_err)) => {
                    if let SdkError::ServiceError(ref service_err) = aws_err {
                        if service_err.err().meta().code() == Some("InvalidPartNumber") {
                            return Poll::Ready(Ok(()));
                        }
                    }

                    let tokio_err = tokio::io::Error::new(tokio::io::ErrorKind::Other, format!("{aws_err:?}"));
                    return Poll::Ready(Err(tokio_err));
                }
            }
        }

        let lower_range_bound = s3_stream.downloaded;
        let chunk_size = std::cmp::min(s3_stream.chunk_size,  buf.capacity() as i64);
        let upper_range_bound = std::cmp::min(s3_stream.downloaded + chunk_size - 1, s3_stream.to_download);
        if lower_range_bound == upper_range_bound {
            return Poll::Ready(Ok(()));
        }

        let range = format!("bytes={}-{}", lower_range_bound, upper_range_bound);
        let get_object_future = s3_stream.client
            .get_object()
            .bucket(s3_stream.bucket.clone())
            .key(s3_stream.key.clone())
            .range(range)
            .send();

        s3_stream.get_object_future.replace(Box::pin(get_object_future));
        cx.waker().wake_by_ref();
        return Poll::Pending;
    }
}
