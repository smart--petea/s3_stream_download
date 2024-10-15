use aws_sdk_s3 as s3;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::ReadBuf;



#[::tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(err) = dotenv::dotenv() {
        println!("Please copy the .env.dist into .env and provide the env variables with correct values");
        return Ok(());
    }

    let config = aws_config::load_from_env().await;
    let s3_aws_client = s3::Client::new(&config);

    let aws_bucket_from = String::from(std::env::var("AWS_BUCKET_FROM")?);
    let aws_key_from = String::from(std::env::var("AWS_KEY_FROM")?);
    let aws_bucket_to = String::from(std::env::var("AWS_BUCKET_TO")?);
    let aws_key_to = String::from(std::env::var("AWS_KEY_TO")?);
    aws_copy( s3_aws_client, aws_bucket_from, aws_key_from, aws_bucket_to, aws_key_to).await?;

    Ok(())
}

async fn aws_copy(
    s3_aws_client: s3::Client,
    aws_bucket_from: String,
    aws_key_from: String,
    aws_bucket_to: String,
    aws_key_to: String,
) -> Result<(), Box<dyn std::error::Error>>
{
    let chunk_size: usize = 5 * 1024 * 1024;
    let download_chunk_size: usize = 1024 * 1024;
    let retries = 3;
    let mut s3_stream = s3_stream_download::S3StreamDownload::new(s3_aws_client.clone(), aws_bucket_from.clone(), aws_key_from.clone(), download_chunk_size as i64, retries).await?;
    //let mut buffer = BytesMut::with_capacity(chunk_size);
    let mut part_number = 0;

    let mut upload_parts: Vec<s3::types::CompletedPart> = Vec::new();

    let multipart_upload_res = s3_aws_client
        .create_multipart_upload()
        .bucket(aws_bucket_to.clone())
        .key(aws_key_to.clone())
        .send()
        .await?;
    let upload_id = multipart_upload_res.upload_id.ok_or(std::io::Error::new(std::io::ErrorKind::Other, "Missing upload_id after CreateMultipartUpload"))?;


    let mut buffer = vec![0u8; chunk_size];
    let mut bufferReader = ReadBuf::new(&mut buffer);
    loop {
        bufferReader.clear();

        loop {
            let l1 = bufferReader.filled().len();
            s3_stream.read_buf(&mut bufferReader).await?;
            let l2 = bufferReader.filled().len();

            println!("downloaded {} bytes of 5mb part No={}", l2 - l1, part_number+1);
            if l1 == l2 {
                //following the tokio::AsyncRead docs. https://docs.rs/tokio/latest/tokio/io/trait.AsyncRead.html
                //if buffer is unchanged than no more bytes are expected
                break;
            }
        }
        if bufferReader.filled().len() == 0 {
            println!("Breaking the loop. No more downloads.");
            break;
        }
        println!("Part {} is downloaded.", part_number+1);

        part_number = part_number + 1;
        let byte_stream = s3::primitives::ByteStream::from(Vec::from(bufferReader.filled()));

        let upload_part_res = s3_aws_client
            .upload_part()
            .key(aws_key_to.clone())
            .bucket(aws_bucket_to.clone())
            .upload_id(upload_id.clone())
            .part_number(part_number)
            .body(byte_stream)
            .send()
            .await?;
        println!("Part {part_number:?} is uploaded");

        upload_parts.push(
            s3::types::CompletedPart::builder()
            .e_tag(upload_part_res.e_tag.unwrap_or_default())
            .part_number(part_number)
            .build()

        );
    }
    println!("Uploading the final part.");
    let completed_multipart_upload = s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();

    let _completed_multipart_upload_res = s3_aws_client 
        .complete_multipart_upload()
        .bucket(aws_bucket_to)
        .key(aws_key_to)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await?;

    println!("The upload of the whole is completed.");

    Ok(())
}
