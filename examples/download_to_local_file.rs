use aws_sdk_s3 as s3;
use tokio::io::AsyncReadExt;
use bytes::BytesMut;
use bytes::Buf;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;


#[::tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(err) = dotenv::dotenv() {
        println!("Please copy the .env.dist into .env and provide the env variables with correct values");
        return Ok(());
    }

    let config = aws_config::load_from_env().await;
    let s3_aws_client = s3::Client::new(&config);

    let aws_bucket = String::from(std::env::var("AWS_BUCKET_FROM")?);
    let local_path = "/tmp/1.jpeg";
    let aws_key = String::from(std::env::var("AWS_KEY_FROM")?);
    download_to_local_file(s3_aws_client.clone(), aws_bucket, aws_key, local_path).await?;

    Ok(())
}

async fn download_to_local_file(
    s3_aws_client: s3::Client,
    aws_bucket: String,
    aws_key: String,
    local_path: &str
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::create(local_path).await?;
    let chunk_size: usize = 100000;
    let retries = 3;
    let mut s3_stream = s3_stream_download::S3StreamDownload::new(s3_aws_client, aws_bucket, aws_key, chunk_size as i64, retries).await?;
    let mut buffer = BytesMut::with_capacity(chunk_size);

    loop {
        buffer.clear();
        s3_stream.read_buf(&mut buffer).await?;
        println!("buffer.len={:?}", buffer.len());

        if buffer.len() == 0 {
            break;
        }

        file.write(&buffer[..]).await?;
    }

    Ok(())
}
