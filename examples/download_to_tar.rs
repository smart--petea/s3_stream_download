use tokio_tar::{Builder, Header};
use aws_sdk_s3::Client;
use std::io::Write;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(err) = dotenv::dotenv() {
        println!("Please copy the .env.dist into .env and provide the env variables with correct values");
        return Ok(());
    }

    let config = aws_config::load_from_env().await;
    let aws_client = Client::new(&config);

    let aws_bucket = String::from(std::env::var("AWS_BUCKET_FROM")?);
    let retries = 3;
    let chunk_size: usize = 100000;

    let mut tarfile = tokio::fs::File::create("/tmp/u.tar").await?;
    let mut ar = Builder::new(tarfile);

    let mut stream1jpeg = s3_stream_download::S3StreamDownload::new(aws_client.clone(), aws_bucket.clone(), String::from("files/1.jpeg"), chunk_size as i64, retries).await?;
    let mut header = Header::new_gnu();
    header.set_path("1.jpeg")?;
    header.set_size(stream1jpeg.content_length as u64);
    header.set_cksum();
    ar.append(&header, stream1jpeg).await?;

    let mut stream135mbmp4 = s3_stream_download::S3StreamDownload::new(aws_client, aws_bucket, String::from("files/135mb.mp4"), chunk_size as i64, retries).await?;
    let mut header = Header::new_gnu();
    header.set_path("135mb.mp4")?;
    header.set_size(stream135mbmp4.content_length as u64);
    header.set_cksum();
    ar.append(&header, stream135mbmp4).await?;

    Ok(())
}
