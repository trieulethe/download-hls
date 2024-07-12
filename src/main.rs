use reqwest::Client;
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::Duration;

// Function to download the M3U8 file and parse its segments
async fn download_m3u8(url: &str) -> Result<(String, Vec<String>), Box<dyn Error + Send + Sync>> {
    let client = Client::new();
    let response = client.get(url).send().await?;
    let content = response.text().await?;
    println!("M3U8 Content:\n{}", content);

    // Create base URL from M3U8 URL
    let base_url = if url.ends_with(".m3u8") {
        url.rsplitn(2, '/').nth(1).unwrap_or("")
    } else {
        url.rsplitn(2, '/').nth(0).unwrap_or("")
    };

    // Collect all segment URLs
    let segments: Vec<String> = content
        .lines()
        .filter(|line| !line.starts_with("#") && !line.trim().is_empty())
        .map(|line| {
            let segment_url = if line.starts_with("http") {
                line.to_string()
            } else {
                format!("{}/{}", base_url, line)
            };
            println!("Segment URL: {}", segment_url);
            segment_url
        })
        .collect();

    Ok((content, segments))
}

// Function to download an individual segment
async fn download_segment(client: &Client, segment_url: &str) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    println!("Downloading segment: {}", segment_url);
    for attempt in 0..5 {
        let result = tokio::time::timeout(Duration::from_secs(15), client.get(segment_url).send()).await;
        match result {
            Ok(Ok(res)) => {
                if res.status().is_success() {
                    let bytes = res.bytes().await?;
                    println!("DONE segment: {}", segment_url);
                    return Ok(bytes.to_vec());
                } else {
                    eprintln!("Failed to download segment {}: HTTP {}", segment_url, res.status());
                }
            }
            Ok(Err(err)) => {
                eprintln!("Error downloading {}: {}", segment_url, err);
            }
            Err(_) => {
                eprintln!("Timeout downloading segment {}", segment_url);
            }
        }

        if attempt < 4 {
            // Delay before retrying
            println!("Retry download: {}", segment_url);
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }
    }

    Err(format!("Failed to download segment {} after 5 attempts", segment_url).into())
}

// Function to download all segments concurrently
async fn download_segments(segments: Vec<String>, output_dir: &str, max_concurrent: usize) -> Result<(), Box<dyn Error + Send + Sync>> {
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let semaphore = Arc::new(Semaphore::new(max_concurrent)); // Limit the number of concurrent tasks
    fs::create_dir_all(output_dir)?;

    let mut tasks = Vec::new();

    for segment_url in segments {
        let client = client.clone();
        let output_dir = output_dir.to_string();
        let semaphore = Arc::clone(&semaphore);

        let task = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap(); // Wait for a permit before proceeding
            let segment_name = segment_url.split('/').last().unwrap_or("segment.ts");
            let segment_path = Path::new(&output_dir).join(segment_name);

            let mut file = File::create(&segment_path).unwrap();

            match download_segment(&client, &segment_url).await {
                Ok(bytes) => {
                    if let Err(e) = file.write_all(&bytes) {
                        eprintln!("Failed to write segment {}: {}", segment_url, e);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to download segment {}: {}", segment_url, e);
                }
            }

            // Check if file is 0 KB and retry download if necessary
            let metadata = fs::metadata(&segment_path).unwrap();
            if metadata.len() == 0 {
                eprintln!("File {} is 0 KB, retrying download...", segment_path.display());
                for _ in 0..5 {
                    match download_segment(&client, &segment_url).await {
                        Ok(bytes) => {
                            let mut file = File::create(&segment_path).unwrap();
                            if let Err(e) = file.write_all(&bytes) {
                                eprintln!("Failed to write segment {}: {}", segment_url, e);
                            }
                            if fs::metadata(&segment_path).unwrap().len() > 0 {
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!("Retry failed to download segment {}: {}", segment_url, e);
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        });

        tasks.push(task);
    }

    for task in tasks {
        task.await.unwrap();
    }

    Ok(())
}

// Function to rewrite the M3U8 file with local segment paths
fn rewrite_m3u8(content: &str, output_file: &str) -> io::Result<()> {
    let reader = BufReader::new(content.as_bytes());
    let mut output = File::create(output_file)?;

    for line in reader.lines() {
        let line = line?;
        if line.starts_with("#") {
            writeln!(output, "{}", line)?;
        } else if line.trim().is_empty() {
            writeln!(output)?;
        } else {
            let file_name = line.split('/').last().unwrap_or(&line);
            writeln!(output, "{}", file_name)?;
        }
    }

    Ok(())
}

// Main function to coordinate the download process
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let m3u8_url = "https://master3.baraq.xyz/media/6663ddab92303XDn/1080/hls/index.m3u8";
    let output_dir = "output_segments";
    let rewritten_m3u8_file = format!("{}/index.m3u8", output_dir); // Save rewritten M3U8 in output directory
    let max_concurrent = 10; // Number of concurrent tasks

    // Download M3U8 and get segments
    let (m3u8_content, segments) = download_m3u8(m3u8_url).await?;

    // Download segments and save to output directory
    download_segments(segments, output_dir, max_concurrent).await?;

    // Rewrite M3U8 file with modified TS file URLs
    rewrite_m3u8(&m3u8_content, &rewritten_m3u8_file)?;

    println!("Download completed!");
    Ok(())
}
