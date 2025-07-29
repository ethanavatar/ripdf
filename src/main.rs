use std::ops::Deref;
use rayon::prelude::*;
use pdf::object::Resolve;
use image::{ImageBuffer, Rgb};
use std::sync::Arc;
use std::sync::Mutex;

use clap::Parser;

use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    input_pdf: PathBuf,

    #[clap(long)]
    dry_run: bool,
}

struct PageImage {
    page_number: usize,
    object_number: usize,
    width: u32,
    height: u32,
    data: Arc<[u8]>,
}

struct ProcessedImage {
    page_number: usize,
    object_number: usize,
    data: ImageBuffer<Rgb<u8>, Arc<[u8]>>,
}

fn main() -> Result<(), String> {
    let args = Args::parse();

    let mut file_name = args.input_pdf.clone();
    file_name.set_extension("");
    let file_name = file_name.as_path().file_name().unwrap().to_str().unwrap();

    let out_directory = std::path::PathBuf::from(file_name);
    if !out_directory.exists() {
        std::fs::create_dir(&out_directory).unwrap();
    }

    let mut out_directory_entries = vec![];

    for entry in std::fs::read_dir(&out_directory).unwrap() {
        match entry {
            Ok(dir) => out_directory_entries.push(dir.path()),
            _ => continue,
        }
    }

    if out_directory_entries.len() > 0 {
        return Err(format!("Output directory {} is not empty", out_directory.display()));
    }

    let in_path = args.input_pdf.clone();
    let file = pdf::file::FileOptions::cached().open(&in_path).expect("Failed to open file");

    let resolver = Arc::new(Mutex::new(file.resolver()));

    let numbered_pages: Vec<_> = file.pages().enumerate().collect();

    let page_images: Vec<_> = numbered_pages.par_iter().map(|(page_number, page)| {
        let image_objects = match page {
            Err(_) => vec![],
            Ok(p) => {
                let resolver = resolver.clone();
                let resolver = resolver.lock().unwrap();
                p.resources().unwrap().xobjects
                    .iter()
                    .map(|(_name, &resource)| resolver.get(resource).unwrap())
                    .filter(|o| matches!(**o, pdf::object::XObject::Image(_)))
                    .collect()
            },
        };

        println!("{} images found on page {}", image_objects.len(), page_number);

        let mut images = vec![];

        for (object_number, object) in image_objects.into_iter().enumerate() {
            let image = match *object {
                pdf::object::XObject::Image(ref im) => im,
                _ => unreachable!(),
            };

            let dict = image.deref().to_owned();
            let image_data = {
                let resolver = resolver.clone();
                let resolver = resolver.lock().unwrap();
                image.image_data(&*resolver).unwrap()
            };

            images.push(PageImage {
                page_number: *page_number,
                object_number: object_number,
                width: dict.width,
                height: dict.height,
                data: image_data.clone(),
            });
        }

        images
    }).collect();

    let mut processed = vec![];

    for images in page_images {
       let batch: Vec<_> = images.par_iter().map(|image| {
            let rgb: ImageBuffer<Rgb<u8>, Arc<[u8]>> = ImageBuffer::from_raw(
                image.width, image.height,
                image.data.clone()
            ).unwrap();

            ProcessedImage {
                page_number:   image.page_number,
                object_number: image.object_number,
                data: rgb,
            }
        }).collect();

       processed.extend(batch);
    }

    println!("processed {} images", processed.len());

    for image in processed {
        let mut out_path = out_directory.clone();
        out_path.push(format!("{}_{}_{}.png", file_name, image.page_number, image.object_number));

        if !args.dry_run {
            let file = std::fs::File::create(&out_path).unwrap();
            let mut writer = std::io::BufWriter::new(file);

            eprintln!(
                "writing image {} from page {} to {:?}",
                image.object_number,
                image.page_number,
                out_path.into_os_string()
            );

            image.data.write_with_encoder(
                image::codecs::png::PngEncoder::new_with_quality(
                    &mut writer,
                    image::codecs::png::CompressionType::Fast,
                    image::codecs::png::FilterType::Adaptive,
                )
            ).unwrap();

        } else {
            eprintln!(
                "would write image {} from page {} to {:?}",
                image.object_number,
                image.page_number,
                out_path.into_os_string()
            );
        }
    }

    return Ok(());
}
