use std::ops::Deref;
use rayon::prelude::*;
use pdf::object::Resolve;
use image::{ImageBuffer, RgbaImage, RgbImage};
use std::sync::Arc;
use std::sync::Mutex;

use image::Rgb;

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

fn main() {
    let file_name = "10AE_Promo_Book";
    let in_path = std::path::PathBuf::from(file_name.to_owned() + ".pdf");
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

            println!("getting data for image {} on page {}", object_number, page_number);

            let dict = image.deref().to_owned();
            let image_data = {
                let resolver = resolver.clone();
                let resolver = resolver.lock().unwrap();
                image.image_data(&*resolver).unwrap()
            };

            println!("got data for image {} on page {}", object_number, page_number);

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
            println!("copying data for image {} on page {}", image.object_number, image.page_number);

            let rgb: ImageBuffer<Rgb<u8>, Arc<[u8]>> = ImageBuffer::from_raw(
                image.width, image.height,
                image.data.clone()
            ).unwrap();

            println!("copied data for image {} on page {}", image.object_number, image.page_number);

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
        let mut out_path = in_path.clone();
        out_path.set_file_name(format!("{}_{}_{}.tiff", file_name, image.page_number, image.object_number));

        println!("would write image to {:?}", out_path.into_os_string());
        //let file = File::create(out_path)?;
        //let mut buf_writer = BufWriter::new(file);
    }
}
