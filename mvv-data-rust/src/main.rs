use polars::prelude::*;
use std::{fs::File, ops::Range};

fn main() -> Result<(), PolarsError> {
    let n = 10_000_000_000;
    let n_chunks = 128;
    let output_dir = "test";
    std::fs::create_dir_all(output_dir).unwrap();

    for chunk in 0..n_chunks {
        println!("writing chunk: {}", chunk);
        let start = (n / n_chunks) * chunk;
        let end = (n / n_chunks) * (chunk + 1);
        let index: Range<i64> = start..end;
        let mvv: Range<i64> = start..end;
        let mut df =
            DataFrame::new(vec![Series::new("index", index), Series::new("mvv", mvv)]).unwrap();

        let mut file = File::create(format!("{}/example_{}.parquet", output_dir, chunk))
            .expect("could not create file");
        let result = ParquetWriter::new(&mut file).finish(&mut df);
        match result {
            Ok(_) => println!("Success writing chunk: {}", chunk),
            Err(e) => println!("Error: {}", e),
        }
    }
    Ok(())
}
