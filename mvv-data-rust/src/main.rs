use polars::prelude::*;
use std::{fs::File, ops::Range};

use kdam::{tqdm, BarExt};

fn main() -> Result<(), PolarsError> {
    let n = 1_000_000_000;
    let n_chunks = 16;
    let output_dir = "test";

    std::fs::create_dir_all(output_dir).unwrap();
    let mut pb = tqdm!(total = n as usize);

    for chunk in 0..n_chunks {
        let start = (n / n_chunks) * chunk;
        let end = (n / n_chunks) * (chunk + 1);
        let n_rows: i64 = end - start;
        let index: Range<i64> = start..end;
        let mvv: Range<i64> = start..end;
        let mut df: DataFrame =
            DataFrame::new(vec![Series::new("index", index), Series::new("mvv", mvv)]).unwrap();

        let mut file = File::create(format!("{}/example_{}.parquet", output_dir, chunk))
            .expect("could not create file");
        let _ = ParquetWriter::new(&mut file).finish(&mut df);
        let _ = pb.update(n_rows as usize);
    }
    Ok(())
}
