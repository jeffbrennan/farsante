use polars::prelude::*;
use std::{fs::File, ops::Range};

use kdam::{tqdm, BarExt};

pub enum DsType {
    XSmall,
    Small,
    Medium,
    Large,
    XLarge,
    XXLarge,
    XXXLarge,
}

fn main() -> Result<(), PolarsError> {
    for ds_type in vec![
        DsType::XSmall,
        DsType::Small,
        DsType::Medium,
        DsType::Large,
        DsType::XLarge,
        DsType::XXLarge,
        DsType::XXXLarge,
    ] {
        let n = match ds_type {
            DsType::XSmall => 1_000,
            DsType::Small => 100_000,
            DsType::Medium => 10_000_000,
            DsType::Large => 100_000_000,
            DsType::XLarge => 1_000_000_000,
            DsType::XXLarge => 10_000_000_000,
            DsType::XXXLarge => 100_000_000_000,
        };

        let n_chunks = match ds_type {
            DsType::XSmall => 1,
            DsType::Small => 1,
            DsType::Medium => 1,
            DsType::Large => 4,
            DsType::XLarge => 16,
            DsType::XXLarge => 128,
            DsType::XXXLarge => 512,
        };

        let dir_name = match ds_type {
            DsType::XSmall => "mvv_xsmall",
            DsType::Small => "mvv_small",
            DsType::Medium => "mvv_medium",
            DsType::Large => "mvv_large",
            DsType::XLarge => "mmv_xlarge",
            DsType::XXLarge => "mvv_xxlarge",
            DsType::XXXLarge => "mvv_xxxlarge",
        };

        let bar_position = match ds_type {
            DsType::XSmall => 0,
            DsType::Small => 1,
            DsType::Medium => 2,
            DsType::Large => 3,
            DsType::XLarge => 4,
            DsType::XXLarge => 5,
            DsType::XXXLarge => 6,
        };

        let output_dir = format!("data/{}", dir_name);
        std::fs::create_dir_all(&output_dir).unwrap();

        let mut pb = tqdm!(total = n as usize, position = bar_position);

        for chunk in 0..n_chunks {
            let start = (n / n_chunks) * chunk;
            let end = (n / n_chunks) * (chunk + 1);
            let n_rows: i64 = end - start;
            let index: Range<i64> = start..end;
            let mvv: Range<i64> = start..end;
            let mut df: DataFrame =
                DataFrame::new(vec![Series::new("index", index), Series::new("mvv", mvv)]).unwrap();

            let mut file = File::create(format!("{}/{}_{}.parquet", &output_dir, dir_name, chunk))
                .expect("could not create file");
            let _ = ParquetWriter::new(&mut file).finish(&mut df);
            let _ = pb.update(n_rows as usize);
        }
    }

    Ok(())
}
