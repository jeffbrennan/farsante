use crate::generators::{
    GroupByGenerator, JoinGeneratorBig, JoinGeneratorMedium, JoinGeneratorSmall, RowGenerator,
};
use std::fs::File;
use std::io::{prelude::*, BufWriter};
use std::{fs, path::PathBuf};

use kdam::{tqdm, BarExt};

use apache_avro::types::Record;
use apache_avro::GenericSingleObjectWriter;
use apache_avro::Schema;
use apache_avro::Writer;
use serde::Serialize;
pub fn pretty_sci(num: u64) -> String {
    if num == 0 {
        return "NA".to_string();
    };
    let mut digits: Vec<u8> = Vec::new();
    let mut x = num;
    while x > 0 {
        digits.push((x % 10) as u8);
        x = x / 10;
    }
    format!("{}e{}", digits.pop().unwrap_or(0), digits.len())
}

#[derive(Debug, Serialize)]
struct AvroGroupbyRow {
    id1: String,
    id2: String,
    id3: String,
    id4: String,
    id5: String,
    id6: String,
    v1: i64,
    v2: i64,
    v3: f32,
}

pub enum DsType {
    GroupBy,
    JoinBig,
    JoinBigNa,
    JoinSmall,
    JoinMedium,
}

pub fn generate_avro(
    output_name: String,
    n: u64,
    k: u64,
    nas: u8,
    seed: u64,
    ds_type: &DsType,
) -> () {
    let groupby_schema_raw = r#"
    {
        "type": "record",
        "name": "G1",
        "fields": [
            {"name": "id1", "type": "string"},
            {"name": "id2", "type": "string"},
            {"name": "id3", "type": "string"},
            {"name": "id4", "type": "string"},
            {"name": "id5", "type": "string"},
            {"name": "id6", "type": "string"},
            {"name": "v1", "type": "long"},
            {"name": "v2", "type": "long"},
            {"name": "v3", "type": "float"}
            ]
    }
    "#;

    let schema = apache_avro::Schema::parse_str(groupby_schema_raw).unwrap();
    let mut file = File::create(output_name).expect("Unable to create file");
    let mut writer = Writer::new(&schema, &mut file);

    // let row = AvroGroupbyRow {
    //     id1: "id1".to_owned(),
    //     id2: "id2".to_owned(),
    //     id3: "id3".to_owned(),
    //     id4: "id4".to_owned(),
    //     id5: "id5".to_owned(),
    //     id6: "id6".to_owned(),
    //     v1: 1,
    //     v2: 2,
    //     v3: 3.0,
    // };

    let generator = GroupByGenerator::new(n, k, nas, seed);
    for _ in 0..n {
        let row = generator.get_values();
        writer.append_ser(row).unwrap();
    }
}

pub fn generate_csv(
    output_name: String,
    n: u64,
    k: u64,
    nas: u8,
    seed: u64,
    ds_type: &DsType,
) -> () {
    // initialize an instance of rowgenerator depending on the ds_type
    let mut generator: Box<dyn RowGenerator> = match ds_type {
        DsType::GroupBy => Box::new(GroupByGenerator::new(n, k, nas, seed)),
        DsType::JoinBig => Box::new(JoinGeneratorBig::new(n, k, 0, seed)),
        DsType::JoinBigNa => Box::new(JoinGeneratorBig::new(n, k, nas, seed)),
        DsType::JoinMedium => Box::new(JoinGeneratorMedium::new(n, k, nas, seed)),
        DsType::JoinSmall => Box::new(JoinGeneratorSmall::new(n, k, nas, seed)),
    };

    let n_divisor = match ds_type {
        DsType::GroupBy => 1,
        DsType::JoinBig => 1,
        DsType::JoinBigNa => 1,
        DsType::JoinMedium => 1_000,
        DsType::JoinSmall => 1_000_000,
    };

    let bar_position = match ds_type {
        DsType::GroupBy => 0,
        DsType::JoinBig => 1,
        DsType::JoinBigNa => 2,
        DsType::JoinMedium => 3,
        DsType::JoinSmall => 4,
    };

    let n_rows = n / n_divisor;
    let mut pb = tqdm!(total = n_rows as usize, position = bar_position);
    pb.set_postfix(format!("{}", output_name));
    let _ = pb.refresh();

    let _ = fs::write(PathBuf::from(&output_name), generator.get_csv_header());
    let file = fs::OpenOptions::new()
        .append(true)
        .write(true)
        .open(output_name)
        .unwrap();

    let mut writer = BufWriter::new(file);
    for _ in 0..n_rows {
        writer
            .write(generator.get_csv_row().as_bytes())
            .expect("couldn't write to file");
        pb.update(1).unwrap();
    }
}
