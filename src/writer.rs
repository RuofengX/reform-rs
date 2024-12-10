use std::path::Path;

use polars::prelude::*;
use tracing::info;

pub fn to_parquet(path: impl AsRef<Path>, df: &mut DataFrame) -> anyhow::Result<()> {
    let path = path.as_ref();
    info!("写入文件 - PARQUET - {:?}", path);
    let mut file = std::fs::File::create(path).unwrap();
    ParquetWriter::new(&mut file).finish(df).unwrap();
    info!("完成 - {:?}", path);
    Ok(())
}

pub fn to_csv(path: impl AsRef<Path>, df: &mut DataFrame) -> anyhow::Result<()> {
    let path = path.as_ref();
    info!("写入文件 - CSV - {:?}", path);
    let mut file = std::fs::File::create(path).unwrap();
    CsvWriter::new(&mut file).finish(df).unwrap();
    info!("完成 - {:?}", path);
    Ok(())
}
