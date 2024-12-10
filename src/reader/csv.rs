use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

use crate::prelude::*;

use super::{fmt_df, GenericFile};

pub struct CsvData {
    path: PathBuf,
    sig: Vec<String>,
    config: Config,
}

impl CsvData {
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();

        // 读取csv文件
        let header = {
            let mut file = BufReader::new(File::open(path)?);
            let mut header = String::with_capacity(1024);
            file.read_line(&mut header)?;
            header
        };
        let sig: Vec<String> = header
            .split(",")
            .into_iter()
            .map(|x| x.to_string())
            .collect();
        let path = path.into();

        let config = Config::auto_detect(&sig)?;

        let ret = CsvData { path, sig, config };

        Ok(ret)
    }
}

impl GenericFile for CsvData {
    fn path(&self) -> PathBuf {
        self.path.clone()
    }

    fn signature(&self) -> &[String] {
        &self.sig
    }

    fn into_dataframe(self) -> anyhow::Result<DataFrame> {
        let df = CsvReadOptions::default()
            .try_into_reader_with_file_path(Some(self.path.clone()))?
            .finish()?;

        fmt_df(df, &self.path, &self.config)
    }
}
