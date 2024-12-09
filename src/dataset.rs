use std::path::Path;

use parking_lot::Mutex;
use polars::prelude::*;
use rayon::prelude::*;
use tracing::{info, info_span, warn};

use crate::reader::{BatchOpen, GEN_SCHEMA};

pub struct Dataset {
    inner: Mutex<LazyFrame>,
}

impl Dataset {
    pub fn new() -> Self {
        let inner = Mutex::new(DataFrame::empty_with_schema(&GEN_SCHEMA).lazy());
        Self { inner }
    }
}

impl Dataset {
    /// Dataframe 必须仅包含GEN_COLUMN
    fn attach_df(&self, value: DataFrame) -> &Self {
        let mut lf = self.inner.lock();
        match concat(
            [lf.clone(), value.lazy()],
            UnionArgs {
                parallel: true,
                rechunk: true,
                to_supertypes: false,
                diagonal: false,
                from_partitioned_ds: false,
            },
        ) {
            Ok(new_lf) => *lf = new_lf,
            Err(e) => warn!("读取文件时出错 - {e}"),
        }
        self
    }

    pub fn attach_file(&self, value: &Path) -> &Self {
        info_span!("添加数据");
        info!("读取文件 - {:?}", value);

        if let Ok(df) = crate::reader::read(value.to_path_buf()) {
            self.attach_df(df);
        } else {
            warn!("读取文件时出错");
        }
        self
    }

    pub fn attach_folder(&self, folder_path: &Path) -> &Self {
        info_span!("添加批量数据");
        let bat = BatchOpen::new(folder_path);
        bat.read_all().par_bridge().for_each(|x| {
            self.attach_df(x);
        });
        self
    }

    pub fn drop_duplicate(&self) -> &Self {
        info_span!("删除重复项（同双向主体ID、同时间、同文件名）");
        let mut lf = self.inner.lock();
        let new_lf = lf.clone().unique(
            Some(vec![
                "_from_id".to_string(),
                "_to_id".to_string(),
                "_amount".to_string(),
                "_datetime".to_string(),
            ]),
            UniqueKeepStrategy::Any,
        );
        *lf = new_lf;
        self
    }

    pub fn done(self) -> DataFrame {
        self.inner.into_inner().collect().unwrap()
    }
}

mod test {

    #[test]
    fn test_folder() -> anyhow::Result<()> {
        use super::*;
        use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

        tracing_subscriber::registry().with(fmt::layer()).init();
        let ds = Dataset::new();
        ds.attach_folder(Path::new("./tmp/batch/"));
        // ds.attach_file(Path::new("./tmp/batch/马文清085e9858e6fad07d2df06b164@wx.tenpay.com财付通支付科技有限公司(20240101000000_20241024000000).xlsx"));
        let mut df = ds.done();

        let mut file = std::fs::File::create("tmp/batch_1.csv").unwrap();
        CsvWriter::new(&mut file).finish(&mut df).unwrap();

        let mut file = std::fs::File::create("tmp/batch_1.parquet").unwrap();
        ParquetWriter::new(&mut file).finish(&mut df).unwrap();

        Ok(())
    }
}
