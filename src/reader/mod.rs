use std::{
    path::{Path, PathBuf},
    sync::LazyLock,
};

use polars::prelude::*;
use tracing::{info, info_span, warn};

pub mod xlsx;

// 表头名
pub const GEN_COLUMN: [&'static str; 10] = [
    "_config_name",
    "_datetime",
    "_amount",
    "_from_id",
    "_from_bank_id",
    "_from_name",
    "_to_id",
    "_to_bank_id",
    "_to_name",
    "_file_path",
];

const GEN_SCHEMA_LIT: [DataType; 10] = [
    DataType::String,
    DataType::Datetime(TimeUnit::Milliseconds, None),
    DataType::Decimal(Some(38), Some(2)),
    DataType::String,
    DataType::String,
    DataType::String,
    DataType::String,
    DataType::String,
    DataType::String,
    DataType::String,
];

fn gen_schema() -> Arc<Schema> {
    let mut ret = Schema::with_capacity(10);
    for i in 0..10 {
        ret.insert(GEN_COLUMN[i].into(), GEN_SCHEMA_LIT[i].clone());
    }
    Arc::new(ret)
}

/// 表头类型
pub const GEN_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(gen_schema);

/// 通用文件读取
pub trait GenericFile {
    /// 加载文件
    fn load(path: &Path) -> anyhow::Result<Box<Self>>;

    /// 获取文件路径
    fn path(&self) -> PathBuf;

    /// 获取签名
    ///
    /// 所有列名作为账单类型签名
    fn signature(&self) -> &[String];

    /// 读取文件内容并转换为df,
    /// df仅包含 [[`GEN_COLUMN`]] 列
    fn into_dataframe(self) -> anyhow::Result<DataFrame>;
}

/// 将任意文件打开为df
pub fn read(path: PathBuf) -> anyhow::Result<DataFrame> {
    info_span!("打开文件");
    let ext = path.extension().unwrap().to_str().unwrap();
    match ext {
        "xlsx" => xlsx::XlsxData::new(&path)?.into_dataframe(),
        _ => anyhow::bail!("未实现的格式 - {}", ext),
    }
}

/// 批量打开
pub struct BatchOpen {
    folder: PathBuf,
}
impl BatchOpen {
    pub const SUPPORT_EXT: &[&str] = &["xlsx"];
    pub fn new(folder: &Path) -> Self {
        let folder = folder.into();
        BatchOpen { folder }
    }

    pub fn walk(&self) -> Vec<PathBuf> {
        info_span!("遍历文件夹");
        let mut ret = vec![];
        let walker = walkdir::WalkDir::new(&self.folder);
        for entry in walker {
            if let Err(e) = entry {
                warn!("遍历文件系统错误 - {}", e);
                continue;
            }
            let entry = entry.unwrap();
            if entry.file_type().is_file() {
                ret.push(entry.path().to_path_buf());
            }
        }
        let ret = ret
            .into_iter()
            .filter(|x| {
                let ext = x.extension().unwrap().to_str().unwrap();
                Self::SUPPORT_EXT.contains(&ext)
            })
            .collect();
        ret
    }

    pub fn read_all<'s>(&'s self) -> impl Iterator<Item = DataFrame> + 's {
        self.walk()
            .into_iter()
            .map(|x| {
                info!("读取文件 - {:?}", &x);
                x
            })
            .filter_map(|path| match read(path) {
                Ok(df) => Some(df),
                Err(e) => {
                    warn!("无法读取文件 - {}", e);
                    None
                }
            })
    }
}
