use std::{
    path::{Path, PathBuf},
    sync::LazyLock,
};

use crate::{conf::Trans, prelude::*};

pub mod csv;
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
pub fn read(path: impl AsRef<Path>) -> anyhow::Result<DataFrame> {
    info_span!("打开文件");
    let path = path.as_ref();
    let ext = path.extension().unwrap().to_str().unwrap();
    match ext {
        "xlsx" => xlsx::XlsxData::new(&path)?.into_dataframe(),
        "csv" => csv::CsvData::new(&path)?.into_dataframe(),
        _ => anyhow::bail!("未实现的格式 - {}", ext),
    }
}

/// 批量打开
pub struct BatchOpen {
    folder: PathBuf,
}
impl BatchOpen {
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
        ret
    }

    pub fn read_all<'s>(&'s self) -> impl Iterator<Item = DataFrame> + 's {
        self.walk()
            .into_iter()
            .map(|x| {
                info!("读取文件 - {:?}", &x);
                x
            })
            .filter_map(|path| match read(&path) {
                Ok(df) => Some(df),
                Err(e) => {
                    warn!("无法读取文件 - {:?} - {}", &path, e);
                    None
                }
            })
    }
}

fn fmt_df(df: DataFrame, path: &PathBuf, config: &Config) -> anyhow::Result<DataFrame> {
    let mut lf = df.lazy();

    // 添加配置名
    lf = lf.with_column(lit(config.name).alias("_config_name"));

    // 去重
    if let Some(col_id) = config.col_id {
        lf = lf.unique(Some(vec![col_id.to_string()]), UniqueKeepStrategy::Any);
    };

    // 自动处理时间
    lf = lf.with_column(
        col(config.time.col)
            .str()
            .to_datetime(
                Some(TimeUnit::Milliseconds),
                None,
                StrptimeOptions {
                    format: Some(config.time.fmt.into()),
                    strict: false,
                    exact: true,
                    cache: false,
                },
                lit("raise"),
            )
            .alias("__time_0"),
    );
    if let Some(fmt_date) = config.time.fmt_alter {
        lf = lf.with_column(
            col(config.time.col)
                .str()
                .to_datetime(
                    Some(TimeUnit::Milliseconds),
                    None,
                    StrptimeOptions {
                        format: Some(fmt_date.into()),
                        strict: false,
                        exact: true,
                        cache: false,
                    },
                    lit("raise"),
                )
                .alias("__time_1"),
        );
    } else {
        lf = lf.with_column(lit(NULL).alias("__time_1"));
    }
    // 将两列时间合并
    lf = lf
        .with_column(coalesce(&[col("__time_0"), col("__time_1")]).alias("_datetime"))
        .drop(["__time_0", "__time_1"]);

    // 自动处理金额
    if let Trans::Duplex {
        col_amount,
        one,
        other,
        col_direct,
        value_out,
    } = config.trans
    {
        lf = lf
            .with_column(
                col(col_amount)
                    .cast(DataType::Float64)
                    .abs()
                    .strict_cast(DataType::Decimal(Some(38), Some(2)))
                    .abs()
                    // .cast(DataType::Decimal(Some(38), None))
                    .alias("_amount"),
            )
            .with_column(
                when(col(col_direct).eq(lit(value_out)))
                    .then(one.id_expr().alias("_from_id"))
                    .otherwise(other.id_expr().alias("_to_id")),
            )
            .with_column(
                when(col(col_direct).eq(lit(value_out)))
                    .then(other.id_expr().alias("_to_id"))
                    .otherwise(one.id_expr().alias("_from_id")),
            )
            .with_column(
                when(col(col_direct).eq(lit(value_out)))
                    .then(one.name_expr().alias("_from_name"))
                    .otherwise(other.name_expr().alias("_to_name")),
            )
            .with_column(
                when(col(col_direct).eq(lit(value_out)))
                    .then(other.name_expr().alias("_to_name"))
                    .otherwise(one.name_expr().alias("_from_name")),
            )
            .with_column(
                when(col(col_direct).eq(lit(value_out)))
                    .then(one.bank_id_expr().alias("_from_bank_id"))
                    .otherwise(other.bank_id_expr().alias("_to_bank_id")),
            )
            .with_column(
                when(col(col_direct).eq(lit(value_out)))
                    .then(other.bank_id_expr().alias("_to_bank_id"))
                    .otherwise(one.bank_id_expr().alias("_from_bank_id")),
            );
    }
    if let Trans::Simple {
        col_amount,
        from,
        to,
    } = config.trans
    {
        lf = lf
            .with_column(
                col(col_amount)
                    .cast(DataType::Float64)
                    .abs()
                    .strict_cast(DataType::Decimal(Some(38), Some(2)))
                    .alias("_amount"),
            )
            .with_column(from.id_expr().alias("_from_id"))
            .with_column(to.id_expr().alias("_to_id"))
            .with_column(from.name_expr().alias("_from_name"))
            .with_column(to.name_expr().alias("_to_name"))
            .with_column(from.bank_id_expr().alias("_from_bank_id"))
            .with_column(to.bank_id_expr().alias("_to_bank_id"))
    }

    // 添加路径列
    let path_lit: PlSmallStr = path.to_str().unwrap().into();
    lf = lf.with_column(lit(path_lit).alias("_file_path"));

    let df = lf.collect()?;

    let ret = df.select(GEN_COLUMN)?;

    Ok(ret)
}
