use std::{
    collections::HashSet,
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
};

use calamine::{Data, Range, Reader};
use chrono::DateTime;
use polars::prelude::*;

use crate::conf::{Config, Trans, NULL_MARK};

use super::{GenericFile, GEN_COLUMN};

pub struct XlsxData {
    path: PathBuf,
    sheet: Range<Data>,
    sig: Vec<String>,
    config: Config,
}
impl XlsxData {
    pub fn new(path: &Path) -> anyhow::Result<Self> {
        // 读取excel表格
        let mut wb: calamine::Xlsx<BufReader<File>> = calamine::open_workbook(path)?;
        let sheet = wb
            .worksheet_range_at(0)
            .ok_or(anyhow::anyhow!("excel文件缺失第一个sheet"))??;
        let sig = sheet.headers().ok_or(anyhow::anyhow!("sheet缺失标题"))?;
        let config = Config::auto_detect(&sig).ok_or(anyhow::anyhow!(
            "未定义的文件表头 - {}",
            path.to_string_lossy()
        ))?;
        let path = path.into();
        Ok(Self {
            path,
            sheet,
            sig,
            config,
        })
    }

    fn to_polars_data(data: &calamine::Data) -> AnyValue<'_> {
        match data {
            calamine::Data::Int(value) => AnyValue::Int64(value.clone()),
            calamine::Data::Float(value) => AnyValue::Float64(value.clone()),
            calamine::Data::String(value) => {
                if NULL_MARK.contains(&value.as_str()) {
                    AnyValue::Null
                } else {
                    AnyValue::StringOwned(value.into())
                }
            }
            calamine::Data::Bool(value) => AnyValue::Boolean(value.clone()),
            calamine::Data::DateTime(value) => {
                if let Some(date_value) = value.as_datetime() {
                    // let ts = date_value.and_local_timezone(TIMEZONE.unwrap()).unwrap().timestamp_millis().expect("过大的日期");
                    // AnyValue::DatetimeOwned(ts, TimeUnit::Milliseconds, todo!("时区缺失"))
                    let ts = date_value.and_utc().timestamp_millis();
                    AnyValue::DatetimeOwned(ts, TimeUnit::Milliseconds, None)
                } else {
                    AnyValue::StringOwned(value.to_string().into())
                }
            }
            calamine::Data::DateTimeIso(value) => {
                if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
                    let dt = dt.to_utc().naive_local();
                    AnyValue::Datetime(datetime_to_timestamp_ns(dt), TimeUnit::Nanoseconds, None)
                } else {
                    AnyValue::StringOwned(value.into())
                }
            }
            calamine::Data::DurationIso(value) => {
                let duration = Duration::parse(value).nanoseconds();
                AnyValue::Duration(duration, TimeUnit::Nanoseconds)
            }
            calamine::Data::Error(e) => AnyValue::StringOwned(e.to_string().into()),
            calamine::Data::Empty => AnyValue::Null,
        }
    }
}
impl GenericFile for XlsxData {
    fn load(path: &Path) -> anyhow::Result<Box<Self>> {
        Ok(Box::new(Self::new(path)?))
    }

    fn path(&self) -> PathBuf {
        self.path.clone()
    }

    fn signature(&self) -> &[String] {
        &self.sig
    }

    fn into_dataframe(self) -> anyhow::Result<DataFrame> {
        let sheet = self.sheet;
        let sig = self.sig;
        let config = self.config;
        let mut unique_col: HashSet<String> = HashSet::default();

        let end_row = sheet.height() as u32 - 1;
        let mut data = Vec::new();
        for (n, name) in sig.iter().enumerate() {
            let col_index = (n) as u32;
            let col = sheet.range((1, col_index), (end_row, col_index));
            let col_parsed: Vec<AnyValue<'_>> = col
                .cells()
                .into_iter()
                .map(|(_, _, data)| Self::to_polars_data(data))
                .collect();

            let mut name = name.clone();
            if unique_col.contains(&name) {
                name = format!("{name}_dup");
            }
            unique_col.insert(name.clone());

            let column = Column::Series(Series::from_any_values(name.into(), &col_parsed, false)?);

            data.push(column);
        }

        let df = DataFrame::new(data)?;
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
        let path_lit: PlSmallStr = self.path.to_str().unwrap().into();
        lf = lf.with_column(lit(path_lit).alias("_file_path"));

        let df = lf.collect()?;

        let ret = df.select(GEN_COLUMN)?;

        Ok(ret)
    }
}

mod test {

    #[test]
    fn test_folder() -> anyhow::Result<()> {
        use super::*;
        use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

        tracing_subscriber::registry().with(fmt::layer()).init();

        // ds.attach_folder(Path::new("./tmp/batch/"));
        let mut df = XlsxData::new(
            Path::new("./tmp/batch/马文清085e9858e6fad07d2df06b164@wx.tenpay.com财付通支付科技有限公司(20240101000000_20241024000000).xlsx"))?
            .into_dataframe()?;

        let mut file = std::fs::File::create("tmp/1.csv").unwrap();
        CsvWriter::new(&mut file).finish(&mut df).unwrap();
        Ok(())
    }
}
