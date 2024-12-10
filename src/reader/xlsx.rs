use std::{
    collections::HashSet,
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
};

use anyhow::bail;
use calamine::{Data, Range, Reader};
use chrono::DateTime;
use polars::prelude::*;

use crate::conf::{Config, NULL_MARK};

use super::{fmt_df, GenericFile};

pub struct XlsxData {
    path: PathBuf,
    sheet: Range<Data>,
    sig: Vec<String>,
    config: Config,
}
impl XlsxData {
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();

        // 读取excel表格
        let mut wb: calamine::Xlsx<BufReader<File>> = calamine::open_workbook(path)?;
        let sheet = wb
            .worksheet_range_at(0)
            .ok_or(anyhow::anyhow!("excel文件缺失第一个sheet"))??;
        let sig = sheet.headers().ok_or(anyhow::anyhow!("sheet缺失标题"))?;
        let config = Config::auto_detect(&sig)?;
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
        if end_row == 0 {
            bail!("文件为空")
        }

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

        fmt_df(df, &self.path, &config)
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
