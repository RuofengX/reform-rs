use std::path::Path;

use dataset::Dataset;
use writer::to_parquet;

pub mod conf;
pub mod dataset;
pub mod reader;
pub mod writer;
pub mod prelude;

use prelude::*;


fn main() -> anyhow::Result<()> {
    use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};
    tracing_subscriber::registry().with(fmt::layer()).init();
    let ds = Dataset::new();
    ds.attach_folder(Path::new("./tmp/batch/"));
    // ds.attach_file(Path::new("./tmp/batch/马文清085e9858e6fad07d2df06b164@wx.tenpay.com财付通支付科技有限公司(20240101000000_20241024000000).xlsx"));
    let mut df = ds.done();

    to_parquet("tmp/batch_2.parquet", &mut df)?;
    to_csv("tmp/batch_2.csv", &mut df)?;

    Ok(())
}
