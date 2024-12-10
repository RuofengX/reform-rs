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
    ds.attach_folder("./tmp/batch");
    let mut df = ds.done();

    to_parquet("tmp/batch.parquet", &mut df)?;
    // to_csv("tmp/batch.csv", &mut df)?;

    Ok(())
}
