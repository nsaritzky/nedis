use clap::Parser;

#[derive(Parser)]
pub struct Args {
    #[arg(long, default_value_t = 6379)]
    pub port: usize,
    #[arg(long)]
    pub replicaof: Option<String>,
    #[arg(long)]
    pub dir: Option<String>,
    #[arg(long)]
    pub dbfilename: Option<String>,
}
