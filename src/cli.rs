use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "trein-video")]
#[command(about = "Distributed video converter for NAS", long_about = None)]
pub struct Args {
    /// Path to configuration file
    #[arg(short, long)]
    pub config: PathBuf,

    /// Instance ID (override config)
    #[arg(short, long)]
    pub instance_id: Option<String>,

    /// Role: master or worker (override config)
    #[arg(short, long)]
    pub role: Option<String>,
}

pub fn parse_args() -> Args {
    Args::parse()
}
