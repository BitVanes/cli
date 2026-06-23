//! `bitvanes` — zero-trust ETL for AI/RAG.
//!
//! Processes documents into BPE-aware chunks with structural context,
//! outputting Apache Arrow IPC, CSV, or JSON. Runs the same pipeline
//! as the web app, natively.

mod headless;
mod tui;

use clap::Parser;
use std::path::PathBuf;
use std::process::ExitCode;

/// Zero-trust ETL for AI/RAG — chunk documents for vector databases.
#[derive(Parser)]
#[command(
    name = "bitvanes",
    version,
    about = "Zero-trust document chunking for RAG",
    long_about = "Processes documents into BPE-aware chunks with structural context.\n\
                  Output formats: Arrow IPC (.arrow), CSV (.csv), JSON (.json).\n\
                  Supports profile replay from the web app."
)]
struct Cli {
    /// Input file or directory (recursive scan).
    #[arg(short, long, value_name = "PATH")]
    input: Option<PathBuf>,

    /// Pipeline profile JSON (exported from the web app).
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,

    /// Headless mode — no TUI, just process and output. (Default if --input is given.)
    #[arg(long)]
    no_tui: bool,

    /// Document format. Inferred from file extension if omitted.
    #[arg(short, long, value_name = "FORMAT")]
    format: Option<String>,

    /// BPE tokenizer for chunk boundary calculation.
    #[arg(short, long, value_name = "NAME")]
    tokenizer: Option<String>,

    /// Maximum tokens per chunk.
    #[arg(short = 'm', long, value_name = "N")]
    max_tokens: Option<u32>,

    /// PII patterns to scrub (comma-separated). E.g. "email,ssn,credit_card".
    #[arg(long, value_name = "PATTERNS")]
    scrub: Option<String>,

    /// Output file. Supports .arrow, .csv, .json extensions.
    /// Use "-" for stdout (JSON only).
    #[arg(short, long, value_name = "FILE")]
    output: Option<String>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    // If --input is provided, run headless. Otherwise launch the TUI.
    if cli.input.is_some() || cli.no_tui {
        match headless::run(&cli) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("Error: {e}");
                ExitCode::FAILURE
            }
        }
    } else {
        match tui::run(&cli) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("Error: {e}");
                ExitCode::FAILURE
            }
        }
    }
}
