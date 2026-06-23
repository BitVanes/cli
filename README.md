# bitvanes-cli

Terminal ETL tool for [BitVanes](https://bitvanes.com) — zero-trust document
chunking for RAG. Runs the same pipeline as the web app, natively. Features
both an interactive TUI (ratatui) and headless mode (CI/CD piping).

## Install

### From source

```bash
cargo install --git https://github.com/BitVanes/cli.git
```

### From release

Download the latest binary from
[GitHub Releases](https://github.com/BitVanes/cli/releases):

```bash
# Linux/macOS
curl -L https://github.com/BitVanes/cli/releases/latest/download/bitvanes-v0.1.0-x86_64-linux.tar.gz | tar xz
sudo mv bitvanes /usr/local/bin/

# Verify
bitvanes --version
```

## Usage

### Process a file or directory

```bash
# Single file → JSON output
bitvanes -i document.md -o chunks.json

# Directory (recursive) → Arrow IPC
bitvanes -i ./docs/ -f markdown -m 512 -o chunks.arrow

# Multiple PII patterns
bitvanes -i ./docs/ --scrub email,ssn,credit_card -o chunks.csv
```

### Interactive TUI mode

Run `bitvanes` with no arguments to launch the terminal UI:

```
bitvanes
```

Three screens:
1. **File Browser** — navigate directories, select files with Space
2. **Config** — adjust format, tokenizer, max tokens, PII patterns
3. **Results** — scrollable chunk preview table with stats

Keybinds:
- `↑↓` / `jk` — navigate
- `Enter` — open directory / toggle selection / process
- `Space` — select file
- `Tab` — switch between browser and config
- `m` — cycle max tokens (128 → 256 → 512 → 1024)
- `t` — cycle tokenizer
- `e/s/a` — toggle email/ssn/aws PII scrubbing
- `b` — back to file browser
- `q` — quit

### Profile replay (from web app)

Export a profile from the BitVanes web app, then replay it identically:

```bash
bitvanes -c profile.json -i ./documents/ -o output.arrow
```

### Unix piping

```bash
cat doc.md | bitvanes -f markdown -m 256 -o - | python process.py
```

### All options

```
bitvanes [OPTIONS]

Input:
  -i, --input <PATH>         File or directory (recursive scan)
  -c, --config <FILE>        Profile JSON from web export
      --no-tui               Headless mode

Pipeline:
  -f, --format <FORMAT>      markdown | text | html | pdf
  -t, --tokenizer <NAME>     cl100k_base | o200k_base | r50k_base | ...
  -m, --max-tokens <N>       Max tokens per chunk
      --scrub <PATTERNS>     Comma-separated PII patterns

Output:
  -o, --output <FILE>        .arrow | .csv | .json | - (stdout)
```

## Output formats

| Format | Extension | Use case |
|--------|-----------|----------|
| Arrow IPC | `.arrow` | DuckDB / Polars / LanceDB direct ingestion |
| CSV | `.csv` | Spreadsheet import, human inspection |
| JSON | `.json` | Python / Node RAG pipelines |

## Supported document formats

| Format | Extensions | Parser |
|--------|------------|--------|
| Markdown | `.md` | `pulldown-cmark` |
| Text | `.txt` | Paragraph-based splitting |
| HTML | `.html` | `scraper` (html5ever) |
| PDF | `.pdf` | `pdf-extract` (native, with `cli-pdf` feature) |

Format is auto-detected from file extension. Override with `--format`.

## Features

- **6 OpenAI tokenizers**: cl100k_base, o200k_base, r50k_base, p50k_base,
  p50k_edit, o200k_harmony (all embedded at compile time)
- **7 PII patterns**: email, SSN, phone, credit card (Luhn), AWS keys,
  GitHub PATs, JWTs
- **Structural context**: heading ancestry preserved per chunk
- **Parallel processing**: rayon multi-core for directory batch processing
- **Profile replay**: byte-for-byte identical output to the web app

## Build from source

```bash
git clone https://github.com/BitVanes/cli.git
cd cli
cargo build --release
./target/release/bitvanes --help
```

The CLI depends on [`bitvanes-core`](https://github.com/BitVanes/core) via a
git dependency (tag `v0.1.0`). No manual checkout of the core repo needed.

## License

MIT OR Apache-2.0
