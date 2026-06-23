//! Application state and key handling for the TUI.

use std::fs;
use std::path::PathBuf;

use crossterm::event::{KeyCode, KeyEvent};

use bitvanes_core::{
    BuiltInPattern, ChunkSpec, DocumentFormat, PipelineConfig, TokenizerKind,
    chunk::chunk_document, parse::parse_bytes, scrub::scrub_document,
};

/// Which screen is currently displayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    FileBrowser,
    Config,
    Results,
}

/// Actions returned by key handling.
pub enum Action {
    Quit,
    None,
}

/// Top-level TUI application state.
pub struct AppState {
    pub screen: Screen,
    pub current_dir: PathBuf,
    pub dir_entries: Vec<PathBuf>,
    pub cursor: usize,
    pub selected_files: Vec<PathBuf>,
    pub config: PipelineConfig,
    pub chunks: Vec<ChunkSpec>,
    pub error: Option<String>,
    pub scroll: usize,
    pub processing: bool,
    pub output_path: String,
}

impl AppState {
    pub fn new(cli: &crate::Cli) -> Self {
        let current_dir = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let mut state = Self {
            screen: Screen::FileBrowser,
            dir_entries: Vec::new(),
            cursor: 0,
            selected_files: Vec::new(),
            config: PipelineConfig::default(),
            chunks: Vec::new(),
            error: None,
            scroll: 0,
            processing: false,
            output_path: cli.output.clone().unwrap_or_else(|| "output.json".to_string()),
            current_dir: current_dir.clone(),
        };
        state.refresh_dir();
        state
    }

    /// Reload directory entries from `current_dir`.
    pub fn refresh_dir(&mut self) {
        let mut entries: Vec<PathBuf> = fs::read_dir(&self.current_dir)
            .map(|rd| rd.filter_map(|e| e.ok()).map(|e| e.path()).collect())
            .unwrap_or_default();

        // Sort: directories first, then alphabetical.
        entries.sort_by(|a, b| {
            let a_dir = a.is_dir();
            let b_dir = b.is_dir();
            b_dir
                .cmp(&a_dir)
                .then_with(|| a.file_name().cmp(&b.file_name()))
        });

        // Prepend parent dir entry if we're not at root.
        if self.current_dir.parent().is_some() {
            entries.insert(0, self.current_dir.join(".."));
        }

        self.dir_entries = entries;
        self.cursor = 0;
    }

    /// Returns the path currently under the cursor, if any.
    fn current_entry(&self) -> Option<&PathBuf> {
        self.dir_entries.get(self.cursor)
    }

    /// Returns true if the file at `path` is in the selected list.
    pub fn is_selected(&self, path: &PathBuf) -> bool {
        self.selected_files.contains(path)
    }

    /// Processes all selected files through the pipeline.
    pub fn process_files(&mut self) {
        self.processing = true;
        self.error = None;
        self.chunks.clear();

        for path in &self.selected_files {
            if !path.is_file() {
                continue;
            }
            let bytes = match fs::read(path) {
                Ok(b) => b,
                Err(e) => {
                    self.error = Some(format!("Could not read {}: {e}", path.display()));
                    continue;
                }
            };

            let cfg = PipelineConfig {
                source_label: Some(path.display().to_string()),
                ..self.config.clone()
            };

            let cfg = infer_format(path, cfg);

            match parse_bytes(&bytes, &cfg)
                .and_then(|doc| scrub_document(doc, &cfg.scrub).map(|(d, _)| d))
                .and_then(|doc| chunk_document(&doc, &cfg.chunk, cfg.source_label.as_deref()))
            {
                Ok(chunks) => self.chunks.extend(chunks),
                Err(e) => {
                    self.error = Some(format!("Failed {}: {e}", path.display()));
                }
            }
        }

        // Renumber chunk indices sequentially.
        for (i, c) in self.chunks.iter_mut().enumerate() {
            c.chunk_index = i as u32;
        }

        self.processing = false;
        self.screen = Screen::Results;
        self.scroll = 0;
    }

    /// Handle a key press. Returns an action for the main loop.
    pub fn handle_key(&mut self, key: KeyEvent) -> Action {
        match self.screen {
            Screen::FileBrowser => self.handle_browser_key(key),
            Screen::Config => self.handle_config_key(key),
            Screen::Results => self.handle_results_key(key),
        }
    }

    fn handle_browser_key(&mut self, key: KeyEvent) -> Action {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return Action::Quit,
            KeyCode::Up | KeyCode::Char('k') => {
                self.cursor = self.cursor.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.cursor = (self.cursor + 1).min(self.dir_entries.len().saturating_sub(1));
            }
            KeyCode::Enter => {
                if let Some(path) = self.current_entry() {
                    if path.is_dir() || path.ends_with("..") {
                        // Navigate into directory.
                        if path.ends_with("..") {
                            if let Some(parent) = self.current_dir.parent() {
                                self.current_dir = parent.to_path_buf();
                            }
                        } else {
                            self.current_dir = path.clone();
                        }
                        self.refresh_dir();
                    } else if path.is_file() {
                        // Toggle selection.
                        if let Some(pos) = self.selected_files.iter().position(|p| p == path) {
                            self.selected_files.remove(pos);
                        } else {
                            self.selected_files.push(path.clone());
                        }
                    }
                }
            }
            KeyCode::Char(' ') => {
                if let Some(path) = self.current_entry() {
                    if path.is_file() {
                        if let Some(pos) = self.selected_files.iter().position(|p| p == path) {
                            self.selected_files.remove(pos);
                        } else {
                            self.selected_files.push(path.clone());
                        }
                    }
                }
            }
            KeyCode::Tab | KeyCode::Char('c') => {
                self.screen = Screen::Config;
            }
            KeyCode::Char('p') if !self.selected_files.is_empty() => {
                self.screen = Screen::Config;
            }
            _ => {}
        }
        Action::None
    }

    fn handle_config_key(&mut self, key: KeyEvent) -> Action {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return Action::Quit,
            KeyCode::BackTab | KeyCode::Char('b') => {
                self.screen = Screen::FileBrowser;
            }
            KeyCode::Char('m') => {
                // Cycle max_tokens: 128 → 256 → 512 → 1024 → 128
                let mt = self.config.chunk.max_tokens;
                self.config.chunk.max_tokens = match mt {
                    0..=128 => 256,
                    129..=256 => 512,
                    257..=512 => 1024,
                    _ => 128,
                };
            }
            KeyCode::Char('t') => {
                // Cycle tokenizer.
                self.config.chunk.tokenizer = match self.config.chunk.tokenizer {
                    TokenizerKind::Cl100kBase => TokenizerKind::O200kBase,
                    TokenizerKind::O200kBase => TokenizerKind::R50kBase,
                    TokenizerKind::R50kBase => TokenizerKind::P50kBase,
                    TokenizerKind::P50kBase => TokenizerKind::P50kEdit,
                    TokenizerKind::P50kEdit => TokenizerKind::O200kHarmony,
                    TokenizerKind::O200kHarmony => TokenizerKind::Cl100kBase,
                };
            }
            KeyCode::Char('e') => {
                // Toggle email scrubbing.
                toggle_pattern(&mut self.config.scrub.patterns, BuiltInPattern::Email);
            }
            KeyCode::Char('s') => {
                toggle_pattern(&mut self.config.scrub.patterns, BuiltInPattern::Ssn);
            }
            KeyCode::Char('a') => {
                toggle_pattern(&mut self.config.scrub.patterns, BuiltInPattern::AwsKey);
            }
            KeyCode::Enter if !self.selected_files.is_empty() => {
                self.process_files();
            }
            _ => {}
        }
        Action::None
    }

    fn handle_results_key(&mut self, key: KeyEvent) -> Action {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return Action::Quit,
            KeyCode::Up | KeyCode::Char('k') => {
                self.scroll = self.scroll.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.scroll = (self.scroll + 1).min(self.chunks.len().saturating_sub(1));
            }
            KeyCode::Char('b') => {
                self.screen = Screen::FileBrowser;
            }
            KeyCode::Char('c') => {
                self.screen = Screen::Config;
            }
            KeyCode::Char('s') => {
                match crate::headless::write_output(&self.chunks, &self.output_path) {
                    Ok(()) => self.error = Some(format!("Saved to {}", self.output_path)),
                    Err(e) => self.error = Some(format!("Failed to save: {}", e)),
                }
            }
            _ => {}
        }
        Action::None
    }
}

fn toggle_pattern(patterns: &mut Vec<BuiltInPattern>, pat: BuiltInPattern) {
    if let Some(pos) = patterns.iter().position(|&p| p == pat) {
        patterns.remove(pos);
    } else {
        patterns.push(pat);
    }
}

fn infer_format(path: &std::path::Path, mut cfg: PipelineConfig) -> PipelineConfig {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_lowercase());
    match ext.as_deref() {
        Some("md") | Some("markdown") => cfg.format = DocumentFormat::Markdown,
        Some("txt") => cfg.format = DocumentFormat::Text,
        Some("html") | Some("htm") => cfg.format = DocumentFormat::Html,
        _ => {}
    }
    cfg
}
