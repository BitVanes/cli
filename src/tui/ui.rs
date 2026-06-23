//! Ratatui rendering for each screen.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, List, ListItem, Paragraph, Row, Table};

use bitvanes_core::BuiltInPattern;

use super::app::{AppState, Screen};

/// Main draw entry point — dispatches to the current screen.
pub fn draw(f: &mut Frame, app: &AppState) {
    let area = f.area();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // title bar
            Constraint::Min(1),    // content
            Constraint::Length(1), // status bar
        ])
        .split(area);

    draw_title_bar(f, chunks[0]);
    match app.screen {
        Screen::FileBrowser => draw_file_browser(f, chunks[1], app),
        Screen::Config => draw_config(f, chunks[1], app),
        Screen::Results => draw_results(f, chunks[1], app),
    }
    draw_status_bar(f, chunks[2], app);
}

// -----------------------------------------------------------------------
// Title + Status bars
// -----------------------------------------------------------------------

fn draw_title_bar(f: &mut Frame, area: Rect) {
    let title = Paragraph::new(Line::from(vec![
        Span::styled(
            " BitVanes ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("│ "),
        Span::styled(
            "Zero-Trust ETL for RAG",
            Style::default().fg(Color::DarkGray),
        ),
    ]))
    .block(Block::default().borders(Borders::TOP));
    f.render_widget(title, area);
}

fn draw_status_bar(f: &mut Frame, area: Rect, app: &AppState) {
    let hints = match app.screen {
        Screen::FileBrowser => {
            "↑↓ navigate · Enter open/select · Space select · Tab config · q quit"
        }
        Screen::Config => {
            "m max-tokens · t tokenizer · e email · s ssn · a aws · Enter process · b back · q quit"
        }
        Screen::Results => "↑↓ scroll · b browser · c config · s save · q quit",
    };

    let bar = Paragraph::new(format!(" {hints}")).style(Style::default().fg(Color::DarkGray));
    f.render_widget(bar, area);
}

// -----------------------------------------------------------------------
// File Browser
// -----------------------------------------------------------------------

fn draw_file_browser(f: &mut Frame, area: Rect, app: &AppState) {
    let entries_area = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(1)])
        .split(area);

    // Path header.
    let path_text = format!(" 📁 {}", app.current_dir.display());
    let header = Paragraph::new(path_text).style(Style::default().fg(Color::Yellow));
    f.render_widget(header, entries_area[0]);

    // File list.
    let items: Vec<ListItem> = app
        .dir_entries
        .iter()
        .enumerate()
        .map(|(i, path)| {
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("?");

            let is_dir = path.is_dir() || name == "..";
            let is_selected = app.is_selected(path);

            let prefix = if is_selected { "✓ " } else { "  " };
            let icon = if name == ".." {
                "↑ .."
            } else if is_dir {
                "📁"
            } else {
                "📄"
            };

            let label = format!("{prefix}{icon} {name}");

            let style = if i == app.cursor {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else if is_dir {
                Style::default().fg(Color::Blue)
            } else if is_selected {
                Style::default().fg(Color::Green)
            } else {
                Style::default()
            };

            ListItem::new(Line::from(Span::styled(label, style)))
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(" Files ({} selected) ", app.selected_files.len())),
    );
    f.render_widget(list, entries_area[1]);
}

// -----------------------------------------------------------------------
// Config Editor
// -----------------------------------------------------------------------

fn draw_config(f: &mut Frame, area: Rect, app: &AppState) {
    let lines = vec![
        Line::from(""),
        styled_line("Pipeline Configuration", Color::Yellow, true),
        Line::from(""),
        kv_line("Format", format!("{:?}", app.config.format)),
        kv_line("Tokenizer", format!("{:?}", app.config.chunk.tokenizer)),
        kv_line("Max Tokens", app.config.chunk.max_tokens.to_string()),
        Line::from(""),
        styled_line("PII Scrubbing", Color::Yellow, true),
        pattern_line(
            "Email",
            app.config.scrub.patterns.contains(&BuiltInPattern::Email),
        ),
        pattern_line(
            "SSN",
            app.config.scrub.patterns.contains(&BuiltInPattern::Ssn),
        ),
        pattern_line(
            "AWS Key",
            app.config.scrub.patterns.contains(&BuiltInPattern::AwsKey),
        ),
        Line::from(""),
        styled_line("Selected Files", Color::Yellow, true),
        Line::from(format!(
            "  {} files ready to process",
            app.selected_files.len()
        )),
        Line::from(""),
        if app.selected_files.is_empty() {
            Line::from(Span::styled(
                "  ⚠ No files selected — go back with 'b' and press Space to select files",
                Style::default().fg(Color::Red),
            ))
        } else {
            Line::from(Span::styled(
                "  ► Press Enter to process ◄",
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ))
        },
    ];

    let para = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Configuration "),
    );
    f.render_widget(para, area);
}

// -----------------------------------------------------------------------
// Results
// -----------------------------------------------------------------------

fn draw_results(f: &mut Frame, area: Rect, app: &AppState) {
    let stats_area = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(1)])
        .split(area);

    // Stats bar.
    let total_tokens: u64 = app.chunks.iter().map(|c| c.token_count as u64).sum();
    let avg = if app.chunks.is_empty() {
        0
    } else {
        total_tokens / app.chunks.len() as u64
    };
    let stats = format!(
        " {} chunks · {} tokens · {} avg tokens/chunk ",
        app.chunks.len(),
        total_tokens,
        avg,
    );
    let stats_widget = Paragraph::new(stats).style(
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    );
    f.render_widget(stats_widget, stats_area[0]);

    // Chunk table.
    let rows: Vec<Row> = app
        .chunks
        .iter()
        .skip(app.scroll)
        .take(area.height.saturating_sub(5) as usize)
        .map(|c| {
            let text = if c.text.len() > 60 {
                format!("{}…", &c.text[..60])
            } else {
                c.text.clone()
            };
            let heading = if c.heading_path.is_empty() {
                "—".to_string()
            } else {
                c.heading_path.join(" › ")
            };
            Row::new(vec![
                Cell::from(c.chunk_index.to_string()),
                Cell::from(text),
                Cell::from(c.token_count.to_string()),
                Cell::from(heading),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(5),
            Constraint::Min(20),
            Constraint::Length(8),
            Constraint::Length(30),
        ],
    )
    .header(
        Row::new(vec![
            Cell::from("#"),
            Cell::from("Text"),
            Cell::from("Tokens"),
            Cell::from("Heading"),
        ])
        .style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(Block::default().borders(Borders::ALL).title(format!(
        " Chunks (showing {}/{}) ",
        app.scroll.min(app.chunks.len()),
        app.chunks.len()
    )));

    f.render_widget(table, stats_area[1]);

    // Show error if any.
    if let Some(e) = &app.error {
        let err = Paragraph::new(format!(" ⚠ {e}")).style(Style::default().fg(Color::Red));
        f.render_widget(err, stats_area[1]);
    }
}

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

fn styled_line(text: &str, color: Color, bold: bool) -> Line<'_> {
    let mut style = Style::default().fg(color);
    if bold {
        style = style.add_modifier(Modifier::BOLD);
    }
    Line::from(Span::styled(format!(" {text}"), style))
}

fn kv_line(key: &str, value: String) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!("  {key:<16} "),
            Style::default().fg(Color::DarkGray),
        ),
        Span::raw(value),
    ])
}

fn pattern_line(name: &str, enabled: bool) -> Line<'_> {
    let check = if enabled { "✓" } else { "✗" };
    let color = if enabled {
        Color::Green
    } else {
        Color::DarkGray
    };
    Line::from(Span::styled(
        format!("  [{check}] {name}"),
        Style::default().fg(color),
    ))
}
