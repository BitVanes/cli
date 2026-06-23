//! TUI module: interactive terminal ETL designer using ratatui.

pub mod app;
pub mod event;
pub mod ui;

use std::io;

use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

use crate::Cli;

/// Runs the interactive TUI. Sets up the terminal, runs the app loop,
/// and restores the terminal on exit.
pub fn run(_cli: &Cli) -> io::Result<()> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Run app
    let result = run_app(&mut terminal, _cli);

    // Restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

fn run_app(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>, cli: &Cli) -> io::Result<()> {
    let mut app = app::AppState::new(cli);

    loop {
        terminal.draw(|f| ui::draw(f, &app))?;

        let key = event::poll_key()?;
        match app.handle_key(key) {
            app::Action::Quit => return Ok(()),
            app::Action::None => {}
        }
    }
}
