//! TUI module: interactive terminal ETL designer using ratatui.

pub mod app;
pub mod event;
pub mod ui;

use std::io;
use std::time::Duration;

use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

use crate::Cli;

/// Runs the interactive TUI. Sets up the terminal, installs a panic hook
/// so the terminal is always restored, runs the app loop, and restores the
/// terminal on exit.
pub fn run(cli: &Cli) -> io::Result<()> {
    install_panic_hook();
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_app(&mut terminal, cli);

    // Always restore the terminal, even on error.
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

/// Installs a panic hook that restores the terminal before the default
/// panic handler runs, so a panic inside the TUI never leaves the user's
/// shell stuck in raw/alternate-screen mode.
fn install_panic_hook() {
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        prev(info);
    }));
}

fn run_app(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>, cli: &Cli) -> io::Result<()> {
    let mut app = app::AppState::new(cli);

    loop {
        terminal.draw(|f| ui::draw(f, &app))?;
        // Absorb any result that arrived from the background processor.
        app.pump();

        if let Some(key) = event::poll_key(Duration::from_millis(120))? {
            match app.handle_key(key) {
                app::Action::Quit => return Ok(()),
                app::Action::None => {}
            }
        }
    }
}
