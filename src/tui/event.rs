//! Terminal event polling.

use std::io;
use std::time::Duration;

use crossterm::event::{self, Event, KeyEvent};

/// Polls for a key event with a 250ms timeout.
pub fn poll_key() -> io::Result<KeyEvent> {
    loop {
        if event::poll(Duration::from_millis(250))? {
            if let Event::Key(key) = event::read()? {
                return Ok(key);
            }
        }
    }
}
