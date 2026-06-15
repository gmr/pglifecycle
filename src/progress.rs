//! Live progress reporting for long-running commands (currently
//! `pull`). Bars render on stderr only when it is a terminal, so piped
//! or redirected output stays clean; otherwise every call is a no-op
//! and the log output is the only signal.

use std::io::IsTerminal;
use std::sync::OnceLock;
use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

static MULTI: OnceLock<Option<MultiProgress>> = OnceLock::new();

/// Initialize progress rendering, returning the [`MultiProgress`] to
/// hand to the log bridge when stderr is a terminal (so log lines print
/// above the bars instead of corrupting them). Call once, from `main`;
/// a no-op result means progress is disabled.
pub fn init() -> Option<MultiProgress> {
    let multi = std::io::stderr().is_terminal().then(MultiProgress::new);
    let _ = MULTI.set(multi.clone());
    multi
}

fn multi() -> Option<&'static MultiProgress> {
    MULTI.get().and_then(Option::as_ref)
}

/// A spinner for a phase of unknown duration; ticks on its own until
/// finished or dropped.
pub fn spinner(message: impl Into<String>) -> Task {
    let Some(multi) = multi() else {
        return Task(None);
    };
    let bar = multi.add(ProgressBar::new_spinner());
    bar.set_style(
        ProgressStyle::with_template("{spinner} {msg} ({elapsed})").unwrap(),
    );
    bar.set_message(message.into());
    bar.enable_steady_tick(Duration::from_millis(100));
    Task(Some(bar))
}

/// A bar tracking `len` items; advance it with [`Task::inc`] and label
/// the item in flight with [`Task::set_message`] so a stall is
/// attributable to a specific object.
pub fn bar(len: u64, message: impl Into<String>) -> Task {
    let Some(multi) = multi() else {
        return Task(None);
    };
    let bar = multi.add(ProgressBar::new(len));
    bar.set_style(
        ProgressStyle::with_template(
            "{msg} [{bar:30}] {pos}/{len} ({elapsed})",
        )
        .unwrap()
        .progress_chars("=>-"),
    );
    bar.set_message(message.into());
    Task(Some(bar))
}

/// Handle to an in-flight bar or spinner; every method is a no-op when
/// progress rendering is disabled.
pub struct Task(Option<ProgressBar>);

impl Task {
    /// Advance a bar by one item.
    pub fn inc(&self) {
        if let Some(bar) = &self.0 {
            bar.inc(1);
        }
    }

    /// Label the item currently being processed.
    pub fn set_message(&self, message: impl Into<String>) {
        if let Some(bar) = &self.0 {
            bar.set_message(message.into());
        }
    }

    /// Clear the bar; the phase's own log line is the lasting record.
    pub fn finish(self) {
        if let Some(bar) = &self.0 {
            bar.finish_and_clear();
        }
    }
}
