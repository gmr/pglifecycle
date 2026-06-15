//! Failure diagnostics for `pull`: a correlatable error report for DDL
//! that fails to parse or format, plus capture of the statement in
//! flight when the process is interrupted.
//!
//! The motivating case is an upstream formatter (libpgfmt) that loops
//! forever on a pathological view: progress shows which object it
//! stalled on, but reproducing the bug needs the exact SQL. [`enter`]
//! records the statement about to be handed to the parser/formatter,
//! and the Ctrl-C handler — which `ctrlc` runs on its own thread, so it
//! fires even while the main thread is wedged — writes that SQL to the
//! report before exiting. Parse/format errors are recorded the same
//! way, so a single file correlates every failure with its DDL.
//!
//! When [`init`] has not been called (e.g. outside `pull`), every entry
//! point is a no-op.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};

struct State {
    path: PathBuf,
    /// The statement currently being parsed/formatted, captured on
    /// interrupt; `None` between operations.
    current: Mutex<Option<InFlight>>,
    /// Whether the report file has been opened this run; the first
    /// write truncates any stale file, later writes append.
    opened: AtomicBool,
}

#[derive(Clone)]
struct InFlight {
    label: String,
    sql: String,
}

static STATE: OnceLock<State> = OnceLock::new();

/// Initialize the error report at `path` and install the interrupt
/// handler that records the in-flight statement before exiting. Safe to
/// call once per process; later calls are ignored.
pub fn init(path: PathBuf) {
    if STATE
        .set(State {
            path,
            current: Mutex::new(None),
            opened: AtomicBool::new(false),
        })
        .is_err()
    {
        return;
    }
    let _ = ctrlc::set_handler(|| {
        if let Some(state) = STATE.get() {
            report_interrupt(state);
        }
        std::process::exit(130);
    });
}

/// Record the statement in flight (if any) to the report; called from
/// the interrupt handler before exiting.
fn report_interrupt(state: &State) {
    let Some(in_flight) = state.current.lock().ok().and_then(|g| g.clone())
    else {
        return;
    };
    write(
        state,
        &format!("INTERRUPTED while processing {}", in_flight.label),
        "SIGINT received while this statement was in flight (a likely \
         parser/formatter hang)",
        &in_flight.sql,
    );
    eprintln!(
        "\nInterrupted while processing {}; its DDL was written to {}",
        in_flight.label,
        state.path.display()
    );
}

/// Mark `sql` (labelled e.g. `view public.foo`) as the statement now in
/// flight, so an interrupt during it captures the offending DDL. Pair
/// every call with [`leave`].
pub fn enter(label: impl Into<String>, sql: &str) {
    if let Some(state) = STATE.get()
        && let Ok(mut guard) = state.current.lock()
    {
        *guard = Some(InFlight {
            label: label.into(),
            sql: sql.to_string(),
        });
    }
}

/// Clear the in-flight statement after it completed.
pub fn leave() {
    if let Some(state) = STATE.get()
        && let Ok(mut guard) = state.current.lock()
    {
        *guard = None;
    }
}

/// Record a parse/format failure and its DDL to the report.
pub fn record_failure(category: &str, label: &str, error: &str, sql: &str) {
    if let Some(state) = STATE.get() {
        write(state, &format!("{category}: {label}"), error, sql);
    }
}

/// Append a delimited record; the first write of the run truncates a
/// stale report and writes a header.
fn write(state: &State, heading: &str, detail: &str, sql: &str) {
    let fresh = !state.opened.swap(true, Ordering::SeqCst);
    let file = OpenOptions::new()
        .create(true)
        .append(!fresh)
        .write(true)
        .truncate(fresh)
        .open(&state.path);
    let Ok(mut file) = file else {
        log::warn!(
            "failed to write the error report at {}",
            state.path.display()
        );
        return;
    };
    let header = if fresh {
        "# pglifecycle error report\n# Each record below is a failure and \
         the DDL that produced it.\n\n"
    } else {
        ""
    };
    let _ = write!(
        file,
        "{header}=== {heading} ===\n{detail}\n--- DDL ---\n{}\n--- END ---\n\n",
        sql.trim_end()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_failure_is_noop_without_init() {
        // STATE is never set in this test process path; the call must
        // simply do nothing rather than panic.
        record_failure("FAILED TO PARSE", "VIEW x.y", "boom", "CREATE ...");
        enter("view x.y", "CREATE VIEW ...");
        leave();
    }

    #[test]
    fn write_truncates_then_appends() {
        let dir = tempfile::tempdir().unwrap();
        let state = State {
            path: dir.path().join("errors.log"),
            current: Mutex::new(None),
            opened: AtomicBool::new(false),
        };
        write(
            &state,
            "FAILED TO PARSE: VIEW a.b",
            "syntax error",
            "CREATE A",
        );
        write(&state, "FAILED TO FORMAT: VIEW c.d", "loop", "CREATE C");
        let body = std::fs::read_to_string(&state.path).unwrap();
        assert!(body.starts_with("# pglifecycle error report"));
        assert!(body.contains(
            "=== FAILED TO PARSE: VIEW a.b ===\nsyntax \
                                error\n--- DDL ---\nCREATE A\n--- END ---"
        ));
        assert!(body.contains("=== FAILED TO FORMAT: VIEW c.d ==="));
        // the header appears exactly once (only the first write is fresh)
        assert_eq!(body.matches("# pglifecycle error report").count(), 1);
    }

    #[test]
    fn interrupt_records_the_in_flight_statement() {
        let dir = tempfile::tempdir().unwrap();
        let state = State {
            path: dir.path().join("errors.log"),
            current: Mutex::new(Some(InFlight {
                label: "view report.weekly_production_trial_accounts".into(),
                sql: "CREATE VIEW report.weekly_production_trial_accounts \
                      AS SELECT 1;"
                    .into(),
            })),
            opened: AtomicBool::new(false),
        };
        report_interrupt(&state);
        let body = std::fs::read_to_string(&state.path).unwrap();
        assert!(body.contains(
            "=== INTERRUPTED while processing view \
             report.weekly_production_trial_accounts ==="
        ));
        assert!(body.contains(
            "CREATE VIEW report.weekly_production_trial_accounts AS SELECT 1;"
        ));
    }

    #[test]
    fn interrupt_with_nothing_in_flight_writes_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let state = State {
            path: dir.path().join("errors.log"),
            current: Mutex::new(None),
            opened: AtomicBool::new(false),
        };
        report_interrupt(&state);
        assert!(!state.path.exists());
    }
}
