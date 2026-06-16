//! CLI entry-point

use std::process::ExitCode;

use clap::Parser;
use indicatif_log_bridge::LogWrapper;
use pglifecycle::{build, cli, deploy, progress, project, pull, skeleton};
use simplelog::SharedLogger;

fn main() -> ExitCode {
    let args = cli::Cli::parse();
    configure_logging(&args);
    log::info!(
        "pglifecycle v{} running {}",
        env!("CARGO_PKG_VERSION"),
        args.action.name()
    );
    let result = match &args.action {
        cli::Action::Build(args) => run_build(args),
        cli::Action::Create(create) => skeleton::create(create),
        cli::Action::Deploy(args) => deploy::deploy(args),
        cli::Action::Pull(args) => pull::pull(args),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}

/// Load the project and write the archive, framing the work with an
/// up-front banner and a closing summary on stdout (matching `pull`).
/// The banner prints before the load so it precedes any warnings the
/// load emits; it names the project path rather than the project's
/// declared name, which is not known until the load completes.
fn run_build(args: &cli::Build) -> Result<(), String> {
    let started = std::time::Instant::now();
    println!(
        "pglifecycle v{} Building {} → {}",
        env!("CARGO_PKG_VERSION"),
        args.project.display(),
        args.destination.display(),
    );
    let project = project::load(&args.project)?;
    build::build(&project, &args.destination)?;
    println!(
        "\nBuilt {} in {:.2?}",
        args.destination.display(),
        started.elapsed(),
    );
    Ok(())
}

fn configure_logging(args: &cli::Cli) {
    let level = if args.debug {
        log::LevelFilter::Debug
    } else if args.verbose {
        log::LevelFilter::Info
    } else {
        log::LevelFilter::Warn
    };
    if let Some(path) = &args.log_file {
        if let Ok(handle) = std::fs::File::create(path) {
            if let Err(err) = simplelog::WriteLogger::init(
                level,
                simplelog::Config::default(),
                handle,
            ) {
                eprintln!("warning: failed to initialize file logging: {err}");
            } else {
                return;
            }
        } else {
            eprintln!("warning: failed to create log file {}", path.display());
        }
    }
    // log to stderr so command output (e.g. the deploy script on
    // stdout) stays clean
    let logger = simplelog::TermLogger::new(
        level,
        simplelog::Config::default(),
        simplelog::TerminalMode::Stderr,
        simplelog::ColorChoice::Auto,
    );
    match progress::init() {
        // on a terminal, route log records through the progress bridge
        // so they print above any live bars instead of corrupting them
        Some(multi) => {
            if let Err(err) = LogWrapper::new(multi, *logger).try_init() {
                eprintln!(
                    "warning: failed to initialize terminal logging: {err}"
                );
            } else {
                log::set_max_level(level);
            }
        }
        None => {
            if let Err(err) = log::set_boxed_logger(logger.as_log()) {
                eprintln!(
                    "warning: failed to initialize terminal logging: {err}"
                );
            } else {
                log::set_max_level(level);
            }
        }
    }
}
