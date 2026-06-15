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
        cli::Action::Build(args) => project::load(&args.project)
            .and_then(|p| build::build(&p, &args.destination)),
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
