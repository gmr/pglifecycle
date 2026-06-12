//! CLI entry-point

use std::process::ExitCode;

use clap::Parser;
use pglifecycle::{build, cli, project, pull, skeleton};

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
    if let Err(err) = simplelog::TermLogger::init(
        level,
        simplelog::Config::default(),
        simplelog::TerminalMode::Stdout,
        simplelog::ColorChoice::Auto,
    ) {
        eprintln!("warning: failed to initialize terminal logging: {err}");
    }
}
