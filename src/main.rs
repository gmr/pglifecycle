//! CLI entry-point

mod cli;
mod constants;
mod skeleton;

use std::process::ExitCode;

use clap::Parser;

fn main() -> ExitCode {
    let args = cli::Cli::parse();
    configure_logging(&args);
    log::info!(
        "pglifecycle v{} running {}",
        env!("CARGO_PKG_VERSION"),
        args.action.name()
    );
    let result = match &args.action {
        cli::Action::Build(_) => {
            Err("build is not implemented yet".to_string())
        }
        cli::Action::Create(create) => skeleton::create(create),
        cli::Action::Pull(_) => Err("pull is not implemented yet".to_string()),
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
    let config = simplelog::Config::default();
    if let Some(path) = &args.log_file {
        if let Ok(handle) = std::fs::File::create(path) {
            if let Err(err) =
                simplelog::WriteLogger::init(level, config, handle)
            {
                eprintln!("warning: failed to initialize file logging: {err}");
            }
            return;
        }
        eprintln!("warning: failed to create log file {}", path.display());
    }
    if let Err(err) = simplelog::TermLogger::init(
        level,
        config,
        simplelog::TerminalMode::Stdout,
        simplelog::ColorChoice::Auto,
    ) {
        eprintln!("warning: failed to initialize terminal logging: {err}");
    }
}
