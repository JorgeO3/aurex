use std::net::SocketAddr;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use aurum_broker::{BrokerServer, SingleNodeBrokerConfig};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        print_usage();
        return;
    }

    match args[1].as_str() {
        "status" => run_status(),
        "broker" => run_broker(&args[2..]),
        _ => print_usage(),
    }
}

fn run_status() {
    use aurum_broker::BrokerPrototype;
    let broker = BrokerPrototype::single_queue(1024);
    println!("AurumMQ prototype: healthy");
    println!("route_table_version={:?}", broker.route_table.version());
    println!("queue_messages={}", broker.queue.len());
}

fn run_broker(args: &[String]) {
    if args.is_empty() {
        eprintln!("usage: aurum broker <dev|start|check-config> ...");
        std::process::exit(1);
    }
    match args[0].as_str() {
        "dev" => run_broker_dev(args.get(1..).unwrap_or(&[])),
        "start" => run_broker_start(args.get(1..).unwrap_or(&[])),
        "check-config" => run_check_config(args.get(1..).unwrap_or(&[])),
        other => {
            eprintln!("unknown broker subcommand: {other}");
            std::process::exit(1);
        }
    }
}

fn run_broker_dev(args: &[String]) {
    let mut config = SingleNodeBrokerConfig::dev_defaults();
    let mut native = config.listeners.native.take().unwrap();
    let mut amqp = config.listeners.amqp.take().unwrap();

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--native" => {
                i += 1;
                native.bind = parse_addr(args.get(i), "--native");
                native.enabled = true;
            }
            "--amqp" => {
                i += 1;
                amqp.bind = parse_addr(args.get(i), "--amqp");
                amqp.enabled = true;
            }
            other => {
                eprintln!("unknown flag: {other}");
                std::process::exit(1);
            }
        }
        i += 1;
    }

    config.listeners.native = Some(native);
    config.listeners.amqp = Some(amqp);
    start_and_block(config);
}

fn run_broker_start(args: &[String]) {
    let config_path = parse_config_path(args).unwrap_or_else(|| {
        eprintln!("usage: aurum broker start --config <file>");
        std::process::exit(1);
    });
    let config = SingleNodeBrokerConfig::from_toml_file(&config_path).unwrap_or_else(|e| {
        eprintln!("failed to load config: {e}");
        std::process::exit(1);
    });
    start_and_block(config);
}

fn run_check_config(args: &[String]) {
    let config_path = parse_config_path(args).unwrap_or_else(|| {
        eprintln!("usage: aurum broker check-config --config <file>");
        std::process::exit(1);
    });
    let config = SingleNodeBrokerConfig::from_toml_file(&config_path).unwrap_or_else(|e| {
        eprintln!("config invalid: {e}");
        std::process::exit(1);
    });
    println!("config ok");
    if let Some(native) = &config.listeners.native {
        println!("native listener: enabled={} bind={}", native.enabled, native.bind);
    }
    if let Some(amqp) = &config.listeners.amqp {
        println!("amqp listener: enabled={} bind={}", amqp.enabled, amqp.bind);
    }
    println!("mode={:?}", config.mode);
    println!("storage={:?}", config.storage.backend);
    println!("exchanges={}", config.routing.exchanges.len());
    println!("queues={}", config.routing.queues.len());
    println!("bindings={}", config.routing.bindings.len());
}

fn start_and_block(config: SingleNodeBrokerConfig) {
    let native = config.listeners.native.as_ref().filter(|l| l.enabled);
    let amqp = config.listeners.amqp.as_ref().filter(|l| l.enabled);
    if let Some(n) = native {
        println!("native listener on {}", n.bind);
    }
    if let Some(a) = amqp {
        println!("amqp listener on {}", a.bind);
    }

    let server = BrokerServer::start(config).unwrap_or_else(|e| {
        eprintln!("broker start failed: {e}");
        std::process::exit(1);
    });
    let health = server
        .service()
        .shared_broker()
        .lock()
        .expect("broker lock")
        .health();
    println!("broker running (state={:?})", health.state);

    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}

fn parse_config_path(args: &[String]) -> Option<PathBuf> {
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--config" {
            return args.get(i + 1).map(PathBuf::from);
        }
        i += 1;
    }
    None
}

fn parse_addr(value: Option<&String>, flag: &str) -> SocketAddr {
    value
        .map(|s| s.parse())
        .transpose()
        .unwrap_or_else(|e| {
            eprintln!("invalid address for {flag}: {e}");
            std::process::exit(1);
        })
        .unwrap_or_else(|| {
            eprintln!("missing address for {flag}");
            std::process::exit(1);
        })
}

fn print_usage() {
    println!("AurumMQ CLI");
    println!("usage:");
    println!("  aurum status");
    println!("  aurum broker dev [--native ADDR] [--amqp ADDR]");
    println!("  aurum broker start --config <file>");
    println!("  aurum broker check-config --config <file>");
}
