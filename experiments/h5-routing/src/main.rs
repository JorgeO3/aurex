use std::sync::Arc;
use std::time::Instant;

use aurum_routing::{
    BindingDecl, ExchangeDecl, RouteCompiler, RoutingConfig,
};
use aurum_types::{ExchangeId, QueueId, RouteTableVersion};

fn build_table(keys: usize) -> Arc<aurum_routing::RouteTable> {
    let mut config = RoutingConfig::new(RouteTableVersion::INITIAL);
    config.add_exchange(ExchangeDecl::direct(ExchangeId(1), "bench"));
    for i in 0..keys {
        let key = format!("key-{i}");
        config.add_binding(BindingDecl::direct(
            ExchangeId(1),
            QueueId(i as u32),
            key,
        ));
    }
    Arc::new(RouteCompiler::compile(&config).unwrap())
}

fn main() {
    let keys = 4096usize;
    let iterations = 1_000_000u64;
    let table = build_table(keys);

    // Warm resolve path
    let resolved = table.resolve_direct(ExchangeId(1), b"key-2048").unwrap();

    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("key-{}", (i as usize) % keys);
        let _ = table.resolve_direct(ExchangeId(1), key.as_bytes()).unwrap();
    }
    let resolve_ns = start.elapsed().as_nanos() as f64 / iterations as f64;

    let start = Instant::now();
    for _ in 0..iterations {
        let _ = table
            .get_by_route_id(resolved.route_id, resolved.version)
            .unwrap();
    }
    let route_id_ns = start.elapsed().as_nanos() as f64 / iterations as f64;

    println!("AurumMQ H5 — Compiled Routing");
    println!("keys={keys} iterations={iterations}");
    println!("resolve_direct ns/op={resolve_ns:.2}");
    println!("route_id lookup ns/op={route_id_ns:.2}");
    println!("ratio resolve/route_id={:.2}x", resolve_ns / route_id_ns);
}
