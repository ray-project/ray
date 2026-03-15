mod scenarios;
mod stats;

use clap::Parser;
use tonic::transport::Channel;

#[derive(Parser)]
#[command(name = "ray-gcs-bench")]
#[command(about = "Benchmark tool for Ray GCS server")]
struct Args {
    /// GCS server address (host:port)
    #[arg(long, default_value = "http://127.0.0.1:6379")]
    target: String,

    /// Benchmark scenario to run
    #[arg(long, value_parser = ["kv-throughput", "actor-lookup", "node-info", "job-lifecycle", "mixed", "all"])]
    scenario: String,

    /// Number of concurrent clients
    #[arg(long, default_value = "10")]
    clients: usize,

    /// Number of requests per client (for non-duration scenarios)
    #[arg(long, default_value = "1000")]
    requests: usize,

    /// Duration in seconds (for duration-based scenarios like mixed)
    #[arg(long, default_value = "30")]
    duration: u64,

    /// Optional second target for comparison mode
    #[arg(long)]
    compare: Option<String>,

    /// Client counts to sweep when running --compare
    #[arg(long, value_delimiter = ',', default_value = "1,10,50,100")]
    sweep: Vec<usize>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if let Some(ref compare_target) = args.compare {
        // Comparison mode: run all scenarios against both targets
        println!("=== GCS Benchmark Comparison ===");
        println!("  Target A: {}", args.target);
        println!("  Target B: {compare_target}");
        println!();

        let scenarios: Vec<&str> = if args.scenario == "all" {
            vec![
                "kv-throughput",
                "actor-lookup",
                "node-info",
                "job-lifecycle",
                "mixed",
            ]
        } else {
            vec![args.scenario.as_str()]
        };

        for scenario in scenarios {
            for &client_count in &args.sweep {
                // Run against target A
                let ch_a = Channel::from_shared(args.target.clone())?
                    .connect()
                    .await?;
                let stats_a =
                    run_scenario(scenario, ch_a, client_count, args.requests, args.duration).await;

                // Run against target B
                let ch_b = Channel::from_shared(compare_target.clone())?
                    .connect()
                    .await?;
                let stats_b =
                    run_scenario(scenario, ch_b, client_count, args.requests, args.duration).await;

                println!("--- {scenario} ({client_count} clients) ---");
                println!(
                    "  Target A: {:.0} req/s  p50={:.1}ms  p99={:.1}ms  errors={}",
                    stats_a.throughput(),
                    stats_a.percentile(50.0) as f64 / 1000.0,
                    stats_a.percentile(99.0) as f64 / 1000.0,
                    stats_a.successes() as i64 - stats_a.successes() as i64, // errors
                );
                println!(
                    "  Target B: {:.0} req/s  p50={:.1}ms  p99={:.1}ms",
                    stats_b.throughput(),
                    stats_b.percentile(50.0) as f64 / 1000.0,
                    stats_b.percentile(99.0) as f64 / 1000.0,
                );
                let speedup = if stats_b.throughput() > 0.0 {
                    stats_a.throughput() / stats_b.throughput()
                } else {
                    f64::INFINITY
                };
                println!("  Speedup (A/B): {speedup:.2}x");
                println!();
            }
        }
    } else {
        // Single target mode
        let channel = Channel::from_shared(args.target.clone())?
            .connect()
            .await?;

        if args.scenario == "all" {
            for scenario in [
                "kv-throughput",
                "actor-lookup",
                "node-info",
                "job-lifecycle",
                "mixed",
            ] {
                let ch = channel.clone();
                let stats =
                    run_scenario(scenario, ch, args.clients, args.requests, args.duration).await;
                stats.print_summary(scenario, args.clients);
            }
        } else {
            let stats = run_scenario(
                &args.scenario,
                channel,
                args.clients,
                args.requests,
                args.duration,
            )
            .await;
            stats.print_summary(&args.scenario, args.clients);
        }
    }

    Ok(())
}

async fn run_scenario(
    scenario: &str,
    channel: Channel,
    clients: usize,
    requests: usize,
    duration: u64,
) -> stats::BenchStats {
    match scenario {
        "kv-throughput" => scenarios::kv_throughput(channel, clients, requests).await,
        "actor-lookup" => scenarios::actor_lookup(channel, clients, requests).await,
        "node-info" => scenarios::node_info(channel, clients, requests).await,
        "job-lifecycle" => scenarios::job_lifecycle(channel, clients, requests).await,
        "mixed" => scenarios::mixed(channel, clients, duration).await,
        _ => {
            eprintln!("Unknown scenario: {scenario}");
            std::process::exit(1);
        }
    }
}
