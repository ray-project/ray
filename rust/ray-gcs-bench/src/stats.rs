use std::time::Duration;

use hdrhistogram::Histogram;

/// Collects latency samples and computes throughput + percentile statistics.
pub struct BenchStats {
    histogram: Histogram<u64>,
    errors: u64,
    total_requests: u64,
    elapsed: Duration,
}

impl BenchStats {
    pub fn new() -> Self {
        Self {
            // Track latencies from 1us to 60s with 3 significant figures
            histogram: Histogram::new_with_bounds(1, 60_000_000, 3).unwrap(),
            errors: 0,
            total_requests: 0,
            elapsed: Duration::ZERO,
        }
    }

    /// Record a successful request latency.
    pub fn record(&mut self, latency: Duration) {
        let micros = latency.as_micros() as u64;
        let _ = self.histogram.record(micros.max(1));
        self.total_requests += 1;
    }

    /// Record a failed request.
    pub fn record_error(&mut self) {
        self.errors += 1;
        self.total_requests += 1;
    }

    /// Set the total elapsed time for the benchmark.
    pub fn set_elapsed(&mut self, elapsed: Duration) {
        self.elapsed = elapsed;
    }

    /// Merge stats from another instance (for aggregating across clients).
    pub fn merge(&mut self, other: &BenchStats) {
        self.histogram.add(&other.histogram).unwrap();
        self.errors += other.errors;
        self.total_requests += other.total_requests;
    }

    /// Total successful requests.
    pub fn successes(&self) -> u64 {
        self.total_requests - self.errors
    }

    /// Throughput in requests/second.
    pub fn throughput(&self) -> f64 {
        if self.elapsed.as_secs_f64() > 0.0 {
            self.successes() as f64 / self.elapsed.as_secs_f64()
        } else {
            0.0
        }
    }

    /// Latency percentile in microseconds.
    pub fn percentile(&self, p: f64) -> u64 {
        self.histogram.value_at_percentile(p)
    }

    /// Print a summary table.
    pub fn print_summary(&self, scenario: &str, clients: usize) {
        println!("--- {scenario} ({clients} clients) ---");
        println!(
            "  Total:     {} requests ({} ok, {} errors)",
            self.total_requests,
            self.successes(),
            self.errors
        );
        println!("  Duration:  {:.2}s", self.elapsed.as_secs_f64());
        println!("  Throughput: {:.0} req/s", self.throughput());
        println!(
            "  Latency:   p50={:.1}ms  p95={:.1}ms  p99={:.1}ms  max={:.1}ms",
            self.percentile(50.0) as f64 / 1000.0,
            self.percentile(95.0) as f64 / 1000.0,
            self.percentile(99.0) as f64 / 1000.0,
            self.percentile(100.0) as f64 / 1000.0,
        );
        println!(
            "  Error rate: {:.2}%",
            if self.total_requests > 0 {
                self.errors as f64 / self.total_requests as f64 * 100.0
            } else {
                0.0
            }
        );
        println!();
    }
}
