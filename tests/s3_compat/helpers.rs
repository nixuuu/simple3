// Not all helpers used by every test in this crate.
#![allow(dead_code)]

use std::collections::HashMap;
use std::fmt;

use serde::Deserialize;
use testcontainers::core::{ContainerRequest, Host, Image};
use testcontainers::ImageExt;

// ── Mint log parsing ──────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct MintResult {
    pub name: Option<String>,
    pub function: Option<String>,
    pub args: Option<serde_json::Value>,
    pub duration: Option<serde_json::Value>,
    pub status: String,
    pub alert: Option<String>,
    pub message: Option<String>,
    pub error: Option<String>,
}

/// Parse Mint's newline-delimited JSON log from stdout.
/// Mint emits non-JSON progress text before the log — only lines starting
/// with `{` are treated as JSON records. Malformed JSON records panic.
pub fn parse_mint_log(raw: &str) -> Vec<MintResult> {
    raw.lines()
        .enumerate()
        .filter(|(_, l)| l.trim_start().starts_with('{'))
        .map(|(idx, l)| {
            serde_json::from_str::<MintResult>(l)
                .unwrap_or_else(|e| panic!("invalid Mint JSON at line {}: {e}", idx + 1))
        })
        .collect()
}

// ── Pytest summary parsing ────────────────────────────────────

/// Parse the pytest summary line, e.g.:
/// `= 312 passed, 35 failed, 140 skipped in 456.78s =`
pub fn parse_pytest_summary(output: &str) -> (usize, usize, usize) {
    let mut passed = 0usize;
    let mut failed = 0usize;
    let mut skipped = 0usize;
    let mut found_summary = false;

    for line in output.lines().rev() {
        let trimmed = line.trim().trim_matches('=').trim();
        if trimmed.contains("passed")
            || trimmed.contains("failed")
            || trimmed.contains("skipped")
            || trimmed.contains("error")
        {
            found_summary = true;
            for part in trimmed.split(',') {
                // Strip trailing " in N.NNs" suffix that pytest appends to the last segment.
                let part = part.trim().split(" in ").next().unwrap_or("").trim();
                let kind = part.split_whitespace().last().unwrap_or("");
                if kind == "passed" {
                    passed = parse_count(part);
                } else if kind == "failed" {
                    failed = parse_count(part);
                } else if kind == "error" || kind == "errors" {
                    failed += parse_count(part);
                } else if kind == "skipped" {
                    skipped = parse_count(part);
                }
            }
            break;
        }
    }

    assert!(
        found_summary,
        "pytest summary line not found; output may be truncated or run may have aborted"
    );
    (passed, failed, skipped)
}

fn parse_count(s: &str) -> usize {
    s.split_whitespace()
        .next()
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

// ── Report types ──────────────────────────────────────────────

pub struct FailureDetail {
    pub test_name: String,
    pub error: String,
    /// GitHub issue URL if this is a known failure.
    pub known_issue: Option<&'static str>,
}

pub struct CompatReport {
    pub suite: String,
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub failures: Vec<FailureDetail>,
}

impl fmt::Display for CompatReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "\n=== {} ===", self.suite)?;
        writeln!(
            f,
            "{}/{} passed, {} skipped, {} failed",
            self.passed, self.total, self.skipped, self.failed
        )?;

        let known_count = self.failures.iter().filter(|fd| fd.known_issue.is_some()).count();
        let unexpected_count = self.failures.iter().filter(|fd| fd.known_issue.is_none()).count();

        if known_count > 0 {
            writeln!(f, "\nKnown failures ({known_count}):")?;
            for fd in self.failures.iter().filter(|fd| fd.known_issue.is_some()) {
                writeln!(f, "  - {} -> {}", fd.test_name, fd.known_issue.unwrap_or(""))?;
            }
        }
        if unexpected_count > 0 {
            writeln!(f, "\nUnexpected failures ({unexpected_count}):")?;
            for fd in self.failures.iter().filter(|fd| fd.known_issue.is_none()) {
                writeln!(f, "  - {}", fd.test_name)?;
                if !fd.error.is_empty() {
                    for line in fd.error.lines().take(3) {
                        writeln!(f, "    {line}")?;
                    }
                }
            }
        }
        Ok(())
    }
}

// ── Mint report builder ───────────────────────────────────────

pub fn build_mint_report(
    results: &[MintResult],
    known: &HashMap<&'static str, &'static str>,
) -> CompatReport {
    let mut passed = 0;
    let mut failed = 0;
    let mut skipped = 0;
    let mut failures = Vec::new();

    for r in results {
        match r.status.as_str() {
            "PASS" => passed += 1,
            "FAIL" => {
                failed += 1;
                let name = format!(
                    "{}::{}",
                    r.name.as_deref().unwrap_or("?"),
                    r.function.as_deref().unwrap_or("?")
                );
                let known_issue = known.get(name.as_str()).copied();
                failures.push(FailureDetail {
                    test_name: name,
                    error: r.error.clone().unwrap_or_default(),
                    known_issue,
                });
            }
            _ => skipped += 1, // "NA" and any other status
        }
    }

    let total = passed + failed + skipped;
    CompatReport {
        suite: "MinIO Mint".to_owned(),
        total,
        passed,
        failed,
        skipped,
        failures,
    }
}

// ── Container networking ──────────────────────────────────────

/// Apply the correct network config so a container can reach the host.
/// - Linux: `--network=host` (container shares host network)
/// - macOS: add `host.docker.internal` pointing to host gateway
pub fn apply_network_config<I: Image>(req: ContainerRequest<I>) -> ContainerRequest<I> {
    if cfg!(target_os = "linux") {
        req.with_network("host")
    } else {
        req.with_host("host.docker.internal", Host::HostGateway)
    }
}
