#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: scripts/semver-baseline.sh <BASELINE_REV>" >&2
  exit 1
fi

if ! cargo semver-checks --version >/dev/null 2>&1; then
  echo "cargo-semver-checks is required; install it with: cargo install cargo-semver-checks --locked" >&2
  exit 1
fi

exec cargo semver-checks check-release --workspace --baseline-rev "$1"
