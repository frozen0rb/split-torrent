#!/usr/bin/env bash
# Runs the st-server and st-client-core tests as if the kernel had no IPv6
# stack (ipv6.disable=1 / OpenVZ / LXC): an LD_PRELOAD shim makes every
# AF_INET6 socket() call fail with EAFNOSUPPORT.
set -euo pipefail
cd "$(dirname "$0")/.."

shim="$(mktemp -d)/no-ipv6.so"
cc -shared -fPIC -O2 -o "$shim" scripts/no-ipv6-preload.c -ldl

# Build first so the shim only affects the test binaries, not cargo/rustc.
cargo test -p st-server -p st-client-core --no-run

export ST_EXPECT_NO_IPV6=1
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="env LD_PRELOAD=$shim"
cargo test -p st-server -p st-client-core
