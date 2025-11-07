# rsedis

[![Build Status](https://travis-ci.org/seppo0010/rsedis.svg?branch=master)](https://travis-ci.org/seppo0010/rsedis)
[![Build status](https://ci.appveyor.com/api/projects/status/m9qeoc83m18q4656?svg=true)](https://ci.appveyor.com/project/seppo0011/rsedis)

**Redis-compatible in-memory data store implemented in Rust.**

rsedis is a high-performance, Redis-compatible server that implements the majority of Redis commands and features. It achieves **80-90% of Redis throughput** in optimized scenarios while maintaining full protocol compatibility.

## Features

### ✅ Core Functionality
- **150+ Redis commands** implemented and tested
- **Full protocol compatibility** - works with redis-cli, redis-py, and other Redis clients
- **Multiple data structures**: Strings, Lists, Sets, Sorted Sets, Hashes, HyperLogLog
- **Pub/Sub** messaging system
- **Transactions** (MULTI/EXEC/DISCARD/WATCH)
- **Persistence**: RDB snapshots and AOF (Append Only File)

### ✅ Advanced Features
- **SLOWLOG** - Track and debug slow commands
- **Keyspace Notifications** - Event-driven architecture support
- **Maxmemory Eviction** - LRU, TTL, and random eviction policies
- **Memory Tracking** - Accurate memory usage reporting
- **Configuration Management** - CONFIG GET/SET support
- **Scan Commands** - SCAN, SSCAN, HSCAN, ZSCAN

### ✅ Production Ready
- **Docker Support** - Multi-stage builds, multiple variants (Debian, distroless)
- **Performance** - 30k-60k requests/sec throughput
- **Low Latency** - Sub-millisecond average latency
- **Cross-platform** - Works on Linux, Windows, macOS

## Performance

See [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md) for detailed performance analysis.

**Highlights:**
- **Throughput**: 30,000-60,000 requests/sec (80-90% of Redis baseline)
- **Latency**: 0.1-1.6ms average (1.2-3.8x Redis latency)
- **Memory**: Efficient memory usage (~34 bytes per key overhead)
- **Stability**: Handles 100k+ operations reliably

## Quick Start

### Using Docker (Recommended)

```bash
# Build and run
docker build -t rsedis:latest .
docker run -d -p 6379:6379 rsedis:latest

# Or use docker-compose
docker-compose up -d
```

See [DOCKER.md](DOCKER.md) for more Docker options and variants.

### Building from Source

**Prerequisites:**
- Rust (stable or nightly)
- Cargo

```bash
# Clone the repository
git clone https://github.com/rafaelescrich/rsedis.git
cd rsedis

# Build
cargo build --release

# Run
./target/release/rsedis

# Or with custom config
./target/release/rsedis /path/to/rsedis.conf
```

## Configuration

rsedis supports Redis-compatible configuration files. See `rsedis.conf` for examples.

**Key configuration options:**
- `port` - Server port (default: 6379)
- `bind` - Bind address (default: 0.0.0.0)
- `maxmemory` - Maximum memory usage
- `maxmemory-policy` - Eviction policy (volatile-lru, allkeys-lru, etc.)
- `slowlog-log-slower-than` - Slow log threshold (microseconds)
- `notify-keyspace-events` - Keyspace notification flags
- `appendonly` - Enable AOF persistence

See [INTEGRATION.md](INTEGRATION.md) for integration guides and examples.

## Documentation

- [TODO.md](TODO.md) - Implementation status and roadmap
- [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md) - Performance benchmarks
- [DOCKER.md](DOCKER.md) - Docker usage and variants
- [INTEGRATION.md](INTEGRATION.md) - Integration guide
- [MISSING_FEATURES.md](MISSING_FEATURES.md) - Missing features documentation
- [CRITICAL_MISSING.md](CRITICAL_MISSING.md) - Critical missing features

## Use Cases

### Why rsedis?

1. **Cross-platform** - Works on Windows, Linux, and macOS without UNIX-specific dependencies
2. **Multi-threaded** - Better utilization of multi-core machines
3. **Rust Safety** - Memory safety guarantees without garbage collection overhead
4. **Redis-Compatible** - Drop-in replacement for many Redis use cases
5. **Learning** - Great way to learn Rust and understand Redis internals

### When to Use

- ✅ Development and testing environments
- ✅ Windows environments where Redis is not available
- ✅ Applications requiring Redis-compatible API
- ✅ Multi-threaded workloads
- ✅ Learning Rust and distributed systems

### When Not to Use

- ❌ Production environments requiring full Redis feature set (replication, clustering)
- ❌ Applications requiring Lua scripting
- ❌ Redis Cluster deployments

## Current Status

**Completion: ~96%** of core Redis functionality for single-instance use cases.

**Implemented:**
- ✅ Core commands (150+)
- ✅ Data structures (Strings, Lists, Sets, Sorted Sets, Hashes, HyperLogLog)
- ✅ Pub/Sub
- ✅ Transactions
- ✅ Persistence (RDB, AOF)
- ✅ SLOWLOG
- ✅ Keyspace Notifications
- ✅ Maxmemory Eviction
- ✅ Configuration Management

**Missing:**
- ⚠️ Replication (SYNC, PSYNC)
- ⚠️ Lua Scripting
- ⚠️ Redis Cluster
- ⚠️ LATENCY command

See [TODO.md](TODO.md) for complete status.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Copyright (c) 2015, Sebastian Waisbrot  
Copyright (c) 2025, Rafael Escrich

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
