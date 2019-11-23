# Sparkpp

A prototype implementation of Spark in C++.

## Design choices

* Serialization
* - which serialize protocol to choose? A common interface?
* - how to serialize the closure?
* - - ask user to write a dynamic library
* - - bytecode + interpreter
* - - expression template `Add<T, Lit<1>>`
* - - convert bytes (need to know function bytes length at runtime) -> reparsing object layout...
* - - vtable (offset if ASLR enabled / address) for same binary
* Native memory management
* - replace malloc
* - columnar representation

## Example

* \[WIP\]

## TODOs

- [ ] Use OpenMP in lambdas? Write MPI wrapper
- [ ] More precise concept control
- [ ] Async support (-fcoroutines, boost::asio::io_service::async_accept), replacing raw socket + thread_pool
- [ ] Compare single boost::serialization without Cap'n Proto (& with boost flags, like no_headers)
- [ ] Add config (master/slave addr/port) file support
- [ ] See other TODOs in files
- [ ] new version of Spark optimizations: ShuffleWriter


## Required Libraries

* Boost
* - [Serialization](https://github.com/boostorg/serialization)
* - [Asio](https://github.com/boostorg/asio)
* - [Beast](https://github.com/boostorg/beast)
* [Cap'n Proto](https://github.com/capnproto/capnproto)
* [fmt](https://github.com/fmtlib/fmt)

## Installation

```shell script
./bin/prepare.sh
export SPARK_LOCAL_IP=<local ip>
# master
./bin/start_master.sh
# slave
./bin/start_slave.sh
```

## Development (Clion)

* Toolchains -> Add remote host (set IP)
* Cmake -> Add build option (set environment variable, IP)
* Deployment -> Set mapping (/home/ubuntu/Sparkpp)
* Start Sparkpp-slave | D/R-slave1, Sparkpp-slave | D/R-slave2, ...
* Start Sparkpp | D/R

## Benchmarks
* 3 Amazon EC2 t3a.large instances (2 cores/4 threads, 8G memory, 8G disk)
* WordCount (1.45G, 80 files) / (0.95G, 40 files)
* - Sparkpp: 43115ms 43696ms / 27931ms 28008ms (overhead, unordered_map::count/string hash (std::_Hash_bytes by default))
* - Spark2.4.4: 85615ms 82586ms / 53336ms 48894ms
* - Memory: 1.13G vs. 2.87G
* Morte Carlo Pi (1e6 * 1e4 tests, hand-written LCG)
* - Sparkpp: 29949ms 29584ms 29917ms
* - Spark2.4.4: 51571ms 50725ms 49859ms
* Morte Carlo Pi for OpenMP test (1e6 * 1e4 tests, slow rand function)
* - Sparkpp-4: 
* - Sparkpp-2: 
* - 





## Reference

* [Spark: Cluster Computing with Working Sets](https://www.usenix.org/legacy/event/hotcloud10/tech/full_papers/Zaharia.pdf)
* [Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf)
* [Spark-0.5](https://github.com/apache/spark/tree/branch-0.5)
* [native_spark](https://github.com/rajasekarv/native_spark)