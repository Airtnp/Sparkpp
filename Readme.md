# Sparkpp

A na(t)ive proof-of-concept implementation of Apache Spark in C++.

Compiled & tested under gcc-9.2.1, boost-1.71, cmake-3.15.

Inspired by rust Spark implementation [native_spark](https://github.com/rajasekarv/native_spark) and based on [Spark-0.5](https://github.com/apache/spark/tree/branch-0.5)

## Example

Check examples/{*.cpp}

## Prerequisites

Check `bin/prepare.sh`

* Boost
* - [Serialization](https://github.com/boostorg/serialization)
* - [Asio](https://github.com/boostorg/asio)
* - [Beast](https://github.com/boostorg/beast)
* [Cap'n Proto](https://github.com/capnproto/capnproto)
* [fmt](https://github.com/fmtlib/fmt)
* [gperftools](https://github.com/gperftools/gperftools)
* - tcmalloc
* - google-gprof
* [concurrentqueue](https://github.com/cameron314/concurrentqueue)
* [phmap](https://github.com/greg7mdp/parallel-hashmap)

## Installation

```shell script
# install
./bin/prepare.sh                 # root
./bin/check.sh                   # check installation version
# env
export SPARK_LOCAL_IP=<local ip>
export CPUPROFILE=<profile file> # if google-gprof enabled
export CPUPROFILESIGNAL=<sig>    # if google-gprof enabled
# master
./bin/start_master.sh
# slave
./bin/start_slave.sh
```

## TODOs

- [ ] More precise concept control
- [ ] Async network support (-fcoroutines, boost::asio::io_service::async_accept), replacing raw socket + thread_pool
- [ ] Compare single boost::serialization without Cap'n Proto (& with boost flags, like no_headers)
- [ ] Add config (master/slave addr/port) file support
- [ ] new version of Spark optimizations: ShuffleWriter
- [ ] See other TODOs in files

## Random Thoughts

Check `miscs/discussion.md`

## Reference

* [Spark: Cluster Computing with Working Sets](https://www.usenix.org/legacy/event/hotcloud10/tech/full_papers/Zaharia.pdf)
* [Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf)
* [Spark-0.5](https://github.com/apache/spark/tree/branch-0.5)
* [native_spark](https://github.com/rajasekarv/native_spark)