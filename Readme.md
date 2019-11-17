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
- [ ] Test optimization flags
```text
CC          = gcc
CCSTD       = -std=c99
CCWFLAGS    = -Wall -Wextra -Wshadow -Wpedantic --pedantic-errors -O2 $(CCSTD)
CCNFLAGS    = -Wno-unused-value -Wno-unused-parameter -Wno-attributes -Wno-unused-variable
CCGDBFLAGS  = -g
CCFLAGS     = $(CCWFLAGS) $(CCNFLAGS) $(CCGDBFLAGS)

CXX         = g++
CXXSTD      = -std=c++17
CXXOPFLAGS  = -Ofast -DNDEBUG \
	      -march=native \
	      -fwhole-program -flto \
	      -fprefetch-loop-arrays \
	      -Wno-coverage-mismatch \
	      -fno-rtti \
	      -fomit-frame-pointer \
	      -falign-functions=16 -falign-loops=16
CXXWFLAGS   = -Wall -Wextra -Wshadow -Wpedantic --pedantic-errors
CXXADWFLAGS = -Wduplicated-cond -Wduplicated-branches \
	      -Wlogical-op -Wnull-deference -Wold-style-cast \
	      -Wuseless-cast -Wjump-misses-init \
	      -Wdoule-promotion -Wformat=2
	      # GCC7: -Wrestrict
CXXNFLAGS   = -Wno-unused-value -Wno-unused-parameter -Wno-attributes -Wno-unused-variable
CXXSANFLAGS = -fsanitize=undefined,address,leak,bounds,bool,enum
CXXDBFLAGS  = -g3 -Og -DDEBUG
CXXFLAGS    = $(CXXWFLAGS) $(CXXNFLAGS) $(CXXSTD)
```



## Required Libraries

* Boost
* - Serialization
* - Outcome
* Cap'n Proto

## Installation

```shell script
./bin/prepare.sh
./bin/gen_capnp.sh
export SPARK_LOCAL_IP=<local ip>
# master
./bin/start_master.sh
# slave
./bin/start_slave.sh
```

Set environment variable "SPARK_LOCAL_IP"





## Reference

* [Spark: Cluster Computing with Working Sets](https://www.usenix.org/legacy/event/hotcloud10/tech/full_papers/Zaharia.pdf)
* [Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf)
