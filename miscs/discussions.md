# Discussion

Some miscellaneous discussions.

## Development (Clion)

* Toolchains -> Add remote host (set IP)
* Cmake -> Add build option (set environment variable, IP)
* Deployment -> Set mapping (/home/ubuntu/Sparkpp)
* Start Sparkpp-slave | D/R-slave1, Sparkpp-slave | D/R-slave2, ...
* Start Sparkpp | D/R

## Design choices

* Serialization
* - which serialize protocol to choose? A common interface?
* - how to serialize the closure?
* - - ask user to write a dynamic library
* - - bytecode + interpreter
* - - expression template `Add<T, Lit<1>>`
* - - convert bytes (need to know function bytes length at runtime) -> re-parsing object layout...
* - - vtable (offset if ASLR enabled / address) for same binary
* Native memory management
* - replace malloc
* - columnar representation


## Benchmarks

* 3 (1 master, 2 slaves) Amazon EC2 t3a.large instances
* - 2 cores/4 threads
* - 8G memory
* - 8G disk
* WordCount (1.45G, 80 files) / (0.95G, 40 files)
* - Sparkpp:    43115ms 43696ms / 27931ms 28008ms
* - - bounded by unordered_map::count/string hash (std::_Hash_bytes by default)
* - Spark2.4.4: 85615ms 82586ms / 53336ms 48894ms
* - - multiple-calling will cause GC overhead limit exceeded
* - Memory: 1.13G vs. 2.87G
* Morte Carlo Pi (1e6 chunks * 1e4 chunk size, hand-written LCG)
* - Sparkpp:    29949ms 29584ms 29917ms
* - Spark2.4.4: 51571ms 50725ms 49859ms
* Morte Carlo Pi for OpenMP partition test (8 chunks * 1e9 chunk size, slow rand function)
* - Sparkpp-8: 86194ms
* - Sparkpp-4: 98916ms
* - Sparkpp-2: 97822ms
* - Sparkpp-2-OpenMP: 85410ms
* - replace thread pool to OpenMP threads
* Morte Carlo Pi for "Scalability" test (24 chunks * 1e8 chunk size, 8 partition)
* - Sparkpp-1 slave: 52469ms
* - Sparkpp-2 slave: 26355ms
* - Sparkpp-3 slave: 20685ms

## Drawbacks

* Require ABI compatible platform
* - dynamic linked library
* Require disabling ASLR
* - can be solved by serializing vtable offset
* Compiling time is a problem...
* 

## FAQ

Q: Is comparing Spark-0.5 mechanism & Spark-2.4.4 mechanism a fair comparision?
A: I think so. I assume that Spark maintains and improves performance as version number increases. E.g.

Spark2.4: [SPARK-21113] Support for read ahead input stream to amortize disk I/O cost in the spill reader
Spark2.3: [SPARK-22062][SPARK-17788][SPARK-21907] Fix various causes of OOMs
Spark2.1: [SPARK-16523]: Speeds up group-by aggregate performance by adding a fast aggregation cache that is backed by a row-based hashmap.
Spark1.6: SPARK-10000 Unified Memory Management - Shared memory for execution and caching instead of exclusive division of the regions.
Spark1.2: netty-based Spark Shuffle Manager, default sort-based Shuffle

Q: Is comparing Sparkpp & Spark-2.4.4 a fair comparision?
A: I have to admit, Sparkpp still lacks of logging, metric, event listening issues, but I suppose they are in the control path. Data path is implemented just same like Spark-0.5. The only difference is the LRU cache. since we don't have cache so far, no unfairness is revealed :).

Q: Why don't compare with native spark?
A: We tried, but failed to setup config (SSH key problem). And native spark caches shuffle in memory (by now) and I don't think it's a fair comparision. We are working on it. Also, we plan to compare it with Spark-0.8 (oldest spark release found yet)

## Import C++ ecosystem

* OpenMP, MPI (without MPI, need extra processing managing), CUDA, many ML frameworks...
* Easy to make things columnar
* strong compilers (GCC, LLVM)