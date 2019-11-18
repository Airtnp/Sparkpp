//
// Created by xiaol on 11/7/2019.
//

#ifndef SPARKPP_COMMON_HPP
#define SPARKPP_COMMON_HPP

#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <cstddef>

#include <atomic>

using std::atomic;

#include <vector>
#include <array>
#include <utility>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <list>
#include <tuple>

/// Everything sequence becomes a vector now...
template <typename T>
using Seq = std::vector<T>;

using std::array;
using std::list;
using std::pair;
using std::vector;
using std::string;
using std::map;
using std::unordered_map;
using std::set;
using std::unordered_set;
using std::tuple;

using std::make_pair;
using std::make_tuple;

#include <mutex>
#include <shared_mutex>
#include <thread>

using std::mutex;
using std::shared_mutex;
using std::lock_guard;
using std::shared_lock;
using std::unique_lock;
using std::thread;

#include <memory>

using std::byte;
using std::forward;
using std::move;
using std::unique_ptr;
using std::shared_ptr;
using std::make_unique;
using std::make_shared;

#include <functional>

using std::invoke;

#include <filesystem>
#include <experimental/filesystem>

namespace fs = std::experimental::filesystem;

// #include <optional>
// #include <variant>

// using std::optional;
// using std::variant;

#include <any>
using std::any;

#include <iostream>

using std::cout;
using std::cerr;
using std::clog;

#include <algorithm>
#include <numeric>

using std::copy_n;
using std::remove_if;

#include <type_traits>

using std::decay_t;

#include <chrono>

using namespace std::chrono_literals;


#include "utils/utils.hpp"


#include "concurrentqueue/blockingconcurrentqueue.h"

using moodycamel::BlockingConcurrentQueue;

#include "parallel-hashmap/parallel_hashmap/phmap.h"

using phmap::parallel_flat_hash_map;
using phmap::parallel_flat_hash_set;

using addr_t = pair<string, uint16_t>;
using host_t = string;

#include <boost/variant.hpp>
#include <boost/optional.hpp>

using boost::optional;
using boost::variant;

#include <fmt/format.h>



#endif //SPARKPP_COMMON_HPP
