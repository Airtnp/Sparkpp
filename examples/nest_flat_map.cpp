#include <iostream>
#include <algorithm>
#include <chrono>
#include "spark_env.hpp"
#include "spark_context.hpp"

using namespace std::chrono;

SparkEnv env;

int main(int argc, char** argv) {
    addr_t masterAddr = make_pair("18.188.215.139", 25544);
    vector<addr_t> slaveAddrs = {
            {"18.218.54.64", 24457},
            {"3.17.81.214", 24457}
    };
    env.init(argc, argv, masterAddr);
    auto sc = SparkContext{argc, argv, masterAddr, slaveAddrs};
    vector<size_t> values(4);
    std::iota(values.begin(), values.end(), 0);

    auto t_begin = steady_clock::now();
    auto rdd = sc.parallelize(values, 8);
    // 0, 1, 1, 2, 2, 3, 3, 4
    auto rdd2 = rdd.flatMap([](int v) {
        return vector<int>{v, v + 1};
    });
    // 0, 2, 2, 4, 4, 6, 6, 8, 0, 1, 1, 2, 2, 3, 3, 4
    auto rdd3 = rdd2.flatMap([](int v) {
        return vector<int>{v, v * 2};
    });
    auto rdd4 = rdd3.mapPair([](int v) {
        return make_pair(v % 2, v);
    });
    auto rdd5 = rdd4.reduceByKey([](int a, int b) {
        return a + b;
    }, 2);
    // 8, 40
    auto result = rdd5.collect();
    auto t_end = steady_clock::now();

    for (auto i : result) {
        std::cout << i.first << ' ' << i.second << '\n';
    }
    std::cout << result.size() << '\n';
    std::cout << "Elapsed time in milliseconds: "
              << duration_cast<milliseconds>(t_end - t_begin).count() << " ms\n";
    return 0;
}