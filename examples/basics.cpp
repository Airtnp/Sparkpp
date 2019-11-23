#include <iostream>
#include "spark_env.hpp"
#include "spark_context.hpp"

SparkEnv env;

int main(int argc, char** argv) {
    addr_t masterAddr = make_pair("18.188.215.139", 25544);
    vector<addr_t> slaveAddrs = {
            {"18.218.54.64", 24457},
            {"3.17.81.214", 24457}
    };
    env.init(argc, argv, masterAddr);
    auto sc = SparkContext{argc, argv, masterAddr, slaveAddrs};
    vector<int> values = {1, 2, 3, 4, 5, 6, 7};
    auto rdd = sc.parallelize(values, 3);
    auto rdd2 = rdd.map([](int x) {
        return x + 1;
    });
    auto rdd3 = rdd2.map([](int x) {
        return x - 1;
    });
    auto rdd4 = rdd3.mapPair([](int x) {
        return make_pair(x % 2, x);
    });
    // (1, 3, 5, 7) | (2, 4, 6)
    auto rdd5 = rdd4.groupByKey(2);
    // 16 | 12
    auto rdd7 = rdd5.map([](pair<int, vector<int>> x) -> int {
        int acc = 0;
        for (auto i : x.second) {
            acc += i;
        }
        return acc;
    });
    auto v = rdd7.collect();
    // 12, 16 or 16, 12
    std::cout << v[0] << v[1] << '\n';
    return 0;
}