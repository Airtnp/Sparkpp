#include <cstdlib>
#include <memory>
#include <iostream>
#include "spark_env.hpp"
#include "spark_context.hpp"
#include "serialize_capnp.hpp"
#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include <kj/io.h>

SparkEnv env;

int main(int argc, char** argv) {
    // TODO: make this external configuration
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
    auto v = rdd2.reduce([](int x, int y) {
        return x + y;
    });
    std::cout << v << '\n';
    return 0;
}