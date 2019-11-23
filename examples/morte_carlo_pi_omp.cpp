#include <iostream>
#include <chrono>
#include <algorithm>
#include <random>
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

    constexpr long long chunks = 8;
    constexpr long long chunkSize = 1e9;
    vector<long long> values(chunks);
    std::iota(values.begin(), values.end(), 0ll);

    auto t_begin = steady_clock::now();

    // insufficient use of slaves (2 cores, 4 threads)
    auto rdd = sc.parallelize(values, 2);
    auto random = rdd.map([](long long) noexcept {
        unsigned long long count = 0;
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<double> dis{-1.0, 1.0};
        // but we can start OpenMP tasks per slave (by FIFO dispatching)
        #pragma omp parallel for reduction(+:count) default(none) private(dis, gen)
        for (auto i = 0; i < chunkSize; ++i) {
            double x = dis(gen);
            double y = dis(gen);
            if (x * x + y * y < 1) {
                ++count;
            }
        }
        return count;
    });

    auto cnt = random.reduce([](unsigned long long n, unsigned long long m) {
        return n + m;
    });

    auto t_end = steady_clock::now();

    std::cout << "Pi = " << (4.0 * cnt / chunks / chunkSize) << '\n';
    std::cout << "Elapsed time in milliseconds: "
              << duration_cast<milliseconds>(t_end - t_begin).count() << " ms\n";
    return 0;
}