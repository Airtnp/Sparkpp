#include <iostream>
#include <chrono>
#include "spark_env.hpp"
#include "spark_context.hpp"
#include <mpi/mpi.h>

using namespace std::chrono;

SparkEnv env;

int main(int argc, char** argv) {
    addr_t masterAddr = make_pair("18.188.215.139", 25544);
    vector<addr_t> slaveAddrs = {
              {"18.218.54.64", 24457}
            , {"3.17.81.214", 24457}
    };
    env.init(argc, argv, masterAddr);
    auto sc = SparkContext{argc, argv, masterAddr, slaveAddrs};

    auto t_begin = steady_clock::now();

    auto rdd = sc.parallelize(slaveAddrs, 2);

    // failed because of no global aware of MPI_COMM_WORLD
    // need a process managing wrapper, like Torque?
    // refer MPI tutorials ping-pong
    auto mpi = rdd.map([](addr_t) {
        const int limit = 10;
        MPI_Init(NULL, NULL);
        int world_rank;
        MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
        int world_size;
        MPI_Comm_size(MPI_COMM_WORLD, &world_size);
        std::cout << world_rank << ' ' << world_size << '\n';
        int count = 0;
        int partner_rank = (world_rank + 1) % 2;
        while (count < limit) {
            if (world_rank == count % 2) {
                ++count;
                MPI_Send(&count, 1, MPI_INT, partner_rank, 0, MPI_COMM_WORLD);
                std::cout << world_rank << " sent & incremented count "
                          << count << " -> " << partner_rank << '\n';
            } else {
                MPI_Recv(&count, 1, MPI_INT, partner_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                std::cout << world_rank << " received count "
                          << count << " <- " << partner_rank << '\n';
            }
        }
        MPI_Finalize();
        return count;
    });
    auto start = mpi.collect();

    auto t_end = steady_clock::now();

    std::cout << "Collect: " << start[0] << ' ' << start[1] << '\n';
    std::cout << "Elapsed time in milliseconds: "
              << duration_cast<milliseconds>(t_end - t_begin).count() << " ms\n";
    return 0;
}