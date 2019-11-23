cmake_minimum_required(VERSION 3.0)
project(Sparkpp)

set(CMAKE_CXX_STANDARD 17)
set (CXX_EXTRA_FLAGS
        "-Wall -Wextra
        -fconcepts
        -Wno-missing-field-initializers
        -Wno-comment
        -lboost_serialization
")
add_definitions(${CXX_EXTRA_FLAGS})
set(CMAKE_CXX_FLAGS_RELEASE
        "${CMAKE_CXX_FLAGS_RELEASE} -march=native -mtune=native -fwhole-program -flto")

# for profiler, since gcc use -Wl,--as-needed by default
# set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-no-as-needed")

if (CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(-march=native -mtune=native -fwhole-program -flto)
endif()

# configure_file()

# execute_process(COMMAND capnp compile -oc++
#         "${CMAKE_CURRENT_LIST_DIR}/capnp/data.capnp"
#         "--src-prefix=")
# set(CAPNP_INCLUDE_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/3rdparty/capnp)
# CAPNP_GENERATE_CPP(CapnpSources CapnpHeaders capnp/data.capnp)
# message("${CapnpSources}")
set (CapnpSources "${PROJECT_SOURCE_DIR}/capnph/data.capnp.c++")
find_package(CapnProto CONFIG REQUIRED)

set(Boost_USE_MULTITHREADED ON)
find_package(Boost REQUIRED COMPONENTS system thread serialization)
add_library(boost::serialization INTERFACE IMPORTED)
include_directories(${Boost_INCLUDE_DIRS})

find_package(fmt)

# for openmp
# find_package(OpenMP REQUIRED)

# for mpich
# find_package(MPI REQUIRED)
# include_directories(SYSTEM ${MPI_INCLUDE_PATH})

include_directories(include)
include_directories(capnph)
include_directories(3rdparty)
include_directories(3rdparty/concurrentqueue)
include_directories(3rdparty/parallel-hashmap)


file(GLOB_RECURSE HEADERS
        /concurrentqueue)
file(GLOB_RECURSE HEADERS
        "${PROJECT_SOURCE_DIR}/include/*.hpp"
        )
file(GLOB_RECURSE SRCS
        "${PROJECT_SOURCE_DIR}/src/*.cpp"
        )
add_executable(Sparkpp ${HEADERS} ${SRCS} ${CapnpSources})

target_include_directories(Sparkpp PRIVATE include)
target_include_directories(Sparkpp PRIVATE capnph)
target_include_directories(Sparkpp PRIVATE 3rdparty)
target_include_directories(Sparkpp PRIVATE 3rdparty/concurrentqueue)
target_include_directories(Sparkpp PRIVATE 3rdparty/parallel-hashmap)

target_link_libraries(Sparkpp stdc++fs)
target_link_libraries(Sparkpp boost::serialization)
target_link_libraries(Sparkpp ${Boost_LIBRARIES})
target_link_libraries(Sparkpp CapnProto::capnp)
target_link_libraries(Sparkpp fmt::fmt)

# for tcmalloc
target_link_libraries(Sparkpp tcmalloc)

# for profiler
# target_link_libraries(Sparkpp profiler)

# for openmp
# target_link_libraries(Sparkpp OpenMP::OpenMP_CXX)

# for mpi
# target_link_libraries(Sparkpp MPI::MPI_C)