cmake_minimum_required(VERSION 3.0)
project(Sparkpp)

set(CMAKE_CXX_STANDARD 17)
set (CXX_EXTRA_FLAGS
        "-Wall -Wextra
        -fconcepts
        -Wno-missing-field-initializers
        -Wno-comment
        -Wno-unused-parameter
        -lboost_serialization
") # G++ 9.2 in MSYS2 cannot support -fcoroutines...
# TODO: remove -Wno-unused-parameter
add_definitions(${CXX_EXTRA_FLAGS})
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

include_directories(include)
include_directories(capnph)
include_directories(3rdparty)
include_directories(3rdparty/concurrentqueue)
include_directories(3rdparty/parallel-hashmap)
include_directories(3rdparty/outcome)
# include_directories(3rdparty/abseil)
# include_directories(3rdparty/ranges-v3/include)
file(GLOB_RECURSE HEADERS
        /concurrentqueue)
include_directories(3rdparty/outcome)
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
target_include_directories(Sparkpp PRIVATE 3rdparty/outcome)
# target_include_directories(Sparkpp PRIVATE 3rdparty/abseil)
# target_include_directories(Sparkpp PRIVATE 3rdparty/ranges-v3/include)
target_include_directories(Sparkpp PRIVATE 3rdparty/parallel-hashmap)
if (WIN32)
    target_link_libraries(Sparkpp wsock32 ws2_32)
endif (WIN32)
target_link_libraries(Sparkpp stdc++fs)
target_link_libraries(Sparkpp boost::serialization)
target_link_libraries(Sparkpp ${Boost_LIBRARIES})
target_link_libraries(Sparkpp CapnProto::capnp)
target_link_libraries(Sparkpp CapnProto::capnp-rpc)
target_link_libraries(Sparkpp fmt::fmt)