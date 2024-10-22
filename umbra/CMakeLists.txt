project("UmbraTest" C CXX)
cmake_minimum_required(VERSION 3.12)

# Set CMAKE_PREFIX_PATH to prioritize correct library paths
set(CMAKE_PREFIX_PATH /usr/lib/x86_64-linux-gnu ${CMAKE_PREFIX_PATH})

set(CMAKE_CXX_STANDARD 17)

# link pq
# find_package(gflags REQUIRED)
find_library(gflags gflags)
find_package(PostgreSQL REQUIRED)
include_directories(${PostgreSQL_INCLUDE_DIRS})

add_executable(umbra_test main.cc sql.cc)
target_link_libraries(umbra_test PRIVATE ${PostgreSQL_LIBRARIES} gflags)