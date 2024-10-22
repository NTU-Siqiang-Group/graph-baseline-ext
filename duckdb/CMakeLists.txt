project("DuckDBTest" C CXX)
cmake_minimum_required(VERSION 3.12)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

find_library(DUCKDB_LIB duckdb)
find_library(gflags gflags)
find_path(DUCKDB_INCLUDE_DIR duckdb.hpp)

if(NOT DUCKDB_LIB)
  message(FATAL_ERROR "DuckDB library not found.")
endif()

if(NOT DUCKDB_INCLUDE_DIR)
  message(FATAL_ERROR "DuckDB include directory not found.")
endif()

include_directories(${DUCKDB_INCLUDE_DIR})

add_executable(duckdb_test main.cc)

target_link_libraries(duckdb_test PRIVATE ${DUCKDB_LIB} gflags)