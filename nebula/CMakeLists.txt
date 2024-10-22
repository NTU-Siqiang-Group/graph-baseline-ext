project("NebulaTest" C CXX)
cmake_minimum_required(VERSION 3.12)

set(CMAKE_CXX_STANDARD 17)
if (NOT NEBULA_HOME)
    set(NEBULA_HOME /usr/local/nebula)
endif()

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -static-libstdc++")
find_package(gflags REQUIRED)
include_directories(${NEBULA_HOME}/include)
link_directories(${NEBULA_HOME}/lib64)
link_directories(${NEBULA_HOME}/lib)

add_library(nebula_tools nebula_tools.cc)
add_executable(nebula_rw nebula_rw.cc)

target_link_libraries(nebula_rw
    PRIVATE
    nebula_graph_client
    nebula_tools
    gflags
    -pthread
)