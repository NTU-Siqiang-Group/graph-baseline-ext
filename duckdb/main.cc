#include "duckdb.hpp"
#include "gflags/gflags.h"

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <random>
#include <chrono>

DEFINE_string(dataset, "", "Dataset name");
DEFINE_int32(vertex_count, 0, "Number of vertices to load");
DEFINE_string(path, "", "Path to the dataset");

const int batch_size = 256;

void load_db(duckdb::DuckDB &db, const std::string &dataset, int vertex_count, const std::string &path) {
  duckdb::Connection con(db);

  // 1. create vertex table
  auto ret = con.Query("CREATE TABLE " + dataset + "_vertex (id INTEGER PRIMARY KEY)");
  if (ret->HasError()) {
    std::cerr << "Failed to create vertex table: " << ret->GetErrorObject().Message() << std::endl;
    return;
  }
  // 2. create edge table
  ret = con.Query("CREATE TABLE " + dataset + "_edge (src INTEGER, dst INTEGER)");
  if (ret->HasError()) {
    std::cerr << "Failed to create edge table" << std::endl;
    return;
  }
  // 3. load vertex
  std::string query = "INSERT INTO " + dataset + "_vertex VALUES ";
  for (int i = 0; i < vertex_count; i++) {
    query += "(" + std::to_string(i) + "), ";
    if (i % batch_size == 0) {
      query.pop_back();
      query.pop_back();
      ret = con.Query(query);
      if (ret->HasError()) {
        std::cerr << "Failed to insert vertex" << std::endl;
        return;
      }
      query = "INSERT INTO " + dataset + "_vertex VALUES ";
    }
  }
  if (query.size() > 0) {
    query.pop_back();
    query.pop_back();
    ret = con.Query(query);
    if (ret->HasError()) {
      std::cerr << "Failed to insert vertex" << std::endl;
      return;
    }
  }
  // 4. load edge
  std::ifstream file(path);
  if (!file.is_open()) {
    std::cerr << "Failed to open file" << std::endl;
    return;
  }
  query = "INSERT INTO " + dataset + "_edge VALUES ";
  int size = 0;
  while (!file.eof()) {
    int src, dst;
    file >> src >> dst;
    query += "(" + std::to_string(src) + ", " + std::to_string(dst) + "), ";
    size ++;
    if (size % batch_size == 0) {
      query.pop_back();
      query.pop_back();
      ret = con.Query(query);
      if (ret->HasError()) {
        std::cerr << "Failed to insert edge" << std::endl;
        return;
      }
      query = "INSERT INTO " + dataset + "_edge VALUES ";
    }
    // if (size % 100000 == 0) {
    //   std::cout << "Loaded " << size << " edges" << std::endl;
    // }
  }
  if (query.size() > 0) {
    query.pop_back();
    query.pop_back();
    ret = con.Query(query);
    if (ret->HasError()) {
      std::cerr << "Failed to insert edge" << std::endl;
      return;
    }
  }
  // build index on vertex
  ret = con.Query("CREATE INDEX " + dataset + "_vertex_index ON " + dataset + "_vertex(id)");
  if (ret->HasError()) {
    std::cerr << "Failed to create index on vertex" << std::endl;
    return;
  }
  // build index on edge
  ret = con.Query("CREATE INDEX " + dataset + "_edge_index ON " + dataset + "_edge(src)");
  if (ret->HasError()) {
    std::cerr << "Failed to create index on edge" << std::endl;
    return;
  }
}

void test_rw_workload(duckdb::DuckDB &db, const std::string &dataset) {
  int read_ops = 100000;
  int write_ops = 100000;
  std::vector<int> ops;
  for (int i = 0; i < read_ops; i++) {
    ops.push_back(0);
  }
  for (int i = 0; i < write_ops; i++) {
    ops.push_back(1);
  }
  std::shuffle(ops.begin(), ops.end(), std::default_random_engine());
  duckdb::Connection con(db);
  std::vector<int64_t> read_times, write_times;
  for (size_t i = 0; i < ops.size(); i++) {
    if (ops[i] == 0) {
      int id = rand() % FLAGS_vertex_count;
      auto start = std::chrono::high_resolution_clock::now();
      auto ret = con.Query("SELECT COUNT(*) FROM " + dataset + "_edge WHERE src = " + std::to_string(id));
      auto end = std::chrono::high_resolution_clock::now();
      read_times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
      if (ret->HasError()) {
        std::cerr << "Failed to read vertex's neighbors" << std::endl;
        return;
      }
    } else {
      int src = rand() % FLAGS_vertex_count;
      int dst = rand() % FLAGS_vertex_count;
      auto start = std::chrono::high_resolution_clock::now();
      auto ret = con.Query("INSERT INTO " + dataset + "_edge VALUES (" + std::to_string(src) + ", " + std::to_string(dst) + ")");
      auto end = std::chrono::high_resolution_clock::now();
      write_times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
      if (ret->HasError()) {
        std::cerr << "Failed to add edge" << std::endl;
        return;
      }
    }
  }
  std::cout << "avg get neighbor time: " << std::accumulate(read_times.begin(), read_times.end(), 0UL) / read_times.size() << " ns" << std::endl;
  std::cout << "avg add edge time: " << std::accumulate(write_times.begin(), write_times.end(), 0UL) / write_times.size() << " ns" << std::endl;
}

int main(int argc, char** argv) {
  duckdb::DuckDB db(nullptr); // in-memory mode
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  load_db(db, FLAGS_dataset, FLAGS_vertex_count, FLAGS_path);
  duckdb::Connection con(db);
  auto ret = con.Query("SELECT COUNT(*) FROM " + FLAGS_dataset + "_vertex");
  if (!ret->HasError()) {
    std::cout << "Vertex count: " << ret->GetValue<int64_t>(0, 0) << std::endl;
  }
  ret = con.Query("SELECT COUNT(*) FROM " + FLAGS_dataset + "_edge");
  if (!ret->HasError()) {
    std::cout << "Edge count: " << ret->GetValue<int64_t>(0, 0) << std::endl;
  }
  test_rw_workload(db, FLAGS_dataset);
  return 0;
}