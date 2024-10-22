#include "sql.h"
#include <iostream>
#include <fstream>
#include <vector>
#include <chrono>
#include <algorithm>
#include <random>

const int max_batch_size = 1024;
const int workload_size = 500;

PGconn* connect_db(const std::string& dbname, const std::string& user, const std::string& password, const std::string& host, const std::string& port) {
  std::string conninfo = "dbname=" + dbname + " user=" + user + " password=" + password + " host=" + host + " port=" + port;
  PGconn *conn = PQconnectdb(conninfo.c_str());
  if (PQstatus(conn) == CONNECTION_BAD) {
    std::cerr << "Connection to database failed: " << PQerrorMessage(conn) << std::endl;
    PQfinish(conn);
    exit(1);
  } else {
    std::cout << "Connected to the database!" << std::endl;
  }

  // Create an SQL command (for example, querying the database)
  const char *query = "SELECT version();";
  PGresult *res = PQexec(conn, query);

  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    std::cerr << "Query failed: " << PQerrorMessage(conn) << std::endl;
  } else {
    std::cout << "PostgreSQL version: " << PQgetvalue(res, 0, 0) << std::endl;
  }
  return conn;
}

void disconnect_db(PGconn* conn) {
  PQfinish(conn);
}

void read_edges(const std::string& path, std::vector<std::pair<int, int>>& edges) {
  std::ifstream file(path);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << path << std::endl;
    exit(1);
  }
  while (!file.eof()) {
    int src, dst;
    file >> src >> dst;
    edges.push_back(std::make_pair(src, dst));
  }
  file.close();
}

void drop_db(PGconn* conn, const std::string& dataset) {
  // drop vertex index
  std::string drop_v_index = "DROP INDEX IF EXISTS " + dataset + "_V_id;";
  PGresult *res = PQexec(conn, drop_v_index.c_str());
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    std::cerr << "Index deletion failed: " << PQerrorMessage(conn) << std::endl;
  }
  // drop vertex table
  std::string drop_v_table = "DROP TABLE IF EXISTS " + dataset + "_V;";
  res = PQexec(conn, drop_v_table.c_str());
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    std::cerr << "Table deletion failed: " << PQerrorMessage(conn) << std::endl;
  }
  // drop edge index
  std::string drop_e_index = "DROP INDEX IF EXISTS " + dataset + "_E_src;";
  res = PQexec(conn, drop_e_index.c_str());
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    std::cerr << "Index deletion failed: " << PQerrorMessage(conn) << std::endl;
  }
  // drop edge table
  std::string drop_e_table = "DROP TABLE IF EXISTS " + dataset + "_E;";
  res = PQexec(conn, drop_e_table.c_str());
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    std::cerr << "Table deletion failed: " << PQerrorMessage(conn) << std::endl;
  }
}


void load_db(PGconn* conn, const std::string& dataset, uint32_t vertex_count, const std::string& path) {
  // Load the dataset into the database
  std::vector<std::pair<int, int>> edges;
  read_edges(path, edges);
  std::cout << "Read " << edges.size() << " edges" << std::endl;  
  std::string create_v_table = "CREATE TABLE " + dataset + "_V (id INT);";
  PGresult *res = PQexec(conn, create_v_table.c_str());
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    std::cerr << "Table creation failed: " << PQerrorMessage(conn) << std::endl;
  }
  // create vertex index
  std::string create_v_index = "CREATE INDEX " + dataset + "_V_id ON " + dataset + "_V (id);";
  res = PQexec(conn, create_v_index.c_str());
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    std::cerr << "Index creation failed: " << PQerrorMessage(conn) << std::endl;
  }
  std::string query = "insert into " + dataset + "_V(id) values ";
  for (int i = 0; i < vertex_count; i++) {
    query += "(" + std::to_string(i) + "),";
    if (i != 0 && i % max_batch_size == 0) {
      query.pop_back();
      res = PQexec(conn, query.c_str());
      if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::cerr << "Insertion failed: " << PQerrorMessage(conn) << std::endl;
      }
      query = "insert into " + dataset + "_V(id) values ";
    }
    if (i % 100000 == 0) {
      std::cout << "Inserted " << i << " vertices" << std::endl;
    }
  }
  if (query.back() == ',') {
    query.pop_back();
    res = PQexec(conn, query.c_str());
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
      std::cerr << "Insertion failed: " << PQerrorMessage(conn) << std::endl;
    }
  }
  std::string create_e_table = "CREATE TABLE " + dataset + "_E (src INT, dst INT);";
  res = PQexec(conn, create_e_table.c_str());
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    std::cerr << "Table creation failed: " << PQerrorMessage(conn) << std::endl;
  }
  // create edge index
  std::string create_e_index = "CREATE INDEX " + dataset + "_E_src ON " + dataset + "_E (src,dst);";
  res = PQexec(conn, create_e_index.c_str());
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    std::cerr << "Index creation failed: " << PQerrorMessage(conn) << std::endl;
  }
  query = "insert into " + dataset + "_E(src,dst) values ";
  for (int i = 0; i < edges.size(); i++) {
    query += "(" + std::to_string(edges[i].first) + "," + std::to_string(edges[i].second) + "),";
    if (i != 0 && i % max_batch_size == 0) {
      query.pop_back();
      res = PQexec(conn, query.c_str());
      if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::cerr << "Insertion failed: " << PQerrorMessage(conn) << std::endl;
      }
      query = "insert into " + dataset + "_E(src,dst) values ";
    }
    if (i % 100000 == 0) {
      std::cout << "Inserted " << i << " edges" << std::endl;
    }
  }
  if (query.back() == ',') {
    query.pop_back();
    res = PQexec(conn, query.c_str());
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
      std::cerr << "Insertion failed: " << PQerrorMessage(conn) << std::endl;
    }
  }
}

int get_neighbors(PGconn* conn, const std::string& dataset, int max_vertex_id) {
  int rand_vertex_id = rand() % max_vertex_id;
  auto start = std::chrono::high_resolution_clock::now();
  std::string query = "SELECT dst FROM " + dataset + "_E WHERE src = " + std::to_string(rand_vertex_id) + ";";
  PGresult *res = PQexec(conn, query.c_str());
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    std::cerr << "Query failed: " << PQerrorMessage(conn) << std::endl;
  }
  auto end = std::chrono::high_resolution_clock::now();
  return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}

int64_t add_edge(PGconn* conn, const std::string& dataset, int max_vertex_id) {
  int src = rand() % max_vertex_id;
  int dst = rand() % max_vertex_id;
  auto start = std::chrono::high_resolution_clock::now();
  std::string query = "INSERT INTO " + dataset + "_E (src, dst) VALUES (" + std::to_string(src) + "," + std::to_string(dst) + ");";
  PGresult *res = PQexec(conn, query.c_str());
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    std::cerr << "Insertion failed: " << PQerrorMessage(conn) << std::endl;
  }
  auto end = std::chrono::high_resolution_clock::now();
  int64_t duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
  // offset the change
  query = "delete from " + dataset + "_E where src = " + std::to_string(src) + " and dst = " + std::to_string(dst) + ";";
  res = PQexec(conn, query.c_str());
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    std::cerr << "Deletion failed: " << PQerrorMessage(conn) << std::endl;
  }
  return duration;
}

void test_rw_workload(PGconn* conn, const std::string& dataset, double read_ratio) {
  // get max vertex id
  std::string query = "SELECT max(id) FROM " + dataset + "_V;";
  PGresult *res = PQexec(conn, query.c_str());
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    std::cerr << "Query failed: " << PQerrorMessage(conn) << std::endl;
  }
  int max_vertex_id = std::stoi(PQgetvalue(res, 0, 0));
  int read_count = (int)(workload_size * read_ratio);
  int write_count = workload_size - read_count;
  std::vector<int64_t> read_durations, write_durations, ops;
  // preload all the edges and vertexes
  query = "SELECT * FROM " + dataset + "_E;";
  res = PQexec(conn, query.c_str());
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    std::cerr << "Query failed: " << PQerrorMessage(conn) << std::endl;
  }
  query = "SELECT * FROM " + dataset + "_V;";
  res = PQexec(conn, query.c_str());
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    std::cerr << "Query failed: " << PQerrorMessage(conn) << std::endl;
  }
  std::cout << "Preloaded all the edges and vertexes" << std::endl;
  for (int i = 0; i < read_count; i++) {
    ops.push_back(0);
  }
  for (int i = 0; i < write_count; i++) {
    ops.push_back(1);
  }
  std::shuffle(ops.begin(), ops.end(), std::default_random_engine());
  for (int i = 0; i < workload_size; i++) {
    if (ops[i] == 0) {
      read_durations.push_back(get_neighbors(conn, dataset, max_vertex_id));
    } else {
      write_durations.push_back(add_edge(conn, dataset, max_vertex_id));
    }
    if (i % 100000 == 0) {
      std::cout << "Processed " << i << " operations" << std::endl;
    }
  }
  std::cout << "Read ratio: " << read_ratio << ", Average read duration: " 
    << std::accumulate(read_durations.begin(), read_durations.end(), 0UL) / read_durations.size() << " ns, Average write duration: "
    << std::accumulate(write_durations.begin(), write_durations.end(), 0UL) / write_durations.size() << " ns" << std::endl;
}