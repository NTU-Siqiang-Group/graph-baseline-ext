#pragma once

#include "nebula/client/ConnectionPool.h"
#include "nebula/client/Config.h"

#include <string.h>
#include <fstream>
#include <unordered_map>
#include <vector>

void drop_test_space(nebula::Session &session, const std::string &space_name);
void create_test_space(nebula::Session &session, const std::string &space_name);
void load_data(nebula::Session &session, const std::string &file_path, const std::string &space_name, bool is_undirected = false);
void read_write_test(nebula::Session &session, const std::string &space_name, double read_ratio = 0.5);
void basic_op_test(nebula::Session &session, const std::string &space_name);
void load_property_graph(nebula::Session &session, const std::string &file_path, const std::string &space_name);
void create_property_test_space(nebula::Session &session, const std::string &space_name);
void test_property_graph(nebula::Session &session, const std::string &space_name);
void test_algm(nebula::Session &session, const std::string &space_name, const std::string &algm);

struct PropertyVertex {
  uint64_t id;
  std::vector<std::string> properties;
  std::vector<std::string> values;

};

struct PropertyEdge {
  uint64_t src;
  uint64_t dst;
  std::vector<std::string> properties;
  std::vector<std::string> values;
};

struct PropertyGraph {
  std::vector<PropertyVertex> vertices;
  std::vector<PropertyEdge> edges;
};

struct VertexSchema {
  std::string schema_name;
  std::unordered_map<std::string, std::string> attrs;
  std::vector<std::string> orders;
  
  std::string get_create_schema_query() const {
    std::string query = "create tag " + schema_name + " (";
    for (auto &attr : attrs) {
      query += attr.first + " " + attr.second + ", ";
    }
    query.pop_back();
    query.pop_back();
    query += ");";
    return query;
  }

  std::string get_insert_query(std::vector<std::vector<std::string>> &values) const {
    std::string query = "insert vertex " + schema_name + "(";
    for (auto &attr : orders) {
      query += attr + ", ";
    }
    query.pop_back();
    query.pop_back();
    query += ") values ";
    for (auto &value : values) {
      query += value[0] + ":(";
      query += value[1] + ", "; // int
      for (int i = 2; i < value.size(); i++) {
        query += "\"" + value[i] + "\", "; // string
      }
      query.pop_back();
      query.pop_back();
      query += "), ";
    }
    query.pop_back();
    query.pop_back();
    query += ";";
    return query;
  }
};

struct EdgeSchema {
  std::string schema_name;
  std::unordered_map<std::string, std::string> attrs;
  std::vector<std::string> orders;
  
  std::string get_create_schema_query() const {
    std::string query = "create edge " + schema_name + " (";
    for (auto &attr : attrs) {
      query += attr.first + " " + attr.second + ", ";
    }
    query.pop_back();
    query.pop_back();
    query += ");";
    return query;
  }

  std::string get_insert_query(std::vector<std::vector<std::string>> &values) const {
    std::string query = "insert edge " + schema_name + "(";
    for (auto &attr : orders) {
      query += attr + ", ";
    }
    query.pop_back();
    query.pop_back();
    query += ") values ";
    for (auto &value : values) {
      query += value[0] + "->" + value[1] + ":(";
      for (int i = 2; i < value.size(); i++) {
        query += "\"" + value[i] + "\", "; // string
      }
      query.pop_back();
      query.pop_back();
      query += "), ";
    }
    query.pop_back();
    query.pop_back();
    query += ";";
    return query;
  }
};