#pragma once

#include <libpq-fe.h>
#include <string>

PGconn* connect_db(const std::string& dbname, const std::string& user, const std::string& password, const std::string& host, const std::string& port);
void disconnect_db(PGconn* conn);

void drop_db(PGconn* conn, const std::string& dataset);
void load_db(PGconn* conn, const std::string& dataset, uint32_t vertex_count, const std::string& path);
int get_neighbors(PGconn* conn, const std::string& dataset, int max_vertex_id);
int64_t add_edge(PGconn* conn, const std::string& dataset, int max_vertex_id);

void test_rw_workload(PGconn* conn, const std::string& dataset, double read_ratio);