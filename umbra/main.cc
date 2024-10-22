#include <iostream>
#include "sql.h"
#include "gflags/gflags.h"

DEFINE_string(command, "load", "Command to run: load, test");
DEFINE_string(dataset, "", "Dataset name");
DEFINE_string(path, "", "Path to the dataset");
DEFINE_double(read_ratio, 0.5, "Read ratio for the test workload");
DEFINE_int32(vertex_count, 0, "Number of vertices to load");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto conn = connect_db("db", "postgres", "postgres", "localhost", "5432");
  if (FLAGS_command == "load") {
    drop_db(conn, FLAGS_dataset);
    load_db(conn, FLAGS_dataset, FLAGS_vertex_count, FLAGS_path);
  } else if (FLAGS_command == "test") {
    test_rw_workload(conn, FLAGS_dataset, FLAGS_read_ratio);
  }
  disconnect_db(conn);
  return 0;
}