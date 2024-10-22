#include <iostream>

#include <chrono>
#include <thread>

#include "nebula_tools.h"
#include "gflags/gflags.h"

DEFINE_string(graph_name, "", "The name of the graph to be created.");
DEFINE_string(data_path, "", "The path of the data file to be loaded.");
DEFINE_string(command, "load/test/basic/load_property/test_property/test_algm", "The command to be executed.");
DEFINE_bool(undirected, false, "Whether the graph is undirected.");
DEFINE_double(read_ratio, 0.5, "The ratio of read operations in read_write_test.");
DEFINE_string(algm, "all", "The algorithm to be tested.");

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto address = "127.0.0.1:9669";
  nebula::ConnectionPool pool;
  pool.init({address}, nebula::Config{});
  auto session = pool.getSession("root", "nebula");
  if (!session.valid()) {
    return -1;
  }
  if (FLAGS_command == "load") {
    drop_test_space(session, FLAGS_graph_name);
    create_test_space(session, FLAGS_graph_name);
    load_data(session, FLAGS_data_path, FLAGS_graph_name, FLAGS_undirected);
  } else if (FLAGS_command == "test") {
    read_write_test(session, FLAGS_graph_name, FLAGS_read_ratio);
  } else if (FLAGS_command == "basic") {
    basic_op_test(session, FLAGS_graph_name);
  } else if (FLAGS_command == "load_property") {
    drop_test_space(session, FLAGS_graph_name);
    create_property_test_space(session, FLAGS_graph_name);
    load_property_graph(session, FLAGS_data_path, FLAGS_graph_name);
  } else if (FLAGS_command == "test_property") {
    test_property_graph(session, FLAGS_graph_name);
  } else if (FLAGS_command == "test_algm") {
    std::thread t([=](){
      std::this_thread::sleep_for(std::chrono::seconds(7200));
      std::cout << FLAGS_algm << " time out." << std::endl;
      exit(0);
    });
    t.detach();
    test_algm(session, FLAGS_graph_name, FLAGS_algm);
  }
  return 0;
}
