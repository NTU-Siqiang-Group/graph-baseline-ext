#include "nebula_tools.h"

#include <queue>

const int batch = 256;
const int max_ops = 2000000;
const int max_basic_op = 1000;

const auto vertex_schemas = std::unordered_map<std::string, VertexSchema>{
  {"freebase_large", { "property_v", {{"id", "int"}, {"freebaseid", "string"}, {"mid", "string"}}, {"id", "freebaseid", "mid"}}},
  {"ldbc", { "property_v", {{"id", "int"}, {"xlabel", "string"}, {"oid", "string"}}, {"id", "xlabel", "oid"}}}
};

const auto edge_schemas = std::unordered_map<std::string, EdgeSchema>{
  {"freebase_large", { "property_e", {{"id", "string"}, {"label", "string"}}, {"id", "label"}}},
  {"ldbc", { "property_e", {{"id", "string"}, {"label", "string"}}, {"id", "label"}}}
};

struct search_property_struct {
  std::string space_name;
  std::string property_name;
  std::string property_value;
};

const auto search_vertex_properties = std::unordered_map<std::string, search_property_struct>{
  {"freebase_large", {"freebase_large", "freebaseid", "484848485248"}},
  {"ldbc", {"ldbc", "xlabel", "post"}}
};

const auto search_edge_properties = std::unordered_map<std::string, search_property_struct>{
  {"freebase_large", {"freebase_large", "label", "/american_football/football_coach/coaching_history"}},
  {"ldbc", {"ldbc", "label", "hasCreator"}}
};

void read_data_from_file(const std::string &file_path, std::unordered_map<int64_t, std::vector<int64_t>>& data, bool is_undirected = false) {
  std::ifstream infile(file_path);
  while (!infile.eof()) {
    int64_t src, dst;
    infile >> src >> dst;
    data[src].push_back(dst);
    if (is_undirected) {
      data[dst].push_back(src);
    }
  }
}

std::vector<std::string> split(const std::string& src, const std::string& delimeter) {
  std::vector<std::string> result;
  auto tmp = src;
  size_t pos = 0;
  while (( pos = tmp.find(delimeter)) != std::string::npos) {
    result.push_back(tmp.substr(0, pos));
    tmp.erase(0, pos + delimeter.length());
  }
  result.push_back(tmp);
  return result;
}

void read_property_graph(const std::string &file_path, PropertyGraph &graph) {
  auto vertex_path = file_path + ".vertex";
  auto edge_path = file_path + ".edge";

  // 1. read vertex
  std::ifstream vertex_file(vertex_path);
  while (!vertex_file.eof()) {
    std::string line;
    std::getline(vertex_file, line);
    if (line.empty()) {
      continue;
    }
    auto items = split(line, " ");
    PropertyVertex vertex;
    vertex.id = std::stoull(items[0]);
    for (int i = 1; i < items.size(); i++) {
      auto kv = split(items[i], ":");
      vertex.properties.push_back(kv[0]);
      vertex.values.push_back(kv[1]);
    }
    graph.vertices.push_back(vertex);
  }

  // 2. read edge
  std::ifstream edge_file(edge_path);
  while (!edge_file.eof()) {
    std::string line;
    std::getline(edge_file, line);
    if (line.empty()) {
      continue;
    }
    auto items = split(line, " ");
    PropertyEdge edge;
    edge.src = std::stoull(items[0]);
    edge.dst = std::stoull(items[1]);
    for (int i = 2; i < items.size(); i++) {
      auto kv = split(items[i], ":");
      edge.properties.push_back(kv[0]);
      edge.values.push_back(kv[1]);
    }
    graph.edges.push_back(edge);
  }
}

int64_t get_max_vid(nebula::Session &session, const std::string& tag_name = "v") {
  auto result = session.execute("match (v1:"+ tag_name + ") return max(id(v1)) as maxId;");
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Fail to get vertex count. Exit with error code: " << static_cast<int>(result.errorCode) << std::endl;
    exit(0);
  }
  int64_t max_vid = result.data->colValues("maxId")[0].getInt();
  return max_vid;
}

void load_property_graph(nebula::Session &session, const std::string &file_path, const std::string &space_name) {
  PropertyGraph graph;
  read_property_graph(file_path, graph);
  std::cout << "Finish read graph, vertex size: " << graph.vertices.size() << ", edge size: " << graph.edges.size() << std::endl;
  auto result = session.execute("use " + space_name + ";");
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Fail to use space. Exit with error code: " << static_cast<int>(result.errorCode) << std::endl;
    exit(0);
  }

  // 1. insert vertices
  auto start_time = std::chrono::steady_clock::now();
  int cur_batch_count = 0;
  int idx = 0;
  std::vector<std::vector<std::string>> values;
  for (auto& vertex : graph.vertices) {
    std::vector<std::string> value;
    value.push_back(std::to_string(vertex.id));
    value.push_back(std::to_string(vertex.id));
    for (auto& val : vertex.values) {
      value.push_back(val);
    }
    values.push_back(value);
    cur_batch_count++;
    idx ++;
    if (cur_batch_count == batch) {
      auto query = vertex_schemas.find(space_name)->second.get_insert_query(values);
      auto result = session.execute(query);
      if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
        std::cout << "Insert Vertex Exit with error code: " << static_cast<int>(result.errorCode) << ", query: " << query << std::endl;
        exit(0);
      }
      cur_batch_count = 0;
      values.clear();
    }
    if (idx % 100000 == 0) {
      auto end_time = std::chrono::steady_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
      std::cout << "Insert Vertex: " << idx << ", Time: " << duration << " ms" << std::endl;
    }
  }
  if (values.size() != 0) {
    auto query = vertex_schemas.find(space_name)->second.get_insert_query(values);
    auto result = session.execute(query);
    if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
      std::cout << "Insert Vertex Exit with error code: " << static_cast<int>(result.errorCode) << ", query: " << query << std::endl;
      exit(0);
    }
  }
  auto end_time = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
  std::cout << "Insert Vertex Done, use time: " << duration << std::endl;
  
  // 2. insert edge
  start_time = std::chrono::steady_clock::now();
  cur_batch_count = 0;
  idx = 0;
  values.clear();
  for (auto& edge : graph.edges) {
    std::vector<std::string> value;
    value.push_back(std::to_string(edge.src));
    value.push_back(std::to_string(edge.dst));
    value.push_back(std::to_string(edge.src) + "=>" + std::to_string(edge.dst));
    for (auto& val : edge.values) {
      value.push_back(val);
    }
    values.push_back(value);
    cur_batch_count++;
    idx ++;
    if (cur_batch_count == batch) {
      auto query = edge_schemas.find(space_name)->second.get_insert_query(values);
      auto result = session.execute(query);
      if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
        std::cout << "Insert Edge Exit with error code: " << static_cast<int>(result.errorCode) << ", query: " << query << std::endl;
        exit(0);
      }
      cur_batch_count = 0;
      values.clear();
    }
    if (idx % 100000 == 0) {
      auto end_time = std::chrono::steady_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
      std::cout << "Insert Edge: " << idx << ", Time: " << duration << " ms" << std::endl;
    }
  }
  if (values.size() != 0) {
    auto query = edge_schemas.find(space_name)->second.get_insert_query(values);
    auto result = session.execute(query);
    if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
      std::cout << "Insert Edge Exit with error code: " << static_cast<int>(result.errorCode) << ", query: " << query << std::endl;
      exit(0);
    }
  }
  end_time = std::chrono::steady_clock::now();
  duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
  std::cout << "Insert Edge Done, use time: " << duration << std::endl;
}

void create_property_test_space(nebula::Session &session, const std::string &space_name) {
  auto result = session.execute("create space if not exists " + space_name + " (vid_type=INT64);");
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Fail to create space. Exit with error code: " << static_cast<int>(result.errorCode) << std::endl;
    exit(0);
  }
  std::this_thread::sleep_for(std::chrono::seconds(10));
  result = session.execute("use " + space_name + ";");
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Fail to use space. Exit with error code: " << static_cast<int>(result.errorCode) << std::endl;
    exit(0);
  }
  auto vertex_schema = vertex_schemas.find(space_name);
  if (vertex_schema == vertex_schemas.end()) {
    std::cout << "Can't find vertex schema for space: " << space_name << std::endl;
    exit(0);
  }
  result = session.execute(vertex_schema->second.get_create_schema_query());
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Create Vertex Schema Exit with error code: " << static_cast<int>(result.errorCode) << std::endl;
    exit(0);
  }
  auto edge_schema = edge_schemas.find(space_name);
  if (edge_schema == edge_schemas.end()) {
    std::cout << "Can't find edge schema for space: " << space_name << std::endl;
    exit(0);
  }
  result = session.execute(edge_schema->second.get_create_schema_query());
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Create Edge Schema Exit with error code: " << static_cast<int>(result.errorCode) << std::endl;
    exit(0);
  }
  std::this_thread::sleep_for(std::chrono::seconds(10));
}

void basic_op_test(nebula::Session &session, const std::string &space_name) {
  auto result = session.execute("use " + space_name + ";");
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Fail to use space. Exit with error code: " << static_cast<int>(result.errorCode) << std::endl;
    exit(0);
  }
  int64_t max_vid = get_max_vid(session);
  auto start_time = std::chrono::steady_clock::now();
  // 1. add vertex
  for (int i = 0; i < max_basic_op; i++) {
    int64_t vid = std::rand() % (max_vid - 1) + 1;
    std::string query = "insert vertex v(id) values " + std::to_string(vid) + ":(" + std::to_string(vid) + ");";
    auto result = session.execute(query);
    if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
      std::cout << "Add Vertex Exit with error code: " << static_cast<int>(result.errorCode) << ", vid: " << vid << ", query: " << query << std::endl;
      exit(0);
    }
  }
  auto end_time = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
  std::cout << "Add Vertex Done, Time per op: " << duration / max_basic_op << " ns" << std::endl;
  // 2. add edge
  start_time = std::chrono::steady_clock::now();
  for (int i = 0; i < max_basic_op; i++) {
    int64_t src = std::rand() % (max_vid - 1) + 1;
    int64_t dst = std::rand() % (max_vid - 1) + 1;
    std::string query = "insert edge e(id) values " + std::to_string(src) + "->" + std::to_string(dst) + ":(\"" + std::to_string(src) + "=>" + std::to_string(dst) + "\");";
    auto result = session.execute(query);
    if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
      std::cout << "Add Edge Exit with error code: " << static_cast<int>(result.errorCode) << ", src: " << src << ", dst: " << dst << ", query: " << query << std::endl;
      exit(0);
    }
  }
  end_time = std::chrono::steady_clock::now();
  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
  std::cout << "Add Edge Done, Time per op: " << duration / max_basic_op << " ns" << std::endl;
  // 3. delete edge
  int used_time = 0, test_time = 0;
  for (int i = 0; i < max_basic_op; i++) {
    int64_t src = std::rand() % (max_vid - 1) + 1;
    // get out neighbors
    std::string query = "match (v1:v)-->(v2:v) where id(v1) == " + std::to_string(src) + " return id(v2) as dst;";
    auto result = session.execute(query);
    // get the first dst
    if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
      std::cout << "Get Out Neighbors Exit with error code: " << static_cast<int>(result.errorCode) << ", src: " << src << ", query: " << query << std::endl;
      exit(0);
    }
    if (result.data->colValues("dst").size() == 0) {
      continue;
    }
    int64_t dst = result.data->colValues("dst")[0].getInt();
    // delete edge
    query = "delete edge e " + std::to_string(src) + "->" + std::to_string(dst) + ";";
    auto start_time = std::chrono::steady_clock::now();
    result = session.execute(query);
    if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
      std::cout << "Delete Edge Exit with error code: " << static_cast<int>(result.errorCode) << ", src: " << src << ", dst: " << dst << ", query: " << query << std::endl;
      exit(0);
    }
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    used_time += duration;
    test_time ++;
  }
  std::cout << "Delete Edge Done, Time per op: " << used_time / test_time << " ns, Test Time: " << test_time << std::endl;
  // 4. get neighbors
  used_time = 0, test_time = 0;
  for (int i = 0; i < max_basic_op; i++) {
    int64_t vid = std::rand() % (max_vid - 1) + 1;
    std::string query = "match (v1:v)-->(v2:v) where id(v1) == " + std::to_string(vid) + " return id(v2) as dst;";
    auto start_time = std::chrono::steady_clock::now();
    auto result = session.execute(query);
    if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
      std::cout << "Get Neighbors Exit with error code: " << static_cast<int>(result.errorCode) << ", vid: " << vid << ", query: " << query << std::endl;
      exit(0);
    }
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    used_time += duration;
    test_time ++;
  }
  std::cout << "Get Neighbors Done, Time per op: " << used_time / test_time << " ns, Test Time: " << test_time << std::endl;
}

void read_write_test(nebula::Session &session, const std::string &space_name, double read_ratio) {
  auto result = session.execute("use " + space_name + ";");
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Fail to use space. Exit with error code: " << static_cast<int>(result.errorCode) << std::endl;
    exit(0);
  }
  result = session.execute("match (v1:v) return max(id(v1)) as maxId;");
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Fail to get vertex count. Exit with error code: " << static_cast<int>(result.errorCode) << std::endl;
    exit(0);
  }
  int64_t max_vid = result.data->colValues("maxId")[0].getInt();
  std::cout << "max vertex id: " << max_vid << std::endl;
  std::vector<int> ops;
  for (int i = 0; i < max_ops; i++) {
    if (i < read_ratio * max_ops) {
      ops.push_back(0);
    } else {
      ops.push_back(1);
    }
  }
  std::random_shuffle(ops.begin(), ops.end());
  auto start_time = std::chrono::steady_clock::now();
  int idx = 0;
  for (auto op : ops) {
    idx ++;
    if (op == 0) {
      int64_t vid = rand() % (max_vid - 1) + 1;
      std::string query = "match (v1:v)-->(v2:v) where id(v1) == " + std::to_string(vid) + " return id(v2);";
      auto result = session.execute(query);
      if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
        std::cout << "Read Vertex Exit with error code: " << static_cast<int>(result.errorCode) << ", vid: " << vid << ", query: " << query << std::endl;
        exit(0);
      }
    } else {
      int64_t src = rand() % (max_vid - 1) + 1;
      int64_t dst = rand() % (max_vid - 1) + 1;
      std::string query = "insert edge e(id) values " + std::to_string(src) + "->" + std::to_string(dst) + ": (\"" + std::to_string(src) + "=>" + std::to_string(dst) + "\");";
      auto result = session.execute(query);
      if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
        std::cout << "Write Edge Exit with error code: " << static_cast<int>(result.errorCode) << ", src: " << src << ", dst: " << dst << ", query: " << query << std::endl;
        exit(0);
      }
    }
    if (idx % 100000 == 0) {
      auto end_time = std::chrono::steady_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
      std::cout << "Read Write Test: " << idx << ", Time: " << duration << " ms" << std::endl;
    }
  }
  auto end_time = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
  std::cout << "Read Write Test Done, Time: " << duration << " ms" << std::endl;
}

void test_property_graph(nebula::Session &session, const std::string &space_name) {
  auto result = session.execute("use " + space_name + ";");
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Fail to use space. Exit with error code: " << static_cast<int>(result.errorCode) << std::endl;
    exit(0);
  }
  int max_vid = get_max_vid(session, "property_v");
  // build index on search property
  auto search_vertex = search_vertex_properties.find(space_name);
  if (search_vertex == search_vertex_properties.end()) {
    std::cout << "Can't find search vertex property for space: " << space_name << std::endl;
    exit(0);
  }
  std::string property_name = search_vertex->second.property_name;
  auto query = "create tag index if not exists " + search_vertex->second.space_name + "_v_index on property_v " + "(" + property_name + "(10));";
  result = session.execute(query);
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Create Vertex Index Exit with error code: " << static_cast<int>(result.errorCode) << ", query: " << query << std::endl;
    exit(0);
  }
  // rebuild tag index
  // query = "rebuild tag index " + search_vertex->second.space_name + "_v_index;";
  // result = session.execute(query);
  // if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
  //   std::cout << "Rebuild Vertex Index Exit with error code: " << static_cast<int>(result.errorCode) << ", query: " << query << std::endl;
  //   exit(0);
  // }
  auto search_edge = search_edge_properties.find(space_name);
  if (search_edge == search_edge_properties.end()) {
    std::cout << "Can't find search edge property for space: " << space_name << std::endl;
    exit(0);
  }
  property_name = search_edge->second.property_name;
  query = "create edge index if not exists " + search_edge->second.space_name + "_e_index on property_e " + "(" + property_name + "(10));";
  // rebuild edge index
  // query = "rebuild edge index " + search_edge->second.space_name + "_e_index;";
  // result = session.execute(query);
  // if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
  //   std::cout << "Rebuild Edge Index Exit with error code: " << static_cast<int>(result.errorCode) << ", query: " << query << std::endl;
  //   exit(0);
  // }
  // 1. search vertex by property
  auto start_time = std::chrono::steady_clock::now();
  query = "match (v1:property_v{" + search_vertex->second.property_name + ":\"" + search_vertex->second.property_value + "\"}) return id(v1);";
  result = session.execute(query);
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Search Vertex Exit with error code: " << static_cast<int>(result.errorCode) << ", query: " << query << std::endl;
    exit(0);
  }
  auto end_time = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
  std::cout << "Search Vertex Done, Time: " << duration << " ns" << std::endl;
  // 2. search edge by property
  start_time = std::chrono::steady_clock::now();
  query = "match (v1)-[e1:property_e{" + search_edge->second.property_name + ":\"" + search_edge->second.property_value + "\"}]->(v2) return id(e1);";
  result = session.execute(query);
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Search Edge Exit with error code: " << static_cast<int>(result.errorCode) << ", query: " << query << std::endl;
    exit(0);
  }
  end_time = std::chrono::steady_clock::now();
  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
  std::cout << "Search Edge Done, Time: " << duration << " ns" << std::endl;
  // 3. update vertex property
  start_time = std::chrono::steady_clock::now();
  int32_t rand_vid = rand() % (max_vid - 1) + 1;
  query = "update vertex on property_v " + std::to_string(rand_vid) + " set " + search_vertex->second.property_name + " = \"new_prop_value\";";
  result = session.execute(query);
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Update Vertex Exit with error code: " << static_cast<int>(result.errorCode) << ", query: " << query << std::endl;
    exit(0);
  }
  end_time = std::chrono::steady_clock::now();
  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
  std::cout << "Update Vertex Done, Time: " << duration << " ns" << std::endl;
  // 4. update edge property
  int32_t rand_src = rand() % (max_vid - 1) + 1;
  int32_t dst = -1;
  while (dst < 0) {
    // get neighbors
    query = "match (v1:property_v)-->(v2:property_v) where id(v1) == " + std::to_string(rand_src) + " return id(v2);";
    result = session.execute(query);
    if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
      std::cout << "Get Neighbors Exit with error code: " << static_cast<int>(result.errorCode) << ", query: " << query << std::endl;
      exit(0);
    }
    if (result.data->colValues("id(v2)").size() == 0) {
      rand_src = rand() % (max_vid - 1) + 1;
      continue;
    }
    dst = result.data->colValues("id(v2)")[0].getInt();
  }
  start_time = std::chrono::steady_clock::now();
  query = "update edge on property_e " + std::to_string(rand_src) + "->" + std::to_string(dst) + " set " + search_edge->second.property_name + " = \"new_prop_value\";";
  result = session.execute(query);
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Update Edge Exit with error code: " << static_cast<int>(result.errorCode) << ", query: " << query << std::endl;
    exit(0);
  }
  end_time = std::chrono::steady_clock::now();
  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
  std::cout << "Update Edge Done, Time: " << duration << " ns" << std::endl;
}

void create_test_space(nebula::Session &session, const std::string &space_name) { 
  auto result = session.execute("create space if not exists " + space_name + " (vid_type=INT64);");
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Fail to create space. Exit with error code: " << static_cast<int>(result.errorCode) << std::endl;
    exit(0);
  }
  std::this_thread::sleep_for(std::chrono::seconds(10));
  result = session.execute("use " + space_name + ";");
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Fail to use space. Exit with error code: " << static_cast<int>(result.errorCode) << std::endl;
    exit(0);
  }
  result = session.execute("create tag v(id int);");
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Create Vertex Schema Exit with error code: " << static_cast<int>(result.errorCode) << std::endl;
    exit(0);
  }
  result = session.execute("create edge e(id string);");
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Create Edge Schema Exit with error code: " << static_cast<int>(result.errorCode) << std::endl;
    exit(0);
  }
  std::this_thread::sleep_for(std::chrono::seconds(10));
}

void drop_test_space(nebula::Session &session, const std::string &space_name) {
  auto result = session.execute("drop space if exists " + space_name + ";");
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Fail to drop space. Exit with error code: " << static_cast<int>(result.errorCode) << std::endl;
    exit(0);
  }
}

void load_data(nebula::Session &session, const std::string &file_path, const std::string &space_name, bool is_undirected) {
  std::unordered_map<int64_t, std::vector<int64_t>> graph;
  read_data_from_file(file_path, graph);
  int idx = 0;
  std::cout << "start loading data..." << std::endl;
  // insert vertices
  auto start_time = std::chrono::steady_clock::now();
  int cur_batch_count = 0;
  std::string init = "insert vertex v(id) values ";
  std::string query = init;
  for (auto& it : graph) {
    int64_t outVid = it.first;
    char buff[1024] = {0};
    sprintf(buff, "%ld:(%ld)", outVid, outVid);
    query += std::string(buff);
    cur_batch_count++;
    if (cur_batch_count == batch) {
      query += ";";
      auto result = session.execute(query);
      if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
        std::cout << "Insert Vertex Exit with error code: " << static_cast<int>(result.errorCode) 
          << ", msg: " << result.errorMsg->c_str() << ", query: " << query << std::endl;
        exit(0);
      }
      cur_batch_count = 0;
      query = init;
    } else {
      query += ",";
    }
    idx ++;
    if (idx % 100000 == 0) {
      auto end_time = std::chrono::steady_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
      std::cout << "Insert Vertex: " << idx << ", Time: " << duration.count() << "ms" << std::endl;
    }
  }
  if (cur_batch_count > 0) {
    query[query.size() - 1] = ';';
    auto result = session.execute(query);
    if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
      std::cout << "Insert Vertex Exit with error code: " << static_cast<int>(result.errorCode) 
        << ", msg: " << result.errorMsg->c_str() << ", query: " << query << std::endl;
      exit(0);
    }
  }
  auto end_time = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
  std::cout << "Insert Vertex Done, Time: " << duration.count() << "ms" << std::endl;
  // 2. insert edges
  start_time = std::chrono::steady_clock::now();
  cur_batch_count = 0;
  init = "insert edge e(id) values ";
  query = init;
  idx = 0;
  for (auto& it : graph) {
    int64_t src = it.first;
    for (auto& dst : it.second) {
      char buff[1024] = {0};
      sprintf(buff, "%ld->%ld:(\"%ld=>%ld\")", src, dst, src, dst);
      query += std::string(buff);
      cur_batch_count++;
      if (cur_batch_count == batch) {
        query += ";";
        auto result = session.execute(query);
        if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
          std::cout << "Insert Edge Exit with error code: " << static_cast<int>(result.errorCode) 
            << ", msg: " << result.errorMsg->c_str() << ", query: " << query << std::endl;
          exit(0);
        }
        cur_batch_count = 0;
        query = init;
      } else {
        query += ",";
      }
      idx ++;
      if (idx % 1000000 == 0) {
        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        std::cout << "Insert Edge: " << idx << ", Time: " << duration.count() << "ms" << std::endl;
      }
    }
  }
  if (cur_batch_count > 0) {
    query[query.size() - 1] = ';';
    auto result = session.execute(query);
    if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
      std::cout << "Insert Edge Exit with error code: " << static_cast<int>(result.errorCode) 
        << ", msg: " << result.errorMsg->c_str() << ", query: " << query << std::endl;
      exit(0);
    }
  }
  end_time = std::chrono::steady_clock::now();
  duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
  std::cout << "Insert Edge Done, Time: " << duration.count() << "ms" << std::endl;
}

void get_out_neighbor_ids(nebula::Session &session, std::vector<int32_t>& out, int32_t vid) {
  auto query = "match (v1:v)-->(v2:v) where id(v1) == " + std::to_string(vid) + " return id(v2);";
  auto result = session.execute(query);
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Get Neighbors Exit with error code: " << static_cast<int>(result.errorCode) << ", query: " << query << std::endl;
    exit(0);
  }
  for (auto& val : result.data->colValues("id(v2)")) {
    out.push_back(val.getInt());
  }
}

void pagerank(nebula::Session &session, int32_t max_vid) {
  const double damping_factor = 0.85;
  const double epsilon = 1e-4;
  const int max_iter = 10;
  std::vector<int32_t> all_ids;
  auto query = "match (v1:v) return id(v1);";
  auto result = session.execute(query);
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Get All Vertex Exit with error code: " << static_cast<int>(result.errorCode) << ", query: " << query << std::endl;
    exit(0);
  }
  for (auto& val : result.data->colValues("id(v1)")) {
    all_ids.push_back(val.getInt());
  }
  std::unordered_map<int32_t, double> pr;
  for (auto& id : all_ids) {
    pr[id] = 1.0 / all_ids.size();
  }
  bool converged = false;
  int iter = 0;
  while (!converged && iter < max_iter) {
    std::unordered_map<int32_t, double> new_pr;
    for (auto& id : all_ids) {
      new_pr[id] = (1 - damping_factor) / all_ids.size();
    }
    for (size_t i = 0; i < all_ids.size(); i++) {
      auto out_ids = std::vector<int32_t>();
      get_out_neighbor_ids(session, out_ids, all_ids[i]);
      for (size_t j = 0; j < out_ids.size(); j++) {
        new_pr[out_ids[j]] += damping_factor * pr[all_ids[i]] / out_ids.size();
      }
    }
    converged = true;
    for (size_t i = 0; i < all_ids.size(); i++) {
      if (std::abs(new_pr[all_ids[i]] - pr[all_ids[i]]) > epsilon) {
        converged = false;
      }
      pr[all_ids[i]] = new_pr[all_ids[i]];
    }
    iter ++;
  }
}

void sssp(nebula::Session &session, int32_t start_vid, int32_t end_vid) {
  std::queue<int32_t> q;
  q.push(start_vid);
  while (!q.empty()) {
    int32_t vid = q.front();
    q.pop();
    if (vid == end_vid) {
      break;
    }
    auto out_ids = std::vector<int32_t>();
    get_out_neighbor_ids(session, out_ids, vid);
    for (auto& out_id : out_ids) {
      q.push(out_id);
    }
  }
}

void bfs(nebula::Session &session, int32_t start_vid, int32_t depth) {
  std::queue<std::pair<int, int>> q;
  q.push({start_vid, 0});
  while (!q.empty()) {
    auto p = q.front();
    q.pop();
    int vid = p.first;
    int cur_depth = p.second;
    if (cur_depth == depth) {
      break;
    }
    auto out_ids = std::vector<int32_t>();
    get_out_neighbor_ids(session, out_ids, vid);
    for (auto& out_id : out_ids) {
      q.push({out_id, cur_depth + 1});
    }
  }
}

void cdlp(nebula::Session &session) {
  std::vector<int32_t> all_ids;
  auto query = "match (v1:v) return id(v1);";
  auto result = session.execute(query);
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Get All Vertex Exit with error code: " << static_cast<int>(result.errorCode) << ", query: " << query << std::endl;
    exit(0);
  }
  for (auto& val : result.data->colValues("id(v1)")) {
    all_ids.push_back(val.getInt());
  }
  std::unordered_map<int32_t, int32_t> labels;
  for (size_t i = 0; i < all_ids.size(); i++) {
    labels[all_ids[i]] = all_ids[i];
  }
  const int max_iters = 10;
  for (int i = 0; i < max_iters; i++) {
    bool changeMade = false;
    for (size_t j = 0; j < all_ids.size(); j++) {
      int rand_idx = rand() % all_ids.size();
      int tmp = all_ids[j];
      all_ids[j] = all_ids[rand_idx];
      all_ids[rand_idx] = tmp;
    }
    for (size_t j = 0; j < all_ids.size(); j++) {
      std::unordered_map<int, int> labelCounts;
      int vid = all_ids[j];
      // get both in and out neighbors
      auto query = "go from " + std::to_string(vid) + " over e bidirect yield e._dst as ids";
      auto result = session.execute(query);
      if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
        std::cout << "Get Neighbors Exit with error code: " << static_cast<int>(result.errorCode) << ", query: " << query << std::endl;
        exit(0);
      }
      std::vector<int32_t> neighbors;
      for (size_t k = 0; k < result.data->colValues("ids").size(); k++) {
        neighbors.push_back(result.data->colValues("ids")[k].getInt());
      }
      for (size_t k = 0; k < neighbors.size(); k++) {
        int label = labels[neighbors[k]];
        labelCounts[label] ++;
      }
      int max_count = -1;
      int new_label = labels[all_ids[j]];
      for (auto& it : labelCounts) {
        if (it.second > max_count || (it.second == max_count && it.first < new_label)) {
          max_count = it.second;
          new_label = it.first;
        }
      }
      if (new_label != labels[all_ids[j]]) {
        changeMade = true;
        labels[all_ids[j]] = new_label;
      }
    }
    if (!changeMade) {
      break;
    }
  }
}

void wcc(nebula::Session &session, int32_t max_vid) {
  std::vector<int32_t> all_ids;
  auto query = "match (v1:v) return id(v1);";
  auto result = session.execute(query);
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Get All Vertex Exit with error code: " << static_cast<int>(result.errorCode) << ", query: " << query << std::endl;
    exit(0);
  }
  for (auto& val : result.data->colValues("id(v1)")) {
    all_ids.push_back(val.getInt());
  }
  std::unordered_map<int32_t, int32_t> labels;
  for (size_t i = 0; i < all_ids.size(); i++) {
    labels[all_ids[i]] = all_ids[i];
  }
  bool changeMade = true;
  while (changeMade) {
    changeMade = false;
    std::unordered_map<int32_t, int32_t> new_labels;
    for (size_t i = 0; i < all_ids.size(); i++) {
      new_labels[all_ids[i]] = labels[all_ids[i]];
      // get both in and out neighbors
      auto query = "go from " + std::to_string(all_ids[i]) + " over e bidirect yield e._dst as ids";
      auto result = session.execute(query);
      if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
        std::cout << "Get Neighbors Exit with error code: " << static_cast<int>(result.errorCode) << ", query: " << query << std::endl;
        exit(0);
      }
      std::vector<int32_t> neighbors;
      for (size_t k = 0; k < result.data->colValues("ids").size(); k++) {
        neighbors.push_back(result.data->colValues("ids")[k].getInt());
      }
      int min_val = labels[all_ids[i]];
      for (size_t k = 0; k < neighbors.size(); k++) {
        if (labels[neighbors[k]] < min_val) {
          min_val = labels[neighbors[k]];
        }
      }
      if (min_val != labels[all_ids[i]]) {
        new_labels[all_ids[i]] = min_val;
        changeMade = true;
      }
    }
    labels = new_labels;
  }
}

void test_algm(nebula::Session &session, const std::string &space_name, const std::string &algm) {
  auto query = "use " + space_name + ";";
  auto result = session.execute(query);
  if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
    std::cout << "Fail to use space. Exit with error code: " << static_cast<int>(result.errorCode) << std::endl;
    exit(0);
  }
  int32_t max_vid = get_max_vid(session);

  if (algm == "pagerank" || algm == "all") {
    auto start_time = std::chrono::steady_clock::now();
    pagerank(session, max_vid);
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "Pagerank Done, Time: " << duration << " ms" << std::endl;
  }
  if (algm == "sssp" || algm == "all") {
    int start_vid = 3494505;
    int end_vid = 754148;
    if (space_name == "wikitalk") {
      start_vid = 32822;
      end_vid = 33;
    }
    auto start_time = std::chrono::steady_clock::now();
    sssp(session, start_vid, end_vid);
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "SSSP Done, Time: " << duration << " ms" << std::endl;
  }
  if (algm == "bfs" || algm == "all") {
    int start_vid = 3494505;
    int depth = 5;
    if (space_name == "wikitalk") {
      start_vid = 6765;
    }
    auto start_time = std::chrono::steady_clock::now();
    bfs(session, start_vid, depth);
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "BFS Done, Time: " << duration << " ms" << std::endl;
  }
  if (algm == "cdlp" || algm == "all") {
    auto start_time = std::chrono::steady_clock::now();
    cdlp(session);
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "CDLP Done, Time: " << duration << " ms" << std::endl;
  }
  if (algm == "wcc" || algm == "all") {
    auto start_time = std::chrono::steady_clock::now();
    wcc(session, max_vid);
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "WCC Done, Time: " << duration << " ms" << std::endl;
  }
}