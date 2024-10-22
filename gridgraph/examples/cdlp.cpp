/*
Copyright (c) 2014-2015 Xiaowei Zhu, Tsinghua University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "core/graph.hpp"

#include <unordered_map>

struct LabelCounts {
  std::unordered_map<int, int> counts;
  std::mutex mutex;
  void add(int label) {
    std::lock_guard<std::mutex> lock(mutex);
    counts[label]++;
  }
};

int main(int argc, char ** argv) {
	if (argc<2) {
		fprintf(stderr, "usage: cdlp [path] [memory budget in GB]\n");
		exit(-1);
	}
	std::string path = argv[1];
	long memory_bytes = (argc>=3)?atol(argv[2])*1024l*1024l*1024l:8l*1024l*1024l*1024l;

	Graph graph(path);
	graph.set_memory_bytes(memory_bytes);
	Bitmap * active_in = graph.alloc_bitmap();
  Bitmap * active_out = graph.alloc_bitmap();
  active_in->fill();
	BigVector<VertexId> label(graph.path+"/label", graph.vertices);
	graph.set_vertex_data_bytes( graph.vertices * sizeof(VertexId) );
  VertexId active_vertices = graph.stream_vertices<VertexId>([&](VertexId i){
		label[i] = i;
		return 1;
	});

	double start_time = get_time();
  bool changed = false;
	int iteration = 0;
	while (iteration < 10) {
		iteration++;
    printf("Iteration: %d\n", iteration);
		std::vector<LabelCounts*> label_counts(graph.vertices);
    for (VertexId i = 0; i < graph.vertices; i++) {
      label_counts[i] = new LabelCounts();
    }
    graph.hint(label);
    graph.stream_edges<VertexId>([&](Edge & e){
      label_counts[e.source]->add(label[e.target]);
      return 1;
    }, active_in);
    // update label
    changed = false;
    for (VertexId i = 0; i < graph.vertices; i++) {
      int max_count = 0;
      int max_label = label[i];
      for (auto it = label_counts[i]->counts.begin(); it != label_counts[i]->counts.end(); it++) {
        if (it->second > max_count) {
          max_count = it->second;
          max_label = it->first;
        }
      }
      if (max_label != label[i]) {
        label[i] = max_label;
        changed = true;
      }
    }
    // delete label counts
    for (VertexId i = 0; i < graph.vertices; i++) {
      delete label_counts[i];
    }
    if (!changed) {
      break;
    }
	}
	double end_time = get_time();

	BigVector<VertexId> label_stat(graph.path+"/label_stat", graph.vertices);
	label_stat.fill(0);
	graph.stream_vertices<VertexId>([&](VertexId i){
		write_add(&label_stat[label[i]], 1);
		return 1;
	});
  printf("Total time: %.2f seconds\n", end_time - start_time);

	return 0;
}
