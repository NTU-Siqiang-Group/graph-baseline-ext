#!/bin/bash

# graphs=(dblp orkut wikipedia)
graphs=(twitter)
ratios=(0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9)

for graph in ${graphs[@]}
do
  for ratio in ${ratios[@]}
  do
    echo "Running $graph with ratio $ratio..."
    ./build/nebula/nebula_rw --graph_name=$graph --command=test --read_ratio=$ratio >> log/nebula/exp1.log 2>&1
  done
done
