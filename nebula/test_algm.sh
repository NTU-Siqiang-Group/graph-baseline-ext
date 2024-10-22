#!/bin/bash

graphs=(citpatents wikitalk)
algms=(pagerank cdlp wcc sssp bfs)

for graph in ${graphs[@]}
do
  echo "Running $graph..."
  for algm in ${algms[@]}
  do
    echo "Running $algm..."
    ./build/nebula/nebula_rw --graph_name=$graph --command=test_algm --algm=$algm
  done
done