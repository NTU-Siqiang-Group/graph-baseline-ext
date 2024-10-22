#!/bin/bash

graphs=(dblp twitch wikipedia orkut)

for graph in ${graphs[@]}
do
  echo "Running $graph..."
  ./build/nebula/nebula_rw --graph_name=$graph --command=basic >> log/exp2.log 2>&1
done