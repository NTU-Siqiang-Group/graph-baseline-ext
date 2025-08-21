#!/bin/bash

# graphs=(dblp twitch wikipedia orkut)
graphs=(twitter)

for graph in ${graphs[@]}
do
  echo "Running $graph..."
  ./build/nebula/nebula_rw --graph_name=$graph --command=basic >> log/nebula/exp2.log 2>&1
done
