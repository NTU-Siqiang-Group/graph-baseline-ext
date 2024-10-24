#!/bin/bash
graphs=(freebase_large ldbc)

for graph in ${graphs[@]}
do
  echo "Running $graph..."
  ./build/nebula/nebula_rw --graph_name=$graph --command=test_property >> log/nebula/property.log 2>&1
  echo "----------"
done