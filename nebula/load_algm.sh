#!/bin/bash
prefix=~/graph-baselines/runtime/data/

graphs=(citpatents wikitalk)
paths=(cit-patents.json3 wikitalk.json3)
undirects=(0 0)

for i in 0
do
  graph_name=${graphs[$i]}
  datapath="${prefix}${paths[$i]}"
  undirect=${undirects[$i]}
  echo "Loading $graph_name..."
  ./build/nebula/nebula_rw --data_path=$datapath --graph_name=$graph_name --command=load --undirected=$undirect >> log/nebula/load_algm.log 2>&1
done
