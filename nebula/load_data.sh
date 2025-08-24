#!/bin/bash
prefix=../graph-baselines/runtime/data/
paths=(com-dblp.ungraph.json3 com-orkut.ungraph.json3 wikipedia.json3)
graphs=(dblp orkut wikipedia)
undirects=(1 1 0)

for i in {0..2}
do
  graph_name=${graphs[$i]}
  datapath="${prefix}${paths[$i]}"
  undirect=${undirects[$i]}
  echo "Loading $graph_name..."
  ./build/nebula/nebula_rw --data_path=$datapath --graph_name=$graph_name --command=load --undirected=$undirect >> log/nebula/load.log 2>&1
done
