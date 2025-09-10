#!/bin/bash
echo "WikiTalk:"
build/duckdb_test --dataset=wikitalk --vertex_count=2394385 --path=../../graph-baselines/runtime/data/wikitalk.json3
echo "DBLP:"
build/duckdb_test --dataset=dblp --vertex_count=425876 --path=../../graph-baselines/runtime/data/com-dblp.ungraph.json3