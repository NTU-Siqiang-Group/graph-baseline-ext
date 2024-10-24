#!/bin/bash

build/duckdb/duckdb_test --dataset=wikitalk --vertex_count=2394385 --path=$HOME/wiki-Talk/wiki-Talk.e
build/duckdb/duckdb_test --dataset=dblp --vertex_count=425876 --path=$HOME/graph-baselines/runtime/data/com-dblp.ungraph.json3