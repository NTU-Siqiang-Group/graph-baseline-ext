#!/bin/bash
cd duckdb
git clone git@github.com:duckdb/duckdb.git
cd duckdb
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
sudo make install
sudo make -j10
cd ../build
sudo cmake .. -DCMAKE_BUILD_TYPE=Release
sudo make -j10