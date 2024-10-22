# Baseline Extension for Aster

More baselines added for revision. Please install the following required package before running the code.

## DuckDB
1. Clone the source code:
```
git clone git@github.com:duckdb/duckdb.git
```
2. Build & install
```
cd duckdb
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
sudo make install
```

## Umbra
You can try using the Docker image:
```
docker run -v umbra-db:/var/db -p 5432:5432 --ulimit nofile=1048576:1048576 --ulimit memlock=8388608:8388608 umbradb/umbra:latest
```

To connect to umbra, you can use `ligpq` library on Linux.

## Nebula
Please follow the steps in the [official document](https://docs.nebula-graph.io/3.8.0/2.quick-start/2.install-nebula-graph/).

You should also install the official connector from [this page](https://docs.nebula-graph.io/3.8.0/14.client/3.nebula-cpp-client/)

