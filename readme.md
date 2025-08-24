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

3. Build test
```bash
cd duckdb
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j10
```

4. Run test:

```bash
cd duckdb
./fig9.sh
```

## Umbra
You can try using the Docker image:
```bash
docker run -v umbra-db:/var/db -p 5432:5432 --ulimit nofile=1048576:1048576 --ulimit memlock=8388608:8388608 umbradb/umbra:latest
```

To connect to umbra, you can use `ligpq` library on Linux:
```bash
sudo apt install libpq5
sudo apt install libpq-dev
```

Build test file:
```bash
cd umbra
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j10
```

Run test:
```bash
cd umbra
./load_data.sh
./fig9.sh
```

## Nebula
Please follow the steps in the [official document](https://docs.nebula-graph.io/3.8.0/2.quick-start/2.install-nebula-graph/):

```bash
# install nebula db
wget https://oss-cdn.nebula-graph.io/package/3.8.0/nebula-graph-3.8.0.ubuntu2004.amd64.deb
sudo dpkg -i nebula-graph-3.8.0.ubuntu2004.amd64.deb
# start db
sudo /usr/local/nebula/scripts/nebula.service start all
sudo /usr/local/nebula/scripts/nebula.service start storaged
sudo /usr/local/nebula/scripts/nebula.service status all
# install client
wget https://github.com/vesoft-inc/nebula-console/releases/download/v3.8.0/nebula-console-linux-amd64-v3.8.0
mv nebula-console-linux-amd64-v3.8.0 nebula-console
chmod 111 nebula-console
```
You can try running `./nebula-console` to check if `Nebula` is running.

You should also install the official connector from [this page](https://docs.nebula-graph.io/3.8.0/14.client/3.nebula-cpp-client/):
```bash
cd $HOME
git clone https://github.com/vesoft-inc/nebula-cpp.git
cd nebula-cpp && mkdir build && cd build
cmake ..
make -j10 && sudo make install
sudo cp -r /usr/local/nebula/include /usr/local/include/nebula
```

## GridGraph
Build GridGraph
```bash
cd gridgrph
mkdir -p bin
mkdir -p data
make -j10
```

Run Test
```bash
cd gridgraph
./tab6.sh
```