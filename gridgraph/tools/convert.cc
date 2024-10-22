#include <fstream>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>

int main(int argc, char** argv) {
  std::fstream file("/home/junfeng/cit-Patents/cit-Patents.e");
  int fd = open("../data/citpatents", O_WRONLY | O_CREAT);
  int edge_count = 0;
  int c = 1;
  int64_t write_bytes = 0;
  while (!file.eof()) {
    int32_t src, dst;
    file >> src >> dst;
    edge_count++;
    write_bytes += write(fd, &src, sizeof(src));
    write_bytes += write(fd, &dst, sizeof(dst));
  }
  std::cout << "write bytes: " << write_bytes << std::endl;
  std::cout << "edge count: " << edge_count << std::endl;
  close(fd);
  return 0;
}