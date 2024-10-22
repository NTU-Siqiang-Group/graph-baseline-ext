import struct
with open('/home/junfeng/cit-Patents/cit-Patents.v', 'r') as f:
  vs = {}
  id = 1
  for line in f:
    vs[int(line.split()[0])] = id
    id += 1
  print(f'max id: {id}')
  with open('/home/junfeng/cit-Patents/cit-Patents.e', 'r') as f:
    with open('../data/citpatents', 'wb+') as f1:
      for line in f:
        u, v = map(int, line.split())
        uid = vs[u]
        vid = vs[v]
        f1.write(struct.pack('<l', int(uid)))
        f1.write(struct.pack('<l', int(vid)))