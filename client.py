from tuplespace import tuple_space
from multiprocessing.dummy import Pool

j = 29
idents = []
tuples = []
for i in range(j):
    idents.append(i)
    tuples.append(tuple(range(i, 3+i)))

pool = Pool(5)
client = tuple_space([1257, 1258], [(0, 9), (10, 19)])
if client == None:
    raise "TupleSpaceError"

print(client.new_server(1260, (20, 29)))

results = pool.starmap(client.push, list(zip(idents, tuples)))
print("PUSHED: ", results, "\n")

idents = list(range(0, j, 2))
results = pool.map(client.pop, idents)
print("POPED:\n", idents, "\n", results, "\n")

print("STOP: ", client.stop())


"""
for i in range(3):
    resp = client.push(i, (1,i))
    print(resp)

for i in range(3):
    resp = client.pop(i)
    print(resp)

for i in range(3):
    resp = client.new_server(1238 + i, (10*i, 11*i))
    print(resp)

print(client.stop())
"""
