from socket import *
import sys
import _pickle as pickle
from concurrent.futures import ThreadPoolExecutor as pool

port = int(sys.argv[1])
neighbor_port = int(sys.argv[2])
tuple_min = int(sys.argv[3])
tuple_max = int(sys.argv[4])

tuple_space = {}

def push(tp_id, tp):
    tuple_space[tp_id] = tp
    return tp


def pop(tp_id):
    try:
        tup = tuple_space.pop(tp_id)
    except KeyError:
        return None
    return tup


def copy(tp_id):
    try:
        tup = tuple_space.get(tp_id)
    except KeyError:
        return None
    return tup


def get_message(sock):
    msg = b""
    while True:
        try:
            tmp = sock.recv(1024)
        except:
            break
        if len(tmp) < 1024:
            break
        msg += tmp
    msg += tmp
    req = pickle.loads(msg)
    return req
    

def ask_next_server(req):
    serv = socket(AF_INET, SOCK_STREAM)
    while True:
        try:
            serv.connect(('localhost', neighbor_port))
        except ConnectionRefusedError:
            pass
        else:
            break
    serv.send(pickle.dumps(req))
    resp = get_message(serv)
    serv.close()
    return resp

def worker(client):
    global neighbor_port, s
    req = get_message(client)
    if req["command"] == "change_neighbor_port":
        neighbor_port = req["neighbor_port"]
        resp = { "respond": "Ok" }
        
    elif req["command"] == "stop":
        try:
            s.close()
        except:
            resp = { "respond": "CloseSocketError" }
        else:
            resp = { "respond": "Ok" }
  
    elif req["id"] < tuple_min or req["id"] > tuple_max:
        if req["first_server_port"] == neighbor_port:
            resp = { "respond": "OutOfRange" }
        else:
            resp = ask_next_server(req)
            
    else:
        if req["command"] == "push":
            resp_tp = push(req["id"], req["tuple"])
        elif req["command"] == "pop":
            resp_tp = pop(req["id"])
        elif req["command"] == "copy":
            resp_tp = copy(req["id"])
        resp = { "respond": "Ok", "id": req["id"], "tuple": resp_tp }
        
    try:
        client.send(pickle.dumps(resp))
        client.close()
    except:
        pass



s = socket(AF_INET, SOCK_STREAM)
s.bind(('', port))
s.listen(1)

with pool(10) as p :
    while True:
        try:
            client_s, client_addr = s.accept()
        except:
            break
        p.submit(worker, client_s)

s.close()
