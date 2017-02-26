from random import sample
from socket import *
import _pickle as pickle
from subprocess import Popen, CREATE_NEW_CONSOLE, CalledProcessError, PIPE

def create_server(port, next_port, tp_size):
    try:
        Popen('.\server.py ' + str(port) + ' ' + str(next_port) + ' ' + str(tp_size[0]) + ' ' + str(tp_size[1]), stdout=PIPE, stderr=PIPE, shell=True, creationflags=CREATE_NEW_CONSOLE)
    except CalledProcessError:
        return False
    return True

class Client():
    def __init__(self, port_list):
        self.serv_port_list = port_list
        

    def send_request(self, req, port=None):
        if port == None:
            port = sample(self.serv_port_list, 1)[0]
        s = socket(AF_INET, SOCK_STREAM)
        while True:
            try:
                s.connect(('localhost', port))
            except ConnectionRefusedError:
                pass
            else:
                req["first_server_port"] = port
                s.send(pickle.dumps(req))
                break
        
        msg = b""
        while True:
            tmp = s.recv(1024)
            if len(tmp) < 1024:
                break
            msg += tmp

        msg += tmp
        s.close()
        return pickle.loads(msg)

        
    def push(self, tup_id, tup):
        resp = self.send_request({ "command": "push", "id": tup_id, "tuple": tup })
        if resp["respond"] == "Ok":
            return resp["tuple"]
        else:
            return resp["respond"]


    def pop(self, tup_id):
        resp = self.send_request({ "command": "pop", "id": tup_id })
        if resp["respond"] == "Ok":
            return resp["tuple"]
        else:
            return resp["respond"]


    def copy(self, tup_id):
        resp = self.send_request({ "command": "copy", "id": tup_id })
        if resp["respond"] == "Ok":
            return resp["tuple"]
        else:
            return resp["respond"]


    def stop(self):
        resp = self.send_request({ "command": "stop" })
        return resp["respond"]


    def new_server(self, port, tup_int):
        if create_server(port, self.serv_port_list[0], tup_int):
            resp = self.send_request({ "command": "change_neighbor_port", "neighbor_port": port }, self.serv_port_list[-1])
            self.serv_port_list.append(port)
            return (resp["respond"], len(self.serv_port_list))
        return ("Can't create new server", len(self.serv_port_list))



def tuple_space(port_list, tuple_int_list):
    length = len(port_list)
    if length < 1 or length != len(tuple_int_list):
        return None

    for i in range(length):
        if not create_server(port_list[i], port_list[(i + 1) % length], tuple_int_list[i]):
            return None

    return Client(port_list)
