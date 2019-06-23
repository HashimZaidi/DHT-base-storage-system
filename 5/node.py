import socket
import sys
import os
import thread
import cPickle as pickle
import time

class node:
    def __init__(self,p):
        self.ip = '127.0.0.1'
        self.port = p
        self.id = hash((self.ip,self.port)) % 32
        self.successor = self
        self.predecessor = None

    def find_successor(self, id):
        n = self.find_predecessor(id)
        return n.successor

    def find_predecessor(self, id):
        n = self
        if n.id < n.successor.id:
            interval = [key for key in range(n.id+1,n.successor.id+1)]
        elif n.id>=n.successor.id:
            interval = [key%32 for key in range(n.id+1,n.successor.id+1+32)]

        while id not in interval:
            n = n.predecessor
            if n.id < n.successor.id:
                interval = [key for key in range(n.id + 1, n.successor.id + 1)]
            else:
                interval = [key % 32 for key in range(n.id + 1, n.successor.id + 1 + 32)]
        return n

    def __getstate__(self):
        odict = self.__dict__.copy()
        return odict
    def __setstate__(self, state):
        self.__dict__.update(state)


def get_size(s):
    return len(s)


def send_all_files(Node):
    c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    c.connect((Node.successor.ip, Node.successor.port))
    c.send('getFiles')
    time.sleep(0.5)
    for fileName in os.listdir(os.getcwd()):
        if fileName == 'node.py' or fileName == 'Thumbs.db':
            continue
        c.send(fileName)
        time.sleep(0.5)
        send_file(c, fileName)
        c.recv(1024)
        os.remove(fileName)
    c.send('done')
    time.sleep(0.5)
    c.send('disconnect')
    c.close()


def send_file(sock, fileName):
    print 'sending', fileName,hash(fileName)%32
    sendFile = open(fileName, "rb")
    sRead = sendFile.read(1024)
    while sRead:
        sock.send(sRead)
        sRead = sendFile.read(1024)
    sendFile.close()
    sock.send("'''D'''")


def get_file(sock, fileName):
    print 'receiving', fileName,hash(fileName)%32
    rData = sock.recv(1024)
    recFile = open(fileName, 'wb')
    while rData:
        if "'''D'''" in rData:
            recFile.write(rData[:-7])
            break
        recFile.write(rData)
        rData = sock.recv(1024)
    recFile.close()


def send_node(client,Node):
    temp = pickle.dumps(Node,2)
    size = get_size(temp)
    client.send(str(size))
    client.recv(1024)
    client.send(temp)
    client.recv(1024)


def recv_node(client):
    str_size = client.recv(1024)
    size = int(str_size)
    client.send('ack')
    if size < 1024:
        temp = client.recv(1024)
    else:
        temp = client.recv(size+1024)
    cp = pickle.loads(temp)
    client.send('ack')
    return cp


def input(dict, Node):
    while True:
        val = raw_input()
        if val == 'exit':
            break
        if val == 'download':
            fileName = raw_input('Enter file name: ')
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            c.connect(('127.0.0.1', Node.successor.port))
            c.send('findSuccessor')
            time.sleep(0.5)
            id = hash(fileName)%32
            c.send(str(id))
            n = recv_node(c)
            c.send('disconnect')
            c.close()
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            c.connect((n.ip, n.port))
            c.send('sendFile')
            time.sleep(0.5)
            c.send(fileName)
            msg = c.recv(1024)
            if msg == 'nack':
                print 'file does not exist'
            else:
                get_file(c,fileName)
                print 'file:',fileName,'downloaded from Node', n.id

    if dict['Alone']:
        dict['ShutDown'] = True
    elif Node.successor.id == Node.predecessor.id:
        Node.predecessor = None
    else:
        Node.predecessor = Node.successor
    dict['Leave'] = True

def leave_network(Node, status):
    print status, 'Network...'
    c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    c.connect((Node.ip, Node.port))
    c.send('disconnect')
    c.close()


def serverFunc(client, Node):
    while True:
        try:
            msg = client.recv(1024)
            if msg == 'disconnect':
                client.close()
                break
            elif msg == 'getCurrentState':
                send_node(client,Node)
            elif msg == 'getCurrentPredecessor':
                send_node(client,Node.predecessor)
            elif msg == 'findSuccessor':
                id = int(client.recv(1024))
                succ = Node.find_successor(id)
                send_node(client, succ)
            elif msg == 'ImYourPredecessor':
                Node.predecessor = recv_node(client)
                Node.predecessor.successor = Node
                print 'Updating Predecessor...'
                time.sleep(0.5)
                print 'Current Predecessor: ',Node.predecessor.id
            elif msg == 'sendFile':
                fileName = client.recv(1024)
                if fileName in os.listdir(os.getcwd()):
                    client.send('ack')
                else:
                    client.send('nack')
                    continue
                time.sleep(0.5)
                send_file(client,fileName)
            elif msg == 'sendFiles':
                for fileName in os.listdir(os.getcwd()):
                    if fileName == 'node.py' or fileName == 'Thumbs.db':
                        continue
                    key = hash(fileName)%32
                    if Node.id < Node.predecessor.id:
                        interval = [k for k in range(Node.id + 1, Node.predecessor.id + 1)]
                    else:
                        interval = [k % 32 for k in range(Node.id + 1, Node.predecessor.id + 1 + 32)]
                    if key in interval:
                        client.send(fileName)
                        time.sleep(0.5)
                        send_file(client,fileName)
                        client.recv(1024)
                        os.remove(fileName)
                client.send('done')
            elif msg == 'getFiles':
                while True:
                    fileName = client.recv(1024)
                    if fileName == 'done':
                        break
                    get_file(client, fileName)
                    client.send('ack')
        except socket.error:
            client.close()
            break


def clientFunc(Node, dict, port=None):
    thread.start_new_thread(input, (dict, Node))
    if port == None:
        print 'Waiting for connections...'
        while Node.predecessor == None:
            if dict['ShutDown']:
                leave_network(Node,'Shutting Down')
                return
        time.sleep(0.5)
        Node.successor = Node.predecessor
        Node.successor.predecessor = Node
        dict['Alone'] = False
    else:
        c = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        c.connect(('127.0.0.1',port))
        c.send('findSuccessor')
        time.sleep(0.5)
        c.send(str(Node.id))
        Node.successor = recv_node(c)
        Node.successor.predecessor = Node
        c.send('disconnect')
        c.close()

    while True:
        c = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        c.connect((Node.successor.ip, Node.successor.port))
        c.send('ImYourPredecessor')
        time.sleep(0.5)
        send_node(c,Node)

        print 'Updating Successor...'
        time.sleep(0.5)
        print 'Current Successor: ', Node.successor.id

        c.send('sendFiles')
        while True:
            fileName = c.recv(1024)
            if fileName == 'done':
                break
            get_file(c,fileName)
            c.send('ack')

        print 'done receiving files'
        dict['updateSuccessor'] = False
        counter = 3
        while not dict['Leave']:
            time.sleep(1)
            while counter > 0:
                try:
                    c.send('getCurrentPredecessor')
                    break
                except:
                    counter -= 1
                    print 'Error connecting to successor.'
                    if counter > 0:
                        print 'Reconnecting...'
                        time.sleep(0.5)

            if counter == 0:
                if Node.id == Node.successor.successor.id:
                    Node.predecessor = None
                    Node.successor = node(Node.port)
                    dict['Alone'] = True
                    dict['updateSuccessor'] = True
                else:
                    Node.successor = Node.successor.successor
                    Node.successor.predecessor = Node
                    dict['updateSuccessor'] = True
                c.close()
                break

            cp = recv_node(c)

            if not dict['Leave'] and cp == None:
                Node.predecessor = cp
                Node.successor = node(Node.port)
                dict['Alone'] = True
                dict['updateSuccessor'] = True
                break
            elif not dict['Leave'] and cp.id != Node.id:
                Node.successor = cp
                Node.successor.predecessor = Node
                dict['updateSuccessor'] = True
                break
            elif not dict['Leave']:
                c.send('getCurrentState')
                s = recv_node(c)
                if Node.successor.successor.id != s.successor.id:
                    Node.successor = s

        try:
            c.send('disconnect')
        except socket.error:
            pass
        c.close()
        if dict['Leave']:
            send_all_files(Node)
            leave_network(Node, 'Leaving')
            break
        if dict['Alone']:
            print 'All other nodes left the network.'
            print 'Waiting for connections...'
            while Node.predecessor == None:
                if dict['ShutDown']:
                    leave_network(Node, 'Shutting Down')
                    return
            time.sleep(0.5)
            Node.successor = Node.predecessor
            Node.successor.predecessor = Node
            dict['Alone'] = False


def Main():
    n = node(8000)
    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((n.ip, n.port))
    s.listen(10)
    dict = {'Leave': False, 'updateSuccessor': False, 'Alone':False, 'ShutDown': False}

    print '******* NODE:',n.id,'*******\n'
    choice = raw_input('Enter 1 to start a new Network\n' \
                        'Enter 2 to join a Network\n')

    if choice == '1':
        print 'Creating a new Network...'
        dict['Alone'] = True
        thread.start_new_thread(clientFunc,(n,dict))
        print 'Done'
    elif choice == '2':
        p = raw_input('Enter the port number to' \
                    ' join a Network: ')
        print 'Joining the Network...'
        thread.start_new_thread(clientFunc,(n,dict,int(p)))
        print 'Done'

    while not dict['Leave']:
        client, addr = s.accept()
        thread.start_new_thread(serverFunc,(client, n))

    time.sleep(1)
    s.close()

if __name__=="__main__":
    Main()