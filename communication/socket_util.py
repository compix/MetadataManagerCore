import socket
import json
import struct
from concurrent.futures import ThreadPoolExecutor
import time
import logging

logger = logging.getLogger(__name__)

def readBlob(sock, size):
    chunks = []
    bytes_recd = 0
    while bytes_recd < size:
        chunk = sock.recv(min(size - bytes_recd, 2048))
        if chunk == b'':
            raise RuntimeError("Socket closed.")

        chunks.append(chunk)
        bytes_recd = bytes_recd + len(chunk)

    return b''.join(chunks)

def readSize(sock):
    size = struct.calcsize("L")
    data = readBlob(sock, size)
    return struct.unpack("L", data)[0]

def recvDict(sock):
    size = readSize(sock)
    jsonBlob = readBlob(sock, size)
    jdict = json.loads(jsonBlob, encoding="utf-8")
    return jdict

def sendDict(sock, theDict : dict):
    jsonBlob = json.dumps(theDict).encode(encoding="utf-8")
    sock.sendall(struct.pack("L", len(jsonBlob)))
    sock.sendall(jsonBlob)

class JsonSocket(object):
    def __init__(self, timeout=None):
        super().__init__()

        self.sock : socket = None
        self.running = True
        self.timeout = timeout

    def connectClient(self, port, host = None):
        """
        Tries to connect once. Raises an exception if the connection fails.
        If the host is None, socket.gethostname() is used.
        """
        if host == None:
            host = socket.gethostname()

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))
        self.sock.settimeout(self.timeout)

    def connectClientInsistently(self, port, host = None):
        """
        Tries to repeatedly connect until a connection is established.
        If the connection times out a socket.timeout exception is raised.
        If the host is None, socket.gethostname() is used.

        Note: The alternative "socket.create_connection((host, port))" is very slow.
        """
        if host == None:
            host = socket.gethostname()

        connected = False
        tStart = time.time()
        while not connected and (self.timeout == None or (time.time() - tStart) < self.timeout):
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((host, port))
                self.sock.settimeout(self.timeout)
                connected = True
            except:
                pass

        if not connected:
            raise socket.timeout(f"Could not connect to ({host},{port}). Timeout.")

    def connectServer(self, port, host = None, numConnections=1):
        self.numConnections = numConnections
        if host == None:
            host = socket.gethostname()

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.bind((host, port))
        self.sock.listen(numConnections)

    def runServer(self):
        """
        If the host is None, socket.gethostname() is used.
        """
        with ThreadPoolExecutor(max_workers=self.numConnections) as executor:
            while self.running:
                clientSocket, address = self.sock.accept()
                executor.submit(self.processClientSocket, clientSocket, address)
    
    def handleClientSocket(self, clientSocket : socket.socket, address, dataDictionary : dict):
        """
        Executed on a different thread. This function is meant to be overriden.
        The client socket will be closed automatically after the end of this function.
        """
        print(f"Handling client socket with address {address}")
        print(dataDictionary)

    def processClientSocket(self, clientSocket, address):
        dataDictionary = recvDict(clientSocket)
        self.handleClientSocket(clientSocket, address, dataDictionary)
        clientSocket.close()

    def close(self):
        self.running = False
        self.sock.close()

if __name__ == "__main__":
    serverSocket = JsonSocket()
    serverSocket.connectServer(5000)

    def runServer():
        time.sleep(1)
        serverSocket.runServer()

    g_executor = ThreadPoolExecutor(max_workers=1)
    g_executor.submit(runServer)

    g_clientSocket = JsonSocket(timeout=10.0)
    g_clientSocket.connectClientInsistently(port=5000)

    testDict = {"Key":"Val", "Key2":"Val2"}
    #sendDict(g_clientSocket.sock, testDict)
    g_clientSocket.sock.sendall(b'Invalid message.')
    g_clientSocket.close()

    g_clientSocket = JsonSocket(timeout=10.0)
    g_clientSocket.connectClientInsistently(port=5000)
    testDict = {"Key3":"Val3", "Key4":"Val4"}
    sendDict(g_clientSocket.sock, testDict)
    g_clientSocket.close()

    time.sleep(2)

    serverSocket.close()
    g_executor.shutdown()    