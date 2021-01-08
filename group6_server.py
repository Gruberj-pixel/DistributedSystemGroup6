import socket
import threading
import time
from datetime import datetime
import struct
import json
#import concurrent
#import pickle
#import uuid

# Broadcast IP Fritzbox 192.168.178.255
# Broadcast_IP = "192.168.178.255"
MULTICAST_GROUP_IP = '224.1.1.1'

# Ports
MULTICAST_PORT_SERVER = 5000 # Port for server to serve Multicast
UNICAST_PORT_SERVER = 6000 # Port for server to server Unicast --> TCP ONLY
MULTICAST_PORT_CLIENT = 7000 # Port for clients to discover servers

CLIENT_CONNECTION_TO_LEADER_PORT = 9000

SERVER_CLIENTLIST_PORT = 5100
SERVER_HEARTBEAT_PORT = 5200
SERVER_MESSAGELIST_PORT = 5300
SERVER_LEADER_ELECTION_PORT = 5400
SERVER_NEW_LEADER_PORT = 5500
SERVER_SERVERLIST_PORT = 5600
SERVER_REPLICA_PORT = 5700
SERVER_CLIENTUPDATE_PORT = 5900


# Localhost information
MY_HOST = socket.gethostname()
MY_IP = socket.gethostbyname(MY_HOST)
# create a ID unique based on host ID and current time
# MY_ID = uuid.uuid1()
buffer_size = 1024

class Server():
    def __init__(self):
        self.isLeader = False # variable to mark self as leader
        self.serverList = [] # list if servers and their addresses
        self.ringForming = False
        self.electionRunning = False # variable to mark an ongoing election
        self.sorted_ip_ring = []
        self.neighbour_left = '' # fix the IP of my neighbour
        #self.neighbour_right = ''
        self.heartbeat_neighbour = ''
        self.electionMessage = {} # variable that contains the LCR-Algorithm
        self.leader_IP = ''
        self.participant = False
        self.electionStarter = False
        self.heartbeatRunning = False
        self.heartbeatMessage = ''
        self.clientList = [] # list of clients and their addresses
        self.auctionList = {'smartphone': 0.00, 'laptop': 0.00, 'monitor': 0.00}
        self.informServer = False
        self.sendReplica = False
        #self.first = True # variable to check if server first started
    
    def printwt(self, msg):
        current_date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f'[{current_date_time}] {msg}')

    def findIndexOfElement(self, element, mylist):
        try:
            index = mylist.index(element)
            return index
        except ValueError:
            return None

    def MulticastListenAndReply(self):
        if MY_IP not in self.serverList:
            self.serverList.append(MY_IP)

        # create socket bind to server address
        multilis_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        multilis_sock.bind(('', MULTICAST_PORT_SERVER))

        # tell the os to add the socket to the multicast group
        group = socket.inet_aton(MULTICAST_GROUP_IP)
        mreg = struct.pack('4sL', group, socket.INADDR_ANY)
        multilis_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreg)
        
        while True:
            try:
                # receive data from other participants
                data, address = multilis_sock.recvfrom(buffer_size)
                #self.printwt('Waiting for other guys...')

                if data:
                    # if you have data decode the message
                    newServer_address = data.decode()
                    self.printwt(f'New participant wants to connect: {newServer_address}')
                    self.heartbeat_neighbour = ''
                    self.isLeader = False
                    self.heartbeatRunning = False
                    
                    # if the decoded address is not in the server list add it and print the list
                    if newServer_address not in self.serverList:
                        self.serverList.append(newServer_address)
                        self.ringForming = True
                        #self.printwt(f'{self.serverList}, start new ring formation')

                    reply_message = MY_IP
                    multilis_sock.sendto(str.encode(reply_message), address)
                    self.SendClientListUpdate(newServer_address)
                    self.informServer = True
                    self.SendStatusOfAuctions(newServer_address)
                    self.printwt('Replied my IP to new participant')
                    self.FormRing()

            except socket.timeout:
                # iam the only participant in the system, no election needed
                #print("time out, no response") 
                break

        time.sleep(1)

    
    def MulticastSendAndReceive(self):
        message = MY_IP
        multicast_group = (MULTICAST_GROUP_IP, MULTICAST_PORT_SERVER)
        multisend_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
        multisend_sock.settimeout(5)
        # set time to live message (network hps; 1 for local)
        ttl = struct.pack('b', 1)
        multisend_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
        self.electionStarter = True

        # send my IP to other participants
        multisend_sock.sendto(message.encode(), multicast_group)
        self.printwt("Sent my IP to server group")

        # if my IP is not in the server list add it
        if MY_IP not in self.serverList:
            self.serverList.append(MY_IP)
        
        maxLoop = 5
        currentLoop = 0
        
        while currentLoop < maxLoop:
            while True:     
                currentLoop += 1

                try:
                    # receive reply data from the other participants
                    reply, address = multisend_sock.recvfrom(1024)

                    if reply:
                        # decode received data
                        reply_address = reply.decode()
                        
                        # if reply address is not in the server list, add it
                        if reply_address not in self.serverList:
                            self.serverList.append(reply_address)

                except socket.timeout:
                    break

        if currentLoop == maxLoop:     
            #self.printwt(f'{maxLoop} loops are done, my groupview is: {self.serverList}')
            multisend_sock.close()
            time.sleep(1)
            self.ringForming = True
            self.FormRing()
            
    
    def FormRing(self):
        while self.ringForming == True:
            sorted_binary_ring = sorted([socket.inet_aton(member) for member in self.serverList])
            self.sorted_ip_ring = [socket.inet_ntoa(node) for node in sorted_binary_ring]
            #self.printwt(f'Sorted group view is: {self.sorted_ip_ring}')

            self.neighbour_left = self.GetNeighbour(self.sorted_ip_ring, MY_IP, direction = 'left')
            self.printwt(f'Sorted group view is: {self.sorted_ip_ring}, my left neighbour is {self.neighbour_left}')
            self.ringForming = False
            if self.electionStarter == True:
                self.electionRunning = True
                self.ElectionSend()
               
    def GetNeighbour(self, ring, current_node_ip, direction = 'left'):
        current_node_index = ring.index(current_node_ip) if current_node_ip in ring else -1
        if current_node_index != -1:
            if direction == 'left':
                if current_node_index + 1 == len(ring):
                    return ring [0]
                else:
                    return ring[current_node_index +1]
            else:
               if current_node_index == 0:
                    return ring[len(ring)-1]
               else:
                    return ring[current_node_index -1]
        else:
            return None

    def ElectionSend(self):
        while self.electionRunning == True:
            self.participant = False
            self.electionMessage = {'mid': MY_IP, 'isLeader': False}

            if len(self.sorted_ip_ring) == 1:
                self.isLeader = True
                self.electionRunning = False
                self.electionStarter = False
                self.leader_IP = self.sorted_ip_ring[0]
                self.printwt(f'I am the only server in the system, so the leader IP is {self.leader_IP}')
                self.SendLeaderIPUpdate()

            elif len(self.sorted_ip_ring) > 1:
                self.neighbour_left = self.GetNeighbour(self.sorted_ip_ring, MY_IP, direction = 'left')
                self.printwt(f'Start Election')
                self.electionRunning = False
                self.participant = True
                election_message = json.dumps(self.electionMessage)
                electionsend_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

                try:
                    electionsend_socket.sendto(election_message.encode(), (self.neighbour_left, SERVER_LEADER_ELECTION_PORT))
                    #self.printwt(f'sent: {election_message} to {self.neighbour_left}')

                except socket.error as e: 
                    print(str(e))
                        
    def ElectionListenAndForward(self):
        electionlis_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        electionlis_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        electionlis_socket.bind(('', SERVER_LEADER_ELECTION_PORT))
        
        #self.printwt(f'listening to election messages at port {SERVER_LEADER_ELECTION_PORT}')

        while True:
            election_message, neighbour_right = electionlis_socket.recvfrom(buffer_size)
            election_message = json.loads(election_message.decode())
            #self.printwt(f'received election message {election_message} from my neighbour {neighbour_right}')
    
            if not self.neighbour_left:
                sorted_binary_ring = sorted([socket.inet_aton(member) for member in self.serverList])
                self.sorted_ip_ring = [socket.inet_ntoa(node) for node in sorted_binary_ring]
                self.neighbour_left = self.GetNeighbour(self.sorted_ip_ring, MY_IP, direction='left')

            if election_message:

                if election_message.get('isLeader') == True and self.participant:
                    self.leader_IP = election_message['mid']
                    self.printwt(f'Leader Election successfully executed, leader is: {self.leader_IP}')
                    self.participant = False
                    leader_message = json.dumps(election_message)
                    electionlis_socket.sendto(leader_message.encode(), (self.neighbour_left, SERVER_LEADER_ELECTION_PORT))
                    self.isLeader = True
                    self.electionStarter = False
                
                elif election_message.get('isLeader') == True and election_message['mid'] == MY_IP:
                    self.printwt('Leader Election finished, I am the leader and starting the heartbeat')
                    self.electionStarter = False
                    self.leader_IP = MY_IP
                    self.isLeader = True
                    self.heartbeatRunning = True
                    self.SendLeaderIPUpdate()
                    time.sleep(3)
                    self.HeartbeatSend()

                if election_message['mid'] < MY_IP and not self.participant:    
                    new_election_message = {'mid': MY_IP, 'isLeader': False }
                    self.participant = True 
                    new_election_message = json.dumps(new_election_message)
                    electionlis_socket.sendto(new_election_message.encode(), (self.neighbour_left, SERVER_LEADER_ELECTION_PORT))
                    #self.printwt(f'forwarded message: {new_election_message}')

                elif election_message['mid'] > MY_IP:
                    if election_message.get('isLeader') == False:
                        self.participant = True 
                        election_message = json.dumps(election_message)
                        electionlis_socket.sendto(election_message.encode(), (self.neighbour_left, SERVER_LEADER_ELECTION_PORT))
                        #self.printwt(f'forwarded message: {election_message}')

                elif election_message['mid'] == MY_IP and self.participant:
                    #self.leader_IP = MY_IP
                    new_election_message = {'mid': MY_IP, 'isLeader': True }
                    new_election_message = json.dumps(new_election_message)
                    self.participant = False 
                    electionlis_socket.sendto(new_election_message.encode(), (self.neighbour_left, SERVER_LEADER_ELECTION_PORT))
                    #self.printwt(f'Forward Elected message: {new_election_message}')
                    #self.printwt(f'{self.leader_IP} IS THE LEADER!')
    
    def HeartbeatListen(self):
        heartbeatlis_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        heartbeatlis_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        heartbeatlis_socket.bind(('', SERVER_HEARTBEAT_PORT))
        heartbeatlis_socket.settimeout(4)

        while True:

            try:
                heartbeat, self.heartbeat_neighbour = heartbeatlis_socket.recvfrom(buffer_size)
                heartbeat_message = heartbeat.decode()

                if heartbeat:
                    self.printwt(f'Received {heartbeat_message} from {self.heartbeat_neighbour[0]}')
                    self.heartbeatRunning = True
                    time.sleep(1)
                    self.HeartbeatSend()
            
            except socket.timeout:

                if self.isLeader == False:
                    pass

                elif self.heartbeat_neighbour:
                    self.printwt(f'No Heartbeat Received')
                    dead_server = self.heartbeat_neighbour[0]   # variable speichert IP & Port, so zugriff auf IP
                    index = self.findIndexOfElement(dead_server, self.serverList)
                    self.serverList.pop(index)

                    for x in range(len(self.serverList)):
                        heartbeatlis_socket.sendto(dead_server.encode(),(self.serverList[x], SERVER_SERVERLIST_PORT))
                
                elif len(self.sorted_ip_ring) == 1:
                    pass

                else:
                    pass

    def HandleServerCrash(self):
        serverlist_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        serverlist_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        serverlist_socket.bind(('', SERVER_SERVERLIST_PORT))

        while True:
            update, address = serverlist_socket.recvfrom(buffer_size)
            dead_server = update.decode()
            #self.printwt(f'Received Update of serverList from {address}, crashed server is {dead_server}')

            if update:

                if dead_server in self.serverList:
                    index = self.findIndexOfElement(dead_server, self.serverList)
                    self.serverList.pop(index)

                if dead_server != self.leader_IP:
                    self.printwt(f'Backupserver {dead_server} crashed, updated serverList: {self.serverList}')
                    self.heartbeat_neighbour = ''
                    self.ringForming = True
                    self.FormRing() # da hier electionStarter nicht auf True gesetzt wird, wird keine election ausgelöst sondern nur form ring
                    time.sleep(2)
                    if MY_IP == self.leader_IP and len(self.serverList) > 1:
                        self.heartbeatRunning = True
                        self.HeartbeatSend()
                    elif MY_IP == self.leader_IP and len(self.serverList) == 1:
                        self.printwt('I am the only server in the system, sending no heartbeat')

                elif dead_server == self.leader_IP:
                    self.printwt(f'Leader {dead_server} crashed, updated serverList: {self.serverList}')
                    self.isLeader = False
                    self.electionStarter = True
                    self.heartbeat_neighbour = ''
                    self.ringForming = True
                    self.FormRing()
                    self.SendLeaderIPUpdate()

    def HeartbeatSend(self):
        while self.heartbeatRunning == True:
            self.heartbeatMessage = '*'

            heartbeatsend_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            heartbeat_message = self.heartbeatMessage

            try:
                heartbeatsend_socket.sendto(heartbeat_message.encode(), (self.neighbour_left, SERVER_HEARTBEAT_PORT))
                self.printwt(f'Sent {heartbeat_message} to {self.neighbour_left}')

            except socket.error as e: 
                print(str(e))
            
            self.heartbeatRunning = False
        
    def ListenForClientAndReply(self):
        # create socket bind to server address
        multilisClient_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        multilisClient_sock.bind(('', MULTICAST_PORT_CLIENT))

        # tell the os to add the socket to the multicast group
        group = socket.inet_aton(MULTICAST_GROUP_IP)
        mreg = struct.pack('4sL', group, socket.INADDR_ANY)
        multilisClient_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreg)
        
        while True:
            #try:
                # receive data from other participants
            data, address = multilisClient_sock.recvfrom(buffer_size)
            #self.printwt('Waiting for clients...')

            if data:
                # if you have data decode the message
                newClient_address = data.decode()
                self.printwt(f'New client wants to connect: {newClient_address}')
                
                # if the decoded address is not in the server list add it and print the list
                if newClient_address not in self.clientList:
                    self.clientList.append(newClient_address)
                    self.printwt(f'client list: {self.clientList}')

                if MY_IP == self.leader_IP:
                    reply_message = MY_IP
                    multilisClient_sock.sendto(str.encode(reply_message), address)
                    self.printwt('Replied my IP to new client')
                    self.SendAuctionList(newClient_address)

            #except socket.timeout:
                #break
            
    def SendAuctionList(self, client_address):
        if self.leader_IP == MY_IP:
            biddingplace_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            biddingplace_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            #biddingplace_sock.settimeout(5)

            auctionList = json.dumps(self.auctionList)
            biddingplace_sock.sendto(auctionList.encode(), (client_address, SERVER_MESSAGELIST_PORT))
            #self.printwt('sent auction list to client')

    def SendLeaderIPUpdate(self):
        if self.leader_IP == MY_IP:
            newLeaderIP_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            newLeaderIP_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            if self.clientList:
                for x in range(len(self.clientList)):
                    newLeaderIP_sock.sendto(self.leader_IP.encode(), (self.clientList[x], SERVER_NEW_LEADER_PORT))
       
    def SendClientListUpdate(self, newServerAddress):
        if self.leader_IP == MY_IP:
            clientListUpdateSend_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            clientListUpdateSend_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            if self.clientList:
                update_msg = json.dumps(self.clientList)
                self.printwt("Trying to send clientList to new server")
                clientListUpdateSend_sock.connect((newServerAddress, SERVER_CLIENTUPDATE_PORT))
                clientListUpdateSend_sock.send(update_msg.encode())

    def SendStatusOfAuctions(self, newServerAddress):
        if self.leader_IP == MY_IP:
            statusOfAuctions_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            statusOfAuctions_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            if self.informServer:
                self.informServer = False
                auctionUpdate_msg = json.dumps(self.auctionList)

                statusOfAuctions_sock.connect((newServerAddress, SERVER_REPLICA_PORT))
                statusOfAuctions_sock.send(auctionUpdate_msg.encode())

            if self.sendReplica:
                self.sendReplica = False
                replica_msg = json.dumps(self.auctionList)

                for x in range(len(self.serverList)):
                    if self.serverList[x] != self.leader_IP:
                        statusOfAuctions_sock.connect((self.serverList[x], SERVER_REPLICA_PORT))
                        statusOfAuctions_sock.send(replica_msg.encode())


    def ClientHandling(self):
        biddingplace_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        biddingplace_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        biddingplace_sock.bind(('', SERVER_MESSAGELIST_PORT))

        while True:

            if self.leader_IP == MY_IP: 

                self.printwt('Listening for client request...')
                #try:
                    # receive reply data from the other participants
                bid, address = biddingplace_sock.recvfrom(1024)

                if bid:
                    # decode received data
                    bidInformation = json.loads(bid.decode())
                    #self.printwt(f'Daten vom Client: {bidInformation}')

                    for i in bidInformation:
                        key = i
                        val = bidInformation[key]

                    if '1' in bidInformation and val > self.auctionList['smartphone']:
                        self.auctionList['smartphone'] = val
                        reply = 'You are the highest bidder'
                        biddingplace_sock.sendto(reply.encode(), address)
                    
                    elif '2' in bidInformation and val > self.auctionList['laptop']:
                        self.auctionList['laptop'] = bidInformation[key]
                        reply = 'You are the highest bidder'
                        biddingplace_sock.sendto(reply.encode(), address)
                    
                    elif '3' in bidInformation and val > self.auctionList['monitor']:
                        self.auctionList['monitor'] = bidInformation[key]
                        reply = 'You are the highest bidder'
                        biddingplace_sock.sendto(reply.encode(), address)
                    else:
                        reply = 'Sorry, you are not the highest bidder'
                        biddingplace_sock.sendto(reply.encode(), address)

                    self.printwt(self.auctionList)
                    self.SendAuctionList(address[0])
                    self.sendReplica = True
                    self.informServer = False
                    self.SendStatusOfAuctions(address[0])
                

                """ for x in range(len(self.serverList)):
                    if self.serverList[x] != self.leader_IP:
                        biddingplace_sock.sendto(replica_msg.encode(), (self.serverList[x], SERVER_REPLICA_PORT)) """
                
                #except socket.error as e:
                    #print(str(e))
                        
    def ListenForReplica(self):
        #self.printwt("Listening for replicas...")
        replicaLis_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        replicaLis_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        replicaLis_sock.bind(('', SERVER_REPLICA_PORT))
        replicaLis_sock.listen()

        while True:

            #try:
            replica, address = replicaLis_sock.accept()
            newAuctionList = replica.recv(buffer_size)

            if newAuctionList:
                newAuctionList = json.loads(newAuctionList.decode())
                self.printwt(f'Received Updated Auction List: {newAuctionList}')
                self.auctionList = newAuctionList

            #except socket.timeout:
                #break

    def ListenForClientListUpdates(self):
        clientUpdateLis_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientUpdateLis_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        clientUpdateLis_sock.bind(('', SERVER_CLIENTUPDATE_PORT))
        clientUpdateLis_sock.listen()

        while True:

            clientListUpdate, address = clientUpdateLis_sock.accept()
            newClientList = clientListUpdate.recv(buffer_size)

            if newClientList:
                newClientList = json.loads(newClientList.decode())
                self.printwt(f'Received client list: {newClientList}')
                self.clientList = newClientList



if __name__== '__main__':
    server = Server()

    thread1 = threading.Thread(target = server.MulticastListenAndReply)
    thread1.start()

    thread2 = threading.Thread(target = server.MulticastSendAndReceive)
    thread2.start()

    thread3 = threading.Thread(target = server.ElectionListenAndForward)
    thread3.start()

    thread4 = threading.Thread(target = server.HeartbeatListen)
    thread4.start()

    thread5 = threading.Thread(target = server.HandleServerCrash)
    thread5.start()

    thread6 = threading.Thread(target = server.ListenForClientAndReply)
    thread6.start()

    thread7 = threading.Thread(target = server.ClientHandling)
    thread7.start()

    thread8 = threading.Thread(target = server.ListenForReplica)
    thread8.start()

    thread9 = threading.Thread(target = server.ListenForClientListUpdates)
    thread9.start()