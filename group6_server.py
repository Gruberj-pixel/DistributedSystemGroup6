import socket
import threading
import time
from datetime import datetime
import struct
import json

MULTICAST_GROUP_IP = '224.1.1.1'

# Ports
SERVER_MULTICAST_PORT = 5000 # Port for server to discover server 
CLIENT_MULTICAST_PORT = 7000 # Port for clients to discover servers
SERVER_HEARTBEAT_PORT = 5200 # Port for sending hearbeat messages
CLIENT_LEADER_COMMUNICATION_PORT = 5300 # port for client-leader communication
SERVER_LEADER_ELECTION_PORT = 5400  # Port for sending leader election messages
SERVER_NEW_LEADER_PORT = 5500 # port for sending election messages
SERVER_SERVERLIST_PORT = 5600 # port for sending serverlist updates
SERVER_REPLICA_PORT = 5700 # port for sending replicas
SERVER_CLIENTLIST_PORT = 5900 # port for sending client list updates

# Localhost information
MY_HOST = socket.gethostname()
MY_IP = socket.gethostbyname(MY_HOST)
buffer_size = 1024

class Server():
    def __init__(self):
        self.isLeader = False # variable to mark self as leader
        self.serverList = [] # list if servers and their addresses
        self.ringForming = False # variable to control ring formation function
        self.electionRunning = False # variable to mark an ongoing election
        self.sorted_ip_ring = [] # list of servers, sorted
        self.neighbour_left = '' # fix the IP of left neighbour
        self.heartbeat_neighbour = '' # fix the IP of right neighbour to identify server crash
        self.electionMessage = {} # variable that contains the LCR-Algorithm
        self.leader_IP = '' # fix the leader IP
        self.participant = False # type of participant in leader election
        self.electionStarter = False # variable to control leader election function
        self.heartbeatRunning = False # variable to control heartbeat function
        self.heartbeatMessage = '' # fix the heartbeat message
        self.clientList = [] # list of clients and their addresses
        self.auctionList = {'smartphone': 0.00, 'laptop': 0.00, 'monitor': 0.00} # example auction list to place bids on
        self.informServer = False # 
        self.sendReplica = False # variable to control replication function
    
    # print the current date and time
    def printwt(self, msg):
        current_date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f'[{current_date_time}] {msg}')

    # identify the server list index of an IP
    def findIndexOfElement(self, element, mylist):
        try:
            index = mylist.index(element)
            return index
        except ValueError:
            return None

    def MulticastListenAndReply(self):
        # if my IP is not in the server list add it
        if MY_IP not in self.serverList:
            self.serverList.append(MY_IP)

        # create socket bind to server address
        multilis_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        multilis_sock.bind(('', SERVER_MULTICAST_PORT))

        # tell the os to add the socket to the multicast group
        group = socket.inet_aton(MULTICAST_GROUP_IP)
        mreg = struct.pack('4sL', group, socket.INADDR_ANY)
        multilis_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreg)
        
        while True:
            try:
                # receive data from other participants
                data, address = multilis_sock.recvfrom(buffer_size)

                if data:
                    # if you have data than decode
                    newServer_address = data.decode()
                    self.printwt(f'New participant wants to connect: {newServer_address}')
                    self.heartbeat_neighbour = ''
                    self.isLeader = False
                    self.heartbeatRunning = False
                    
                    # if the decoded address is not in the server list add it and print the list
                    if newServer_address not in self.serverList:
                        self.serverList.append(newServer_address)
                        self.ringForming = True

                    # reply my IP, send current client list to new server and start ring formation function
                    reply_message = MY_IP
                    multilis_sock.sendto(str.encode(reply_message), address)
                    self.SendClientListUpdate(newServer_address)
                    self.informServer = True
                    self.SendStatusOfAuctions(newServer_address)
                    self.printwt('Replied my IP to new participant')
                    self.FormRing()

            except socket.timeout:
                break

        time.sleep(1)

    def MulticastSendAndReceive(self):
        # create socket
        multicast_group = (MULTICAST_GROUP_IP, SERVER_MULTICAST_PORT)
        multisend_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
        multisend_sock.settimeout(2)

        # set time to live message (network hps; 1 for local)
        ttl = struct.pack('b', 1)
        multisend_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
        self.electionStarter = True

        # send my IP to other participants
        message = MY_IP
        multisend_sock.sendto(message.encode(), multicast_group)
        self.printwt("Sent my IP to server group")

        # if my IP is not in the server list add it
        if MY_IP not in self.serverList:
            self.serverList.append(MY_IP)
        
        # listen for IPs from existing servers
        maxLoop = 5
        currentLoop = 0
        
        while currentLoop < maxLoop:
            while True:     
                currentLoop += 1

                try:
                    # receive reply data from the other participants
                    reply, address = multisend_sock.recvfrom(buffer_size)

                    if reply:
                        # decode received data
                        reply_address = reply.decode()
                        
                        # if reply address is not in the server list, add it
                        if reply_address not in self.serverList:
                            self.serverList.append(reply_address)

                except socket.timeout:
                    break
        
        # after collecting IPs start ring formation
        if currentLoop == maxLoop:     
            multisend_sock.close()
            time.sleep(1)
            self.ringForming = True
            self.FormRing()
            
    
    def FormRing(self):
        # sort the IPs in the server list
        while self.ringForming == True:
            sorted_binary_ring = sorted([socket.inet_aton(member) for member in self.serverList])
            self.sorted_ip_ring = [socket.inet_ntoa(node) for node in sorted_binary_ring]

            # find out the left neighbour
            self.neighbour_left = self.GetNeighbour(self.sorted_ip_ring, MY_IP, direction = 'left')
            self.printwt(f'Sorted group view is: {self.sorted_ip_ring}, my left neighbour is {self.neighbour_left}')
            self.ringForming = False

            # if Iam the new participant, start the leader election
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

            # if the sorted server list has just one Index, I mark myself as the leader
            if len(self.sorted_ip_ring) == 1:
                self.isLeader = True
                self.electionRunning = False
                self.electionStarter = False
                self.leader_IP = self.sorted_ip_ring[0]
                self.printwt(f'I am the only server in the system, so the leader IP is {self.leader_IP}')
                self.SendLeaderIPUpdate()

            # if the sored server list has more than one index, create a socket and send an election message
            elif len(self.sorted_ip_ring) > 1:
                self.neighbour_left = self.GetNeighbour(self.sorted_ip_ring, MY_IP, direction = 'left')
                self.printwt(f'Start Election')
                self.electionRunning = False
                self.participant = True
                election_message = json.dumps(self.electionMessage)
                electionsend_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

                try:
                    electionsend_socket.sendto(election_message.encode(), (self.neighbour_left, SERVER_LEADER_ELECTION_PORT))

                except socket.error as e: 
                    print(str(e))
                        
    def ElectionListenAndForward(self):
        # create socket
        electionlis_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        electionlis_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        electionlis_socket.bind(('', SERVER_LEADER_ELECTION_PORT))

        while True:
            # listen for election messages
            election_message, neighbour_right = electionlis_socket.recvfrom(buffer_size)
            election_message = json.loads(election_message.decode())

            # if the left neighbour is not identified, do it here agein (this is coded because we had some coordination issues of threats)
            if not self.neighbour_left:
                sorted_binary_ring = sorted([socket.inet_aton(member) for member in self.serverList])
                self.sorted_ip_ring = [socket.inet_ntoa(node) for node in sorted_binary_ring]
                self.neighbour_left = self.GetNeighbour(self.sorted_ip_ring, MY_IP, direction='left')

            # if you receive an election message, compare the received IP with your IP and/or also the leader status
            if election_message:

                # if the election message contains that a leader is elected and Iam already marked as a participant
                # save the leader IP, mark myself as a non-participant and forward the existing election message
                if election_message.get('isLeader') == True and self.participant:
                    self.leader_IP = election_message['mid']
                    self.printwt(f'Leader Election successfully executed, leader is: {self.leader_IP}')
                    self.participant = False
                    leader_message = json.dumps(election_message)
                    electionlis_socket.sendto(leader_message.encode(), (self.neighbour_left, SERVER_LEADER_ELECTION_PORT))
                    self.isLeader = True
                    self.electionStarter = False
                
                # if the election message contains that a leader is elected and the IP in the election message is equal to my IP
                # save my IP as the leader IP and start the heartbeat procedure
                elif election_message.get('isLeader') == True and election_message['mid'] == MY_IP:
                    self.printwt('Leader Election finished, I am the leader and starting the heartbeat')
                    self.electionStarter = False
                    self.leader_IP = MY_IP
                    self.isLeader = True
                    self.heartbeatRunning = True
                    self.SendLeaderIPUpdate()
                    time.sleep(3)
                    self.HeartbeatSend()

                # if the IP in the election message is smaller than my IP and Iam already not a participant
                # mark myself as a participant and forward my IP to my neighbour.
                if election_message['mid'] < MY_IP and not self.participant:    
                    new_election_message = {'mid': MY_IP, 'isLeader': False }
                    self.participant = True 
                    new_election_message = json.dumps(new_election_message)
                    electionlis_socket.sendto(new_election_message.encode(), (self.neighbour_left, SERVER_LEADER_ELECTION_PORT))

                # if the IP in the election message is bigger than my IP and a leader is already not elected
                # forward the existing election message
                elif election_message['mid'] > MY_IP:
                    if election_message.get('isLeader') == False:
                        self.participant = True 
                        election_message = json.dumps(election_message)
                        electionlis_socket.sendto(election_message.encode(), (self.neighbour_left, SERVER_LEADER_ELECTION_PORT))

                # if the IP in the election message equal to my IP and Iam already a participant
                # mark myself as a non-participant, forward my IP to my neighbour and also mark in the election message that a leader is elected
                elif election_message['mid'] == MY_IP and self.participant:
                    new_election_message = {'mid': MY_IP, 'isLeader': True }
                    new_election_message = json.dumps(new_election_message)
                    self.participant = False 
                    electionlis_socket.sendto(new_election_message.encode(), (self.neighbour_left, SERVER_LEADER_ELECTION_PORT))
    
    def HeartbeatListen(self):
        # create socket
        heartbeatlis_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        heartbeatlis_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        heartbeatlis_socket.bind(('', SERVER_HEARTBEAT_PORT))
        heartbeatlis_socket.settimeout(4)

        while True:

            # receive hearbeat message and decode it
            try:
                heartbeat, self.heartbeat_neighbour = heartbeatlis_socket.recvfrom(buffer_size)
                heartbeat_message = heartbeat.decode()

                # if I received a message, forward it
                if heartbeat:
                    self.printwt(f'Received {heartbeat_message} from {self.heartbeat_neighbour[0]}')
                    self.heartbeatRunning = True
                    time.sleep(1)
                    self.HeartbeatSend()
            
            except socket.timeout:
                # do not wait for heartbeats when no leader is elected
                if self.isLeader == False:
                    pass

                # if no heartbeat received, delete crashed server IP from the server list and inform all existing servers
                elif self.heartbeat_neighbour:
                    self.printwt(f'No Heartbeat Received')
                    dead_server = self.heartbeat_neighbour[0]   # variable speichert IP & Port, so zugriff auf IP
                    index = self.findIndexOfElement(dead_server, self.serverList)
                    self.serverList.pop(index)

                    for x in range(len(self.serverList)):
                        heartbeatlis_socket.sendto(dead_server.encode(),(self.serverList[x], SERVER_SERVERLIST_PORT))
                
                # if Iam alone, do nothing
                elif len(self.sorted_ip_ring) == 1:
                    pass

                else:
                    pass

    def HandleServerCrash(self):
        # create socket
        serverlist_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        serverlist_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        serverlist_socket.bind(('', SERVER_SERVERLIST_PORT))

        while True:
            # receive server crash message
            update, address = serverlist_socket.recvfrom(buffer_size)
            dead_server = update.decode()

            if update:
                # if the IP of crashed server is in the server list, delete it
                if dead_server in self.serverList:
                    index = self.findIndexOfElement(dead_server, self.serverList)
                    self.serverList.pop(index)

                # if backup server crashed, form a new ring and start the heartbeat again, in case the sorted server list includes more than one IPs
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

                # if leader server crashed form start the hole election process again
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

            # create socket
            heartbeatsend_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            heartbeat_message = self.heartbeatMessage

            # send heartbeat message
            try:
                heartbeatsend_socket.sendto(heartbeat_message.encode(), (self.neighbour_left, SERVER_HEARTBEAT_PORT))
                self.printwt(f'Sent {heartbeat_message} to {self.neighbour_left}')

            except socket.error as e: 
                print(str(e))
            
            self.heartbeatRunning = False
        
    def ListenForClientAndReply(self):
        # create socket bind to server address
        multilisClient_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        multilisClient_sock.bind(('', CLIENT_MULTICAST_PORT))

        # tell the os to add the socket to the multicast group
        group = socket.inet_aton(MULTICAST_GROUP_IP)
        mreg = struct.pack('4sL', group, socket.INADDR_ANY)
        multilisClient_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreg)
        
        while True:

            # receive data from other participants
            data, address = multilisClient_sock.recvfrom(buffer_size)

            if data:
                # if you have data decode the message
                newClient_address = data.decode()
                self.printwt(f'New client wants to connect: {newClient_address}')
                
                # if the decoded address is not in the server list add it and print the list
                if newClient_address not in self.clientList:
                    self.clientList.append(newClient_address)
                    self.printwt(f'client list: {self.clientList}')

                # if Iam the leader, answer the client including my IP
                if MY_IP == self.leader_IP:
                    reply_message = MY_IP
                    multilisClient_sock.sendto(str.encode(reply_message), address)
                    self.printwt('Replied my IP to new client')
                    self.SendAuctionList(newClient_address)
            
    def SendAuctionList(self, client_address):
        # if Iam the leader send the updated auction list to the client
        if self.leader_IP == MY_IP:
            biddingplace_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            biddingplace_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            auctionList = json.dumps(self.auctionList)
            biddingplace_sock.sendto(auctionList.encode(), (client_address, CLIENT_LEADER_COMMUNICATION_PORT))

    def SendLeaderIPUpdate(self):
        # if Iam the leader send the updated leader IP to the clients
        if self.leader_IP == MY_IP:
            newLeaderIP_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            newLeaderIP_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            if self.clientList:
                for x in range(len(self.clientList)):
                    newLeaderIP_sock.sendto(self.leader_IP.encode(), (self.clientList[x], SERVER_NEW_LEADER_PORT))
       
    def SendClientListUpdate(self, newServerAddress):
        # if Iam the leader send the updated client list to the servers
        if self.leader_IP == MY_IP:
            clientListUpdateSend_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            clientListUpdateSend_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            if self.clientList:
                update_msg = json.dumps(self.clientList)
                self.printwt("Trying to send clientList to new server")
                clientListUpdateSend_sock.connect((newServerAddress, SERVER_CLIENTLIST_PORT))
                clientListUpdateSend_sock.send(update_msg.encode())

    def SendStatusOfAuctions(self, newServerAddress):
        # if Iam the leader send replication to the servers
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
        # create socket
        biddingplace_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        biddingplace_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        biddingplace_sock.bind(('', CLIENT_LEADER_COMMUNICATION_PORT))

        while True:
            # if iam the leader, listen for client messages
            if self.leader_IP == MY_IP: 
                self.printwt('Listening for client request...')
                bid, address = biddingplace_sock.recvfrom(1024)

                # decode received data
                if bid:
                    bidInformation = json.loads(bid.decode())

                    for i in bidInformation:
                        key = i
                        val = bidInformation[key]
                    
                    # if the client bids on smartphone, compare the incoming with the current bid
                    # if the incoming bid is higher send a positive response
                    if '1' in bidInformation and val > self.auctionList['smartphone']:
                        self.auctionList['smartphone'] = val
                        reply = 'You are the highest bidder'
                        biddingplace_sock.sendto(reply.encode(), address)

                    # if the client bids on laptop, compare the incoming with the current bid
                    # if the incoming bid is higher send a positive response
                    elif '2' in bidInformation and val > self.auctionList['laptop']:
                        self.auctionList['laptop'] = bidInformation[key]
                        reply = 'You are the highest bidder'
                        biddingplace_sock.sendto(reply.encode(), address)
                    
                    # if the client bids on monitor, compare the incoming with the current bid
                    # if the incoming bid is higher send a positive response
                    elif '3' in bidInformation and val > self.auctionList['monitor']:
                        self.auctionList['monitor'] = bidInformation[key]
                        reply = 'You are the highest bidder'
                        biddingplace_sock.sendto(reply.encode(), address)
                    
                    # if the incoming bid is lower send a negative response
                    else:
                        reply = 'Sorry, you are not the highest bidder'
                        biddingplace_sock.sendto(reply.encode(), address)

                    # send the updated auction list to the client, send replication to the servers
                    self.printwt(self.auctionList)
                    self.SendAuctionList(address[0])
                    self.sendReplica = True
                    self.informServer = False
                    self.SendStatusOfAuctions(address[0])
                        
    def ListenForReplica(self):
        #create TCP socket
        replicaLis_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        replicaLis_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        replicaLis_sock.bind(('', SERVER_REPLICA_PORT))
        replicaLis_sock.listen()

        while True:
            # listen for replication message
            replica, address = replicaLis_sock.accept()
            newAuctionList = replica.recv(buffer_size)

            # decode replication message
            if newAuctionList:
                newAuctionList = json.loads(newAuctionList.decode())
                self.printwt(f'Received Updated Auction List: {newAuctionList}')
                self.auctionList = newAuctionList

    def ListenForClientListUpdates(self):
        # create TCP socket
        clientUpdateLis_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientUpdateLis_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        clientUpdateLis_sock.bind(('', SERVER_CLIENTLIST_PORT))
        clientUpdateLis_sock.listen()

        while True:
            # listen for client list update
            clientListUpdate, address = clientUpdateLis_sock.accept()
            newClientList = clientListUpdate.recv(buffer_size)

            # decode update message
            if newClientList:
                newClientList = json.loads(newClientList.decode())
                self.printwt(f'Received client list: {newClientList}')
                self.clientList = newClientList


# starting all simultaneously working procedures
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