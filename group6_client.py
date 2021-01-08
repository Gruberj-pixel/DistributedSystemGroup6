import socket
import threading
import time
import struct
from datetime import datetime
import json

MULTICAST_GROUP_IP = '224.1.1.1'

MULTICAST_PORT_CLIENT = 7000 # Port for clients to discover servers
CLIENT_CONNECTION_TO_LEADER_PORT = 9000
SERVER_MESSAGELIST_PORT = 5300
SERVER_NEW_LEADER_PORT = 5500

# Localhost information
MY_HOST = socket.gethostname()
MY_IP = socket.gethostbyname(MY_HOST)
buffer_size = 1024

class Client():
    def __init__(self):
        self.auctionList = {}
        self.currentLeader = ''

    def printwt(self, msg):
        current_date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f'[{current_date_time}] {msg}')
    """
    def findIndexOfElement(self, element, mylist):
        try:
            index = mylist.index(element)
            return index
        except ValueError:
            return None
    """
    def MulticastSendAndReceive(self):
        message = MY_IP
        multicast_group = (MULTICAST_GROUP_IP, MULTICAST_PORT_CLIENT)
        multisend_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
        multisend_sock.settimeout(5)

        # set time to live message (network hps; 1 for local)
        ttl = struct.pack('b', 1)
        multisend_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)

        # send my IP to other participants
        multisend_sock.sendto(message.encode(), multicast_group)
        self.printwt("Sent my IP to server group")
        
        while True:     
            try:
                # receive reply data from the other participants
                reply, address = multisend_sock.recvfrom(1024)

                if reply:
                    # decode received data
                    reply_address = reply.decode()
                    self.currentLeader = reply_address
                    self.printwt(f'Got Leader address {self.currentLeader}')

            except socket.timeout:
                pass

            #finally:
            #   multisend_sock.close()

    def ListenForAuctionInformation(self):
        biddingplacelis_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        biddingplacelis_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        biddingplacelis_sock.bind((MY_IP, SERVER_MESSAGELIST_PORT))

        while True:

            try:
                auctions, address = biddingplacelis_sock.recvfrom(buffer_size)
                self.auctionList = json.loads(auctions.decode())

                if auctions:
                    no = 1
                    for i in self.auctionList:
                        auction = i
                        bid = self.auctionList[i]
                        self.printwt(f'Auction {no}: {auction}, Highest Bid: {bid}')
                        no += 1

                    self.SendBid()
            
            except socket.error as e:
                print(str(e))


    def SendBid(self):
        biddingplace_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
        biddingplace_sock.settimeout(5)

        # set time to live message (network hps; 1 for local)
        ttl = struct.pack('b', 1)
        biddingplace_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)

        item = int(input('On which item you want to bid? '))
        bid = float(input('Please set your bid amount: '))
        bidInformation = {item: bid}
        bidInformation = json.dumps(bidInformation)
        biddingplace_sock.sendto(bidInformation.encode(), (self.currentLeader, SERVER_MESSAGELIST_PORT))

        while True:     
            try:
                # receive reply data from the other participants
                reply, address = biddingplace_sock.recvfrom(1024)

                if reply:
                    # decode received data
                    message = reply.decode()
                    self.printwt(message)

            except socket.timeout:
                break

    def ListenForLeaderServerUpdate(self):
        newLeaderlis_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        newLeaderlis_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        newLeaderlis_sock.bind((MY_IP, SERVER_NEW_LEADER_PORT))

        while True:

            try:
                newLeader, address = newLeaderlis_sock.recvfrom(buffer_size)

                if newLeader:
                    newLeaderIP = newLeader.decode()
                    #self.printwt(f'System communicates via a new Leader: {newLeaderIP}')
                    self.currentLeader = newLeaderIP

            except socket.error as e:
                print(str(e))


if __name__ == "__main__":
    client = Client()

    thread1 = threading.Thread(target = client.MulticastSendAndReceive)
    thread1.start()

    thread2 = threading.Thread(target = client.ListenForAuctionInformation)
    thread2.start()

    thread3 = threading.Thread(target = client.ListenForLeaderServerUpdate)
    thread3.start()
