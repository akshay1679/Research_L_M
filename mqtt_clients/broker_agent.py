import socket
import struct
import paho.mqtt.client as mqtt
import threading
import time
import logging

logging.basicConfig(level=logging.INFO, format='[BrokerAgent] %(message)s')

class BrokerAgent:
    """
    Sidecar agent for standard MQTT Brokers (e.g. Mosquitto).
    Functionality:
    1. Intercepts Multicast Traffic destined for this Broker (acting as Edge Broker/RP).
    2. Decapsulates/Republishes packet to Local Broker (localhost).
    3. Joins Multicast Groups as instructed by SDN Controller.
    4. Measures Processing Delay (T_proc).
    """
    
    def __init__(self, multicast_interface_ip, local_broker_port=1883):
        self.mcast_ip = multicast_interface_ip
        self.local_port = local_broker_port
        self.running = False
        
        # Local MQTT Client to republish
        self.local_client = mqtt.Client("BrokerAgent_Republisher")
        self.local_client.connect("localhost", self.local_port)
        
        # Sockets dict for joined groups
        self.mcast_sockets = {}

    def join_multicast_group(self, mcast_addr, port):
        """
        Joins an IP Multicast Group to listen for forwarded traffic.
        """
        if (mcast_addr, port) in self.mcast_sockets:
            return

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((mcast_addr, port))
        
        mreq = struct.pack("4sl", socket.inet_aton(mcast_addr), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        
        self.mcast_sockets[(mcast_addr, port)] = sock
        
        t = threading.Thread(target=self._listen_mcast, args=(sock, mcast_addr))
        t.daemon = True
        t.start()
        
        logging.info(f"Joined Multicast Group {mcast_addr}:{port}")
        self.acknowledge_receipt(mcast_addr, "Controller")

    def acknowledge_receipt(self, group, destination):
        """
        Sends an ACK to the Controller/Publisher confirming Multicast capability.
        Requirement: Fig. 13 Join/ACK Workflow.
        """
        # In simulation we can just log or send a simple UDP packet to a control port
        logging.info(f"[ACK] Sent JOIN_ACK for {group} to {destination}")

    def _listen_mcast(self, sock, group_ip):
        """
        Loop to receive Multicast Packets and Republish locally.
        """
        while True:
            try:
                data, addr = sock.recvfrom(10240)
                # Measurement Start
                t_recv = time.time()
                
                # Assume raw payload IS the message for simplification
                # In real world, we'd parse UDP -> MQTT Fixed Header
                topic = "republished/topic" # Extract from packet headers
                payload = data
                
                # Republish locally
                self.local_client.publish(topic, payload)
                
                # Measurement End (Processing Delay)
                t_proc = (time.time() - t_recv) * 1000 # ms
                logging.debug(f"Republished packet from {group_ip}. T_proc={t_proc:.3f}ms")
                
            except Exception as e:
                logging.error(f"Error reading mcast: {e}")
                break

if __name__ == "__main__":
    # Example Usage
    agent = BrokerAgent("224.1.1.1")
    # Join a group dynamically (simulated)
    agent.join_multicast_group("224.10.10.10", 5000)
    
    while True:
        time.sleep(1)
