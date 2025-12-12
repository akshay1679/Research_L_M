import threading
import json
import socket
import logging

# Configure Logging
logging.basicConfig(level=logging.INFO, format='[MSDP] %(message)s')

class MSDP_Signaling:
    """
    Implements MSDP-like (Multicast Source Discovery Protocol) signaling for MRT-MQTT.
    Allows Edge Brokers to discover active sources (Publishers) and Topics in other domains.
    """
    
    def __init__(self, my_ip, peers):
        self.my_ip = my_ip
        self.peers = peers # List of Peer Broker IPs
        self.active_sources = {} # Key: Topic, Value: SourceIP
        self.running = False
        self.sock = None

    def start_listener(self, port=1791):
        """Start TCP listener for MSDP messages."""
        self.running = True
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(('0.0.0.0', port))
        self.sock.listen(5)
        
        listener_thread = threading.Thread(target=self._listen_loop)
        listener_thread.daemon = True
        listener_thread.start()
        logging.info(f"MSDP Listener started on port {port}")

    def _listen_loop(self):
        while self.running:
            try:
                client, addr = self.sock.accept()
                threading.Thread(target=self._handle_peer, args=(client, addr)).start()
            except Exception as e:
                logging.error(f"Listener Error: {e}")

    def _handle_peer(self, client, addr):
        try:
            data = client.recv(1024).decode()
            if data:
                msg = json.loads(data)
                self.process_sa_message(msg, addr[0])
        except Exception as e:
            logging.error(f"Peer Error {addr}: {e}")
        finally:
            client.close()

    def process_sa_message(self, msg, peer_ip):
        """Process Source Active (SA) message."""
        msg_type = msg.get('type')
        if msg_type == 'SA':
            topic = msg.get('topic')
            src_ip = msg.get('src_ip')
            # RP = msg.get('rp')
            
            logging.info(f"Received SA for {topic} from {src_ip} via {peer_ip}")
            
            # Update Local Table
            if topic not in self.active_sources:
                self.active_sources[topic] = src_ip
                # Trigger Join if we have local subscribers?
                # This depends on Broker Logic.
                pass

    def send_sa_message(self, topic, src_ip):
        """Broadcast Source Active (SA) message to all peers."""
        msg = {
            'type': 'SA',
            'topic': topic,
            'src_ip': src_ip,
            'origin_conn': self.my_ip
        }
        
        for peer in self.peers:
            self._send_to_peer(peer, msg)

    def _send_to_peer(self, peer_ip, msg, port=1791):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((peer_ip, port))
            s.send(json.dumps(msg).encode())
            s.close()
            logging.info(f"Sent SA for {msg['topic']} to {peer_ip}")
        except Exception as e:
            logging.error(f"Failed to send to {peer_ip}: {e}")
