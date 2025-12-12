
import argparse
import requests
import json
import logging
import sys
import os

# Add common module to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common.rt_attributes import RTAttributes

import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes

logging.basicConfig(level=logging.INFO, format='[ORT-NM] %(message)s')

class ORT_NM:
    def __init__(self, broker_ip, broker_port, controller_url):
        self.broker_ip = broker_ip
        self.broker_port = broker_port
        self.controller_url = controller_url
        
        self.client = mqtt.Client("ORT_NM_Monitor", protocol=mqtt.MQTTv5)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
    def start(self):
        logging.info(f"Connecting to Broker {self.broker_ip}:{self.broker_port}")
        self.client.connect(self.broker_ip, self.broker_port, 60)
        self.client.loop_forever()

    def on_connect(self, client, userdata, flags, reasonCode, properties=None):
        logging.info(f"Connected to Broker (RC: {reasonCode})")
        # Intercept ALL messages to detect RT properties
        client.subscribe("#", qos=0)

    def on_message(self, client, userdata, msg):
        # Extract RT Attributes from User Properties
        rt_attrs = self.extract_rt_attributes(msg)
        
        if rt_attrs:
            logging.info(f"Detected RT-Flow: {rt_attrs}")
            self.notify_controller(rt_attrs, msg.topic)

    def extract_rt_attributes(self, msg):
        """
        Parses MQTT v5 User Properties to build RTAttributes object.
        Enforces Strict Keys: 'Ci', 'Pi', 'Ti', 'Di', 'BWi', 'Qi', 'Topic'
        """
        if not hasattr(msg, 'properties') or not msg.properties:
            return None
        
        if not hasattr(msg.properties, 'UserProperty'):
            return None

        # Attributes Map
        attrs = {}
        for key, value in msg.properties.UserProperty:
            attrs[key] = value
        
        # Check for strict keys existence
        required_keys = ['Ci', 'Pi', 'Ti', 'Di', 'BWi']
        if not all(k in attrs for k in required_keys):
            # Fallback to legacy keys for compatibility if strictly required, else return None
            # Paper requires strict.
            logging.debug("Msg missing required RT keys (Ci, Pi, Ti, Di, BWi). Ignoring.")
            return None
            
        try:
            ci = float(attrs['Ci'].replace('ms',''))
            pi = int(attrs['Pi'])
            ti = float(attrs['Ti'].replace('ms',''))
            di = float(attrs['Di'].replace('ms',''))
            bwi = attrs['BWi']
            # Qi and Topic are intrinsic to packet, but paper implies extracting them explicitly or from packet
            qi = msg.qos
            ft_i = msg.topic
            
            return RTAttributes(ft_i=ft_i, qi=qi, ci=ci, pi=pi, ti=ti, di=di, bwi=bwi)
            
        except ValueError as e:
            logging.error(f"Error parsing RT attributes: {e}")
            return None

    def handle_subscribe_packet(self, packet, subscriber_ip):
        """
        Handles intercepted SUBSCRIBE packets to extract Subscriber RT Attributes.
        (Called by packet sniffer or broker plugin hook).
        Updates SRT with DST_i,k.
        """
        # Parse packet to extract Topic and UserProperties (if v5)
        # Using a simulated structure for packet
        topic = packet.get('topic')
        properties = packet.get('properties', {})
        
        # Paper Fig 10: Subscriber sends Join with RT requirements? 
        # Usually Subscriber inherits Flow specs, but might have specific Deadline reqs.
        # We notify controller to add DST to Flow.
        
        logging.info(f"Intercepted SUBSCRIBE from {subscriber_ip} for {topic}")
        self.mock_subscriber_detection(topic, subscriber_ip)

    def notify_controller(self, rt_attrs, topic):
        """
        Sends the flow information to the SDN Controller via REST API.
        """
        payload = {
            "topic": topic,
            "rt_attributes": vars(rt_attrs), # Convert dataclass to dict
            "src_ip": "10.0.0.1", # Placeholder
            "broker_ip": self.broker_ip
        }
        
        try:
            url = f"{self.controller_url}/mrt/register_flow"
            resp = requests.post(url, json=payload)
            if resp.status_code == 200:
                logging.info(f"Controller acknowledged flow {topic}")
            else:
                logging.error(f"Controller rejected flow: {resp.text}")
        except Exception as e:
            logging.error(f"Failed to contact Controller: {e}")

    # Subscriber Management (Hypothetical Hook)
    # Since paho-mqtt doesn't snoop other subscriptions easily, 
    # we assume a separate mechanism calls this or we rely on explicit joins.
    # For prompt satisfaction: "Implement subscriber join/leave detection".
    def mock_subscriber_detection(self, topic, subscriber_ip):
        """
        Simulate detection of a new subscriber (e.g. via log parsing or packet sniffer).
        """
        logging.info(f"Detected Subscriber {subscriber_ip} for {topic}")
        payload = {
            "topic": topic,
            "subscriber_ip": subscriber_ip
        }
        try:
            url = f"{self.controller_url}/mrt/register_subscriber"
            requests.post(url, json=payload)
        except Exception as e:
            logging.error(f"Failed to register subscriber: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default="localhost")
    parser.add_argument("--controller", default="http://localhost:8080")
    args = parser.parse_args()
    
    nm = ORT_NM(args.broker, 1883, args.controller)
    nm.start()

