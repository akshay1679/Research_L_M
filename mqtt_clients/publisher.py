
import time
import argparse
import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes

def main():
    parser = argparse.ArgumentParser(description="MRT-MQTT Extended Publisher")
    parser.add_argument("--host", default="localhost", help="Broker host")
    parser.add_argument("--port", type=int, default=1883, help="Broker port")
    parser.add_argument("--topic", required=True, help="Topic")
    
    # RT Attributes
    parser.add_argument("--deadline", required=True, help="Deadline in ms (e.g. 50ms)")
    parser.add_argument("--trans_time", required=True, help="Transmission Time in ms")
    parser.add_argument("--period", required=True, help="Period in ms")
    parser.add_argument("--min_bw", required=True, help="Min Bandwidth")
    parser.add_argument("--priority", type=int, required=True, help="Priority (int)")
    parser.add_argument("--qos", type=int, default=1, help="MQTT QoS")
    
    parser.add_argument("--multicast_dst", default=None, help="Multicast IP:Port for QoS 0")

    args = parser.parse_args()

    # Strict Keys matching Paper Eq. 4
    rt_props = [
        ("Di", args.deadline),
        ("Ci", args.trans_time),
        ("Ti", args.period),
        ("BWi", args.min_bw),
        ("Pi", str(args.priority))
    ]

    if args.qos == 0 and args.multicast_dst:
        # Direct Multicast Mode (QoS 0)
        import socket
        import struct
        print(f"QoS 0: Sending Direct Multicast to {args.multicast_dst}")
        
        ip, port = args.multicast_dst.split(":")
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
        
        # Serialize Mock Packet with Properties (custom format or JSON for simulation)
        payload = f"{args.topic}|{str(rt_props)}|RT Data Payload".encode('utf-8')
        sock.sendto(payload, (ip, int(port)))
        print("Sent UDP Multicast.")
        
    else:
        # Broker Mode (QoS 1, 2)
        client = mqtt.Client("mrt_pub", protocol=mqtt.MQTTv5)
        print(f"Connecting to Broker {args.host}:{args.port} (QoS {args.qos})...")
        client.connect(args.host, args.port, 60)
        client.loop_start()
        time.sleep(1)

        # Publish Properties
        props = Properties(PacketTypes.PUBLISH)
        props.UserProperty = rt_props

        print(f"Publishing RT Message to {args.topic}...")
        client.publish(args.topic, "RT Data Payload", qos=args.qos, properties=props)
        
        time.sleep(1)
        client.loop_stop()
        client.disconnect()

    # Logic handled in if/else blocks above.

if __name__ == "__main__":
    main()
