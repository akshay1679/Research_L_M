import threading
from typing import Dict, List, Optional
from common.rt_attributes import RTAttributes, Link, Switch

class OFDB:
    """
    Singleton In-Memory Database representing the OpenFlow Database (OF-DB).
    Stores:
    - Flows (SRT Tables)
    - Topology (Switches, Links)
    - Multicast Groups
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(OFDB, cls).__new__(cls)
                cls._instance.flows: Dict[str, RTAttributes] = {} # Key: Topic
                cls._instance.switches: Dict[int, Switch] = {} # Key: DPID
                cls._instance.links: Dict[str, Link] = {} # Key: "src_dpid:port->dst_dpid"
                cls._instance.multicast_groups: Dict[str, int] = {} # Key: Topic, Value: GroupID
        return cls._instance

    def add_flow(self, topic: str, flow_specs: RTAttributes):
        """Register or update a flow in the database."""
        with self._lock:
            self.flows[topic] = flow_specs
            print(f"[OF-DB] Updated Flow for Topic: {topic} -> {flow_specs}")

    def get_flow(self, topic: str) -> Optional[RTAttributes]:
        with self._lock:
            return self.flows.get(topic)

    def get_all_flows(self) -> Dict[str, RTAttributes]:
        with self._lock:
            return self.flows.copy()

    def add_subscriber(self, topic: str, sub_ip: str):
        with self._lock:
            if topic in self.flows:
                if sub_ip not in self.flows[topic].dst_ips:
                    self.flows[topic].dst_ips.append(sub_ip)
                print(f"[OF-DB] Added Subscriber {sub_ip} to Topic {topic}")
            else:
                print(f"[OF-DB] Warn: Topic {topic} not found. Subscriber {sub_ip} pending.")

    # Topology Management
    def add_switch(self, dpid: int, switch_data: Switch):
        with self._lock:
            self.switches[dpid] = switch_data

    def add_link(self, src: str, dst: str, port: int, link_data: Link):
        key = f"{src}:{port}->{dst}"
        with self._lock:
            self.links[key] = link_data

    # Multicast Management
    def get_multicast_group_id(self, topic: str) -> int:
        with self._lock:
            if topic not in self.multicast_groups:
                # Simple generation of Group ID based on hash or counter
                # For safety, ensure it fits in OpenFlow Group ID space
                gid = abs(hash(topic)) % 0xFFFFFFF
                self.multicast_groups[topic] = gid
            return self.multicast_groups[topic]

# Global Instance
of_db = OFDB()
