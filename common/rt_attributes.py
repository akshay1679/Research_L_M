
from dataclasses import dataclass, field
from typing import List, Optional, Dict

@dataclass
class RTAttributes:
    """
    Data class representing the Real-Time Attributes for an MQTT Flow (FiTS).
    Paper Equation (5) SRT Structure:
    - FiTS: Flow real-time specifications (Ci, Pi, Ti, Di, BWi)
    - SRCi: Source node
    - DSTi,k: Set of destination nodes (Subscribers)
    - BAi: Broker agents involved
    - Li: List of links (Route)
    - ni: Number of hops
    - ID: Flow ID (FTi)
    """
    ft_i: str  # Flow Topic (ID)
    qi: int    # QoS Level (0, 1, 2)
    ci: float = 0.0   # Max transmission time (ms)
    pi: int = 1       # Priority (Higher is better)
    ti: float = 1000.0 # Period / Min Inter-arrival Time (ms)
    di: float = 1000.0 # Relative Deadline (ms)
    bwi: str = "1Mbps" # Bandwidth String originally, converted during usage
    
    # Dynamic Routing Attributes
    src_ip: str = "" 
    dst_ips: List[str] = field(default_factory=list) # DSTi,k
    broker_ips: List[str] = field(default_factory=list) # BAi
    route_links: List['Link'] = field(default_factory=list) # Li (List of Link objects)
    num_hops: int = 0 # ni
    
    # Multicast & Processing
    multicast_group_id: int = 0
    processing_delay: float = 0.0 # T_proc (Broker Processing Time)
    measured_jitter: float = 0.0

    def __str__(self):
        return f"Flow {self.ft_i} [QoS={self.qi}, P={self.pi}, D={self.di}ms, BW={self.bwi}Mbps]"

    def __hash__(self):
        return hash(self.ft_i)

    def __eq__(self, other):
        if not isinstance(other, RTAttributes): return False
        return self.ft_i == other.ft_i

@dataclass
class Link:
    """
    Represents a Network Link with Delay Attributes.
    """
    src: str # Switch DPID or Host IP
    dst: str # Switch DPID or Host IP
    port_out: int # Port on src connected to dst
    bw_capacity: float = 1000.0 # Mbps
    bw_used: float = 0.0
    
    # Delays (ms)
    prop_delay: float = 0.01   # Propagation Delay (Distance / Speed)
    switch_delay: float = 0.01 # Switching Delay (Hardware dependent)
    proc_delay: float = 0.01   # Processing Delay (Software/OS dependent)
    queuing_delay: float = 0.0 # Variable, calculated based on load
    jitter: float = 0.0        # Measured Jitter (J_SD)

    def get_transmission_delay(self, packet_size_bits: int) -> float:
        """
        Calculate Transmission Delay = Packet Size / Bandwidth.
        """
        if self.bw_capacity <= 0: return float('inf')
        return (packet_size_bits / (self.bw_capacity * 1e6)) * 1000 # returns ms

@dataclass
class Switch:
    """
    Represents a Switch in the topology.
    """
    dpid: int
    name: str = ""
    ip: str = ""
    is_broker: bool = False
    connected_hosts: List[str] = field(default_factory=list)

