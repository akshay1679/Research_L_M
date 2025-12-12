import time
import threading
import statistics
import logging
from common.of_db import of_db

logging.basicConfig(level=logging.INFO, format='[Monitor] %(message)s')

class NetworkMonitor:
    """
    Monitors Network Performance (Delay, Jitter) and updates OF-DB.
    Uses Packet Probes or passive monitoring (simulated).
    """

    def __init__(self, simulation_mode=True):
        self.running = False
        self.simulation_mode = simulation_mode
        self.history = {} # Key: LinkID, Value: List[Latencies]

    def start_monitoring(self):
        self.running = True
        t = threading.Thread(target=self._monitor_loop)
        t.daemon = True
        t.start()
        logging.info("Network Monitor Started.")

    def _monitor_loop(self):
        while self.running:
            self._measure_links()
            time.sleep(5) # Update every 5 seconds

    def _measure_links(self):
        """
        Iterate over all links in OF-DB and update their delay/jitter stats.
        """
        links = of_db.links
        for key, link in links.items():
            # In a real SDN, we'd send probe packets (LLDP or custom) and measure RTT.
            # Here we simulate or read from a 'mock' interface.
            
            measured_delay = self._get_latency(link)
            
            # Store history for Jitter calc
            if key not in self.history: self.history[key] = []
            self.history[key].append(measured_delay)
            if len(self.history[key]) > 20: self.history[key].pop(0)
            
            # Calculate Jitter (Std Dev or Max-Min)
            if len(self.history[key]) > 1:
                jitter = statistics.stdev(self.history[key])
            else:
                jitter = 0.0
            
            # Update Link Object
            link.prop_delay = measured_delay # Update "Propagation" as measured RTT/2
            link.jitter = jitter
            
            logging.debug(f"Link {key}: Delay={measured_delay:.3f}ms, Jitter={jitter:.3f}ms")

    def _get_latency(self, link):
        if self.simulation_mode:
            # Simulate random fluctuation around base latency
            import random
            base = 5.0 # ms
            noise = random.uniform(-0.5, 0.5)
            # Add Load factor
            load_factor = (link.bw_used / link.bw_capacity) * 2.0 if link.bw_capacity > 0 else 0
            return base + noise + load_factor
        else:
            # Real Implementation: Send Probe using Scapy
            # Requirement: Section VI-B, "Real switching delay measurement".
            try:
                from scapy.all import sr1, IP, ICMP
                
                # We need the destination IP. 
                # Link object has src/dst as strings (DPIDs). We need to map DPID -> Management IP.
                # Assuming of_db switch entry has IP.
                from common.of_db import of_db
                target_ip = None
                
                # Look up Switch object
                # This is tricky if link.dst is DPID string '1'.
                # OF-DB has switches by integer DPID.
                try:
                    dpid_int = int(link.dst)
                    sw = of_db.switches.get(dpid_int)
                    if sw: target_ip = sw.ip
                except:
                   pass
                   
                if not target_ip:
                    # Fallback if IP unknown or host
                    return 0.1
                
                # Send ICMP Probe
                pkt = IP(dst=target_ip)/ICMP()
                
                start = time.time()
                # timeout=1s, verbose=0
                resp = sr1(pkt, timeout=1.0, verbose=0)
                end = time.time()
                
                if resp:
                    rtt_ms = (end - start) * 1000.0
                    # Switching Delay ~ RTT/2 roughly, or Processing + Queue
                    return rtt_ms / 2.0
                else:
                    return 0.1 # Timeout
                    
            except ImportError:
                logging.error("Scapy not installed. Install scapy for real mode.")
                return 0.1
            except Exception as e:
                logging.debug(f"Probe failed: {e}")
                return 0.1
