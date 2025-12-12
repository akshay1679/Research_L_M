import sys
import os
import time

# Add path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common.rt_attributes import RTAttributes, Link, Switch
from common.of_db import of_db
from schedulability.analysis import HolisticApproach
from sdn_controller.routing import RoutingEngine
from simulation.monitor import NetworkMonitor

def run_advanced_verification():
    print("=== MRT-MQTT Advanced Verification: Real-Time Stats & Multicast ===")
    
    # 1. Start Network Monitor (Simulation Mode)
    monitor = NetworkMonitor(simulation_mode=True)
    monitor.start_monitoring()
    
    # 2. Setup Topology (Triangle)
    print("\n[Setup] Building Topology...")
    switches = {1: Switch(1, "S1"), 2: Switch(2, "S2"), 3: Switch(3, "S3")}
    for s in switches.values(): of_db.add_switch(s.dpid, s)
    
    # Links with Capacity
    def add(s, d, p):
        l = Link(str(s), str(d), p, bw_capacity=100.0)
        # Initialize with baseline
        l.prop_delay = 5.0 
        of_db.add_link(str(s), str(d), p, l)
        
    add(1, 2, 1)
    add(2, 3, 1)
    add(1, 3, 2)
    
    # Wait for Monitor to update stats
    print("[Setup] Waiting for Network Monitor to measure delay/jitter...")
    time.sleep(6) # Monitor updates every 5s
    
    # Check if stats updated
    l_12 = of_db.links.get((str(1), str(2), 1)) or of_db.links.get("S1:1->S2:1") # Handling Key var
    # My add_link usage: add_link(src, dst, port, link_obj) -> key might be tuple or string depending on impl
    # Checking of_db implementation:
    # It stores based on whatever key I pass. In add_link calls above I passed src, dst, port.
    # But Monitor iterates `of_db.links.items()`.
    
    # 3. Create Complex Flow
    print("\n[Test] Creating Delay-Sensitive Flow...")
    flow = RTAttributes(ft_i="topic/alert", qi=2, ci=2.0, pi=10, ti=20.0, di=20.0, bwi=5.0)
    flow.src_ip = "1"
    flow.dst_ips = ["3"]
    
    # Simulate Broker Processing Delay
    flow.processing_delay = 1.5 # 1.5ms
    
    # 4. Route Calculation
    re = RoutingEngine()
    flow.route_links = re.calculate_path(flow.src_ip, flow.dst_ips)
    print(f"Calculated Path Length: {len(flow.route_links)}")
    
    # 5. Analysis with Real Stats
    wcrt = HolisticApproach.calculate_wcrt(flow, [flow])
    print(f"WCRT (with Measured Jitter/Delay): {wcrt:.3f}ms")
    
    # Verify Jitter influence
    if wcrt > 7.0: # Base 5ms + Processing 1.5 + Jitter
        print("-> Real-time Jitter correctly accounted for.")
    else:
        print("-> Warning: WCRT seems too low, check Jitter integration.")
        
    if wcrt <= flow.di:
        print("-> Flow ADMITTED.")
    else:
        print("-> Flow REJECTED.")

if __name__ == "__main__":
    run_advanced_verification()
