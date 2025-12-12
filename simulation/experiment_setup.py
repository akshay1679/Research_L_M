import sys
import os
import random
import time

# Add path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common.rt_attributes import RTAttributes, Link, Switch
from common.of_db import of_db
from schedulability.analysis import HolisticApproach, TrajectoryApproach
from sdn_controller.routing import RoutingEngine

def run_experiment():
    print("=== MRT-MQTT Experiment: WCRT Validation ===")
    
    # 1. Setup Mock Topology
    print("\n[Setup] Building Mock Topology (Diamond Shape)...")
    # S1 -> S2, S1 -> S3, S2 -> S4, S3 -> S4
    
    # Define Switches
    switches = {
        1: Switch(dpid=1, name="S1"),
        2: Switch(dpid=2, name="S2"),
        3: Switch(dpid=3, name="S3"),
        4: Switch(dpid=4, name="S4")
    }
    for dpid, sw in switches.items():
        of_db.add_switch(dpid, sw)
        
    # Define Links (100Mbps capacity)
    def add_link(s, d, p):
        l = Link(src=str(s), dst=str(d), port_out=p, bw_capacity=100.0)
        of_db.add_link(str(s), str(d), p, l)
        
    add_link(1, 2, 1) # S1->S2
    add_link(1, 3, 2) # S1->S3
    add_link(2, 4, 1) # S2->S4
    add_link(3, 4, 1) # S3->S4
    
    # 2. Setup Routing Engine
    re = RoutingEngine()
    
    # 3. Create Flows
    print("\n[Test] Creating Random Flows...")
    flows = []
    
    # Flow A: High Priority, Low traffic
    f_a = RTAttributes(ft_i="topic/sensor_A", qi=1, ci=0.5, pi=10, ti=20.0, di=10.0, bwi=1.0)
    f_a.src_ip = "1" # Attached to S1
    f_a.dst_ips = ["4"] # Attached to S4
    
    # Flow B: Low Priority, High traffic interference
    f_b = RTAttributes(ft_i="topic/camera_B", qi=1, ci=5.0, pi=5, ti=50.0, di=50.0, bwi=10.0)
    f_b.src_ip = "1"
    f_b.dst_ips = ["4"]
    
    # Calculate Routes
    f_a.route_links = re.calculate_path(str(1), str(4))
    f_b.route_links = re.calculate_path(str(1), str(4))
    
    print(f"Flow A Route Hops: {len(f_a.route_links)}")
    print(f"Flow B Route Hops: {len(f_b.route_links)}")
    
    all_flows = [f_a, f_b]
    
    # 4. WCRT Analysis
    print("\n[Analysis] Calculating WCRT...")
    
    # Analyze Flow A (Should be fast)
    wcrt_a_ha = HolisticApproach.calculate_wcrt(f_a, all_flows)
    wcrt_a_ta = TrajectoryApproach.calculate_wcrt(f_a, all_flows)
    print(f"Flow A (High Prio) WCRT: HA={wcrt_a_ha:.3f}ms, TA={wcrt_a_ta:.3f}ms (Deadline: {f_a.di}ms)")
    
    if wcrt_a_ta <= f_a.di:
        print("-> Flow A Schedulable: YES")
    else:
        print("-> Flow A Schedulable: NO")

    # Analyze Flow B (Should suffer interference from A)
    wcrt_b_ha = HolisticApproach.calculate_wcrt(f_b, all_flows)
    wcrt_b_ta = TrajectoryApproach.calculate_wcrt(f_b, all_flows)
    print(f"Flow B (Low Prio) WCRT: HA={wcrt_b_ha:.3f}ms, TA={wcrt_b_ta:.3f}ms (Deadline: {f_b.di}ms)")
    
    # 5. Measure Jitter (Simulated)
    # Jitter = WCRT - BestCRT
    print("\n[Analysis] Estimated Jitter...")
    bcrt_a = f_a.ci # Best case is just transmission + prop
    jitter_a = wcrt_a_ta - bcrt_a
    print(f"Flow A Jitter: {jitter_a:.3f}ms")

if __name__ == "__main__":
    run_experiment()
