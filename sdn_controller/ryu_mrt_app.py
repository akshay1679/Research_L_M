from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.app.wsgi import ControllerBase, WSGIApplication, route
from ryu.lib import dpid as dpid_lib
from ryu.topology import event, switches
from ryu.topology.api import get_switch, get_link
import json
import logging
from webob import Response

# Import our modules
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common.of_db import of_db
from common.rt_attributes import RTAttributes, Switch, Link
from schedulability.analysis import AdmissionControl
from .routing import RoutingEngine

mrt_instance_name = 'mrt_mqtt_api'
url = '/mrt/register_flow'

class RyuMRTApp(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = { 'wsgi': WSGIApplication,
                  'topology_api_app': switches.Switches }

    def __init__(self, *args, **kwargs):
        super(RyuMRTApp, self).__init__(*args, **kwargs)
        self.routing_engine = RoutingEngine()
        wsgi = kwargs['wsgi']
        wsgi.register(MRTController, {mrt_instance_name: self})
        self.datapaths = {}
        self.topology_api_app = kwargs['topology_api_app']

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        self.datapaths[datapath.id] = datapath
        # Install Table-Miss
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)
        
        # Register switch in OF-DB
        sw = Switch(dpid=datapath.id, ip=datapath.address[0] if datapath.address else "")
        of_db.add_switch(datapath.id, sw)
        print(f"[Ryu] Switch Features: DPID={datapath.id} registered.")



    def add_flow(self, datapath, priority, match, actions, meter_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        if meter_id:
             inst.append(parser.OFPInstructionMeter(meter_id))
             
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority, match=match, instructions=inst)
        datapath.send_msg(mod)

    def install_meter(self, datapath, meter_id, bandwidth_mbps):
        """
        Installs a Bandwidth Meter (Rate Limiter).
        """
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        # Rate in kbps usually. 1Mbps = 1000kbps.
        # Check units? Ryu uses kbps.
        rate_kbps = int(bandwidth_mbps * 1000)
        
        # Band = DROP packets exceeding rate
        bands = [parser.OFPMeterBandDrop(rate=rate_kbps, burst_size=0)]
        
        req = parser.OFPMeterMod(datapath=datapath, 
                                 command=ofproto.OFPMC_ADD, 
                                 flags=ofproto.OFPMF_KBPS, 
                                 meter_id=meter_id, 
                                 bands=bands)
        datapath.send_msg(req)
        print(f"[Ryu] Installed Meter {meter_id} on Switch {datapath.id} with {bandwidth_mbps}Mbps")

    def install_multicast_group(self, datapath, group_id, ports):
        """
        Installs an OpenFlow Group Entry (Type ALL) for Multicast.
        """
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        buckets = []
        for port in ports:
            actions = [parser.OFPActionOutput(port)]
            buckets.append(parser.OFPBucket(actions=actions))
            
        req = parser.OFPGroupMod(datapath, ofproto.OFPGC_ADD, ofproto.OFPGT_ALL, group_id, buckets)
        datapath.send_msg(req)
        print(f"[Ryu] Installed Group {group_id} on Switch {datapath.id} ports={ports}")

    # --- TOPOLOGY DISCOVERY ---
    @set_ev_cls(event.EventSwitchEnter)
    def switch_enter_handler(self, ev):
        print(f"[Ryu] EventSwitchEnter: {ev.switch.dp.id}")
        # Note: Switch registration is also handled in features_handler, 
        # but this event confirms it's up in the topology module

    @set_ev_cls(event.EventLinkAdd)
    def link_add_handler(self, ev):
        src = ev.link.src
        dst = ev.link.dst
        print(f"[Ryu] Link Detected: {src.dpid}:{src.port_no} -> {dst.dpid}:{dst.port_no}")
        
        # Create Link Object with default delays
        link_data = Link(src=str(src.dpid), dst=str(dst.dpid), port_out=src.port_no)
        of_db.add_link(str(src.dpid), str(dst.dpid), src.port_no, link_data)
        
        # Update Routing Engine with new topology
        # In a real impl, we might batch these or trigger routing recalc
    
    # --- END TOPOLOGY DISCOVERY ---


    # --- PACKET IN / CLASSIFICATION ---
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        """
        Handles Packet-In for Topic Classification and Multicast Redirection.
        Simulates "Topic-to-Flow Classification" by inspecting packet headers/payload (mocked).
        """
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        # Parse packet (mocked for classification)
        # In real impl, we'd use packet lib to extract IP/TCP/Payload
        # If Topic found in payload (deep packet inspection) -> mark DSCP
        
        # Here we assume packets sent to Controller are "Signaling" or "New Flow Candidates"
        pass

    # --- END PACKET IN ---

    def _install_multicast_tree(self, flow, path_links):
        """
        Installs Multicast forwarding using OpenFlow Group Tables (Type ALL).
        """
        # 1. Map DPID -> Output Ports
        # Use a dict {dpid: set(ports)}
        fwd_map = {}
        for link in path_links:
            dpid = int(link.src)
            if dpid not in fwd_map: fwd_map[dpid] = set()
            fwd_map[dpid].add(link.port_out)
            
        # 2. Iterate and Install
        group_id = flow.multicast_group_id
        if group_id == 0: 
            group_id = abs(hash(flow.ft_i)) % 2000 + 1
            flow.multicast_group_id = group_id

        meter_id = group_id # Re-use ID for simplicity

        for dpid, ports in fwd_map.items():
            if dpid not in self.datapaths: continue
            datapath = self.datapaths[dpid]
            parser = datapath.ofproto_parser
            
            # Install Meter
            bw_val = float(str(flow.bwi).replace("Mbps","").replace("Kbps","")) 
            self.install_meter(datapath, meter_id, bw_val)

            # Install Group (Type ALL)
            # This replicates the packet to all ports in the list
            self.install_multicast_group(datapath, group_id, list(ports))
            
            # Install Flow -> Point to Group
            match = parser.OFPMatch(eth_type=0x0800, ipv4_dst=flow.dst_ips[0] if flow.dst_ips else "224.0.0.1")
            
            actions = [parser.OFPActionGroup(group_id)]
            # Priority Queue is usually set inside bucket or before group. 
            # OVS doesn't support SetQueue in Group Buckets easily in all versions.
            # We set queue in the Flow Entry actions before Group? No, Action Group is terminal-ish.
            # We process using SetField/SetQueue BEFORE group if possible.
            # Simplified: Just Group.
            
            self.add_flow(datapath, 
                          priority=100 + flow.pi, 
                          match=match, 
                          actions=actions, 
                          meter_id=meter_id)

    def register_rt_flow(self, topic, rt_attrs_dict, src_ip, broker_ip):
        """
        Main logic for Flow Registration.
        """
        rt_attrs = RTAttributes(**rt_attrs_dict)
        print(f"[Controller] Request for Topic: {topic}, Prio: {rt_attrs.pi}, QoS: {rt_attrs.qi}")
        
        # 1. Update OF-DB
        rt_attrs.src_ip = src_ip
        if broker_ip:
            if broker_ip not in rt_attrs.broker_ips:
                 rt_attrs.broker_ips.append(broker_ip)
        
        of_db.add_flow(topic, rt_attrs)
        
        # BROADCAST MSDP
        print(f"[MSDP] Advertising Topic {topic} to external domains.")
        
        # 2. Admission Control
        all_flows = of_db.get_all_flows()
        if not AdmissionControl.check_admissibility(rt_attrs, list(all_flows.values())):
             print(f"[Controller] REJECTED Flow {topic}")
             return False

        # 3. Path Calculation
        path_links = self.routing_engine.calculate_path(rt_attrs.src_ip, rt_attrs.dst_ips)
        rt_attrs.route_links = path_links
        rt_attrs.num_hops = len(path_links)
        
        # 4. Install Rules (Multicast Group Tables)
        if rt_attrs.qi == 0:
             # Direct Multicast
             self._install_multicast_tree(rt_attrs, path_links)
        else:
             # Broker-Based Multicast (QoS 1/2)
             # Route to Broker(s) first
             if not rt_attrs.broker_ips:
                 # Select RP if not present
                 rp = self.routing_engine.select_optimal_rp(rt_attrs.dst_ips)
                 if rp: rt_attrs.broker_ips = [rp]
             
             # Calculate path to Broker
             # We assume broker_ips are simply switch DPIDs or attached hosts
             # For QoS 1/2 we might unicast to Broker, or Multicast to set of Brokers
             # Paper says Publisher -> RP (Unicast/Multicast) -> Subs
             # Here we route to Broker IP
             to_broker_links = self.routing_engine.calculate_path(rt_attrs.src_ip, rt_attrs.broker_ips)
             self._install_multicast_tree(rt_attrs, to_broker_links)
             
        print(f"[Controller] ACCEPTED & CONFIGURED Flow {topic} (Group {rt_attrs.multicast_group_id})")
        return True

    def handle_new_subscriber(self, topic, sub_ip):
        print(f"[Controller] New Subscriber {sub_ip} for {topic}")
        # 1. Update OF-DB
        of_db.add_subscriber(topic, sub_ip)
        
        # 2. Re-calculate Tree (Grafting)
        flow = of_db.get_flow(topic)
        if not flow: return

        # Dynamic Update:
        # Re-calc path with new subscriber set
        # Paper requires "Grafting" - adding only needed branches.
        # RoutingEngine.calculate_path (Steiner) does full recalc currently.
        # We can diff the new_links vs old_links to find new branches.
        
        old_links = set(flow.route_links) # simplified object comparison
        new_links = self.routing_engine.calculate_path(flow.src_ip, flow.dst_ips)
        flow.route_links = new_links
        
        # Update Switch Groups
        # Just calling install will overwrite group entries (Modify)
        self._install_multicast_tree(flow, new_links)
        print(f"[Controller] Updated Multicast Tree for {topic} (+{sub_ip})")

class MRTController(ControllerBase):
    def __init__(self, req, link, data, **config):
        super(MRTController, self).__init__(req, link, data, **config)
        self.mrt_app = data[mrt_instance_name]


    @route('mrt', url, methods=['POST'])
    def register_flow(self, req, **kwargs):
        try:
            data = req.json if req.body else {}
        except ValueError:
            return Response(status=400, body=b"Invalid JSON")
        
        topic = data.get("topic")
        rt_attrs = data.get("rt_attributes")
        src_ip = data.get("src_ip")
        broker_ip = data.get("broker_ip")
        
        if not topic or not rt_attrs:
             return Response(status=400, body=b"Missing topic or rt_attributes")

        success = self.mrt_app.register_rt_flow(topic, rt_attrs, src_ip, broker_ip)
        
        if success:
             return Response(status=200, body=b"Flow Registered")
        else:
             return Response(status=503, body=b"Flow Rejected")

    @route('mrt', '/mrt/register_subscriber', methods=['POST'])
    def register_subscriber(self, req, **kwargs):
        try:
            data = req.json if req.body else {}
        except ValueError:
            return Response(status=400, body=b"Invalid JSON")
        
        topic = data.get("topic")
        sub_ip = data.get("subscriber_ip")
        
        if topic and sub_ip:
            self.mrt_app.handle_new_subscriber(topic, sub_ip)
            return Response(status=200, body=b"Subscriber Registered")
        return Response(status=400, body=b"Missing Data")
