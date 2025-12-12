
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import RemoteController, OVSKernelSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel
from mininet.link import TCLink

class MRTTopo(Topo):
    """
    Multi-Edge Network Topology
    3 Edge Networks (En-A, En-B, En-C) connected via a Core Switch.
    """
    def build(self):
        # Core Switch
        core = self.addSwitch('s100')
        
        # Edge Networks
        # En-A
        en_a = self.addSwitch('s1')
        self.addLink(en_a, core, bw=100, delay='1ms')
        
        # En-B
        en_b = self.addSwitch('s2')
        self.addLink(en_b, core, bw=100, delay='1ms')
        
        # En-C
        en_c = self.addSwitch('s3')
        self.addLink(en_c, core, bw=100, delay='1ms')
        
        # Hosts in En-A (Assume Publisher/Broker loc)
        h1 = self.addHost('h1', ip='10.0.0.1')
        self.addLink(h1, en_a, bw=10)
        
        h2 = self.addHost('h2', ip='10.0.0.2') # Broker A
        self.addLink(h2, en_a, bw=100)
        
        # Hosts in En-B (Subscriber)
        h3 = self.addHost('h3', ip='10.0.0.3')
        self.addLink(h3, en_b, bw=10)
        
        # Hosts in En-C (Subscriber)
        h4 = self.addHost('h4', ip='10.0.0.4')
        self.addLink(h4, en_c, bw=10)

def run():
    topo = MRTTopo()
    # Note: Replace IP with actual controller IP if external
    net = Mininet(topo=topo, controller=RemoteController('c0', ip='127.0.0.1', port=6633), switch=OVSKernelSwitch, link=TCLink)
    
    net.start()
    print("[Mininet] Topology Started. Type 'exit' to stop.")
    
    # Configure simple routing for test (normally handled by pure SDN app)
    # net['h1'].cmd('route add default gw ...') 
    
    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()
