import networkx as nx
from networkx.algorithms.approximation import steinertree
from common.of_db import of_db
from common.rt_attributes import Link

class RoutingEngine:
    """
    Implements Routing Algorithms:
    - Delay-Aware Dijkstra for Unicast
    - Steiner Tree for Multicast (simplified)
    - Cost Function based on Paper Eq (1)
    """
    
    def __init__(self):
        # Graph is rebuilt from OF-DB on each calculation to ensure freshness
        pass

    def _build_graph(self):
        """Construct NetworkX graph from OF-DB links."""
        G = nx.Graph()
        links = of_db.links # Dict[key, Link]
        for key, link in links.items():
            # Cost = Prop + Switch + Proc + Queuing (Approximated 0 here, handled in schedulability)
            # Paper Eq (1) focuses on selecting paths with least delay/cost.
            # We use static delays + utilization logic as base weight.
            cost = link.prop_delay + link.switch_delay + link.proc_delay
            # Add utilization penalty: Cost / (1 - U)
            utilization = link.bw_used / link.bw_capacity if link.bw_capacity > 0 else 0.99
            if utilization >= 1.0: utilization = 0.99
            
            final_cost = cost / (1 - utilization)
            
            G.add_edge(link.src, link.dst, weight=final_cost, link_obj=link)
        return G

    def calculate_path(self, src: str, dsts: list):
        """
        Calculate optimal path(s) from src to one or multiple dsts.
        Returns flattened list of Link objects (the tree/path).
        """
        G = self._build_graph()
        
        # Ensure nodes exist
        if src not in G: 
            print(f"[Routing] Error: Source {src} not in topology.")
            return []
        
        valid_dsts = [d for d in dsts if d in G]
        if not valid_dsts:
            print(f"[Routing] Error: No valid destinations in topology.")
            return []

        path_links = []

        if len(valid_dsts) == 1:
            # Unicast -> Dijkstra
            dst = valid_dsts[0]
            try:
                path_nodes = nx.dijkstra_path(G, src, dst, weight='weight')
                # Convert node path to Link list
                path_links = self._nodes_to_links(G, path_nodes)
            except nx.NetworkXNoPath:
                print(f"[Routing] No path found from {src} to {dst}")
                return []
        else:
            # Multicast -> Steiner Tree
            # Terminals = {src} U {dsts}
            terminals = [src] + valid_dsts
            try:
                st_tree = steinertree.steiner_tree(G, terminals, weight='weight')
                # Extract edges from tree
                for u, v in st_tree.edges():
                    if G.has_edge(u, v):
                        link_obj = G[u][v]['link_obj']
                        path_links.append(link_obj)
            except Exception as e:
                print(f"[Routing] Steiner Tree failed: {e}")
                # Fallback: Union of Unicast Paths
                for dst in valid_dsts:
                    try:
                        p_nodes = nx.dijkstra_path(G, src, dst, weight='weight')
                        p_links = self._nodes_to_links(G, p_nodes)
                        for l in p_links:
                            if l not in path_links: path_links.append(l)
                    except: pass

        return path_links

    def select_optimal_rp(self, subscribers: list) -> str:
        """
        Selects the Rendezvous Point (RP) based on Centrality (Minimize Max Distance).
        In MRT-MQTT, RP should be central to subscribers to minimize latency variance.
        """
        G = self._build_graph()
        if not G.nodes: return None
        
        # Valid candidates are Switches (exclude hosts if possible, but here nodes are mixed)
        # We assume switches have integer DPIDs or specific naming convention.
        # For safety, let's consider all nodes in topology as candidates initially.
        
        best_rp = None
        min_max_dist = float('inf')
        
        valid_subs = [s for s in subscribers if s in G]
        if not valid_subs: return None
        
        for node in G.nodes():
            # Calculate max distance to any subscriber
            try:
                max_dist = 0
                for sub in valid_subs:
                    dist = nx.dijkstra_path_length(G, node, sub, weight='weight')
                    if dist > max_dist:
                        max_dist = dist
                
                if max_dist < min_max_dist:
                    min_max_dist = max_dist
                    best_rp = node
            except nx.NetworkXNoPath:
                continue
                
        return best_rp

    def _nodes_to_links(self, G, nodes):
        links = []
        for i in range(len(nodes)-1):
            u, v = nodes[i], nodes[i+1]
            if G.has_edge(u, v):
                links.append(G[u][v]['link_obj'])
        return links
