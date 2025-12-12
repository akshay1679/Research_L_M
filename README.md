# Research_L_M
A Scalable Real-Time SDN-Based MQTT Framework for Industrial Applications
# MRT-MQTT: Scalable Real-Time SDN-Based MQTT Framework

This repository contains the complete implementation of the **MRT-MQTT Framework** as described in the associated research paper. It transforms standard MQTT into a real-time, delay-aware protocol using Software-Defined Networking (SDN).

## 🚀 Key Features
- **Deterministic Latency**: Guarantees deadlines using **Holistic (HA)** and **Trajectory (TA)** schedulability analysis.
- **SDN-Based Routing**: Centralized Controller (Ryu) calculates optimal paths based on dynamic link delays and jitter.
- **Full Multicast Support**: Implements **IP Multicast routing** (Steiner Tree), **MSDP** for inter-broker discovery, and **OpenFlow Group Tables**.
- **QoS Enforcement**: Uses OpenFlow **Meters** (Bandwidth Reservation) and **Priority Queues**.
- **Real-Time Monitoring**: Live measurement of Switch Delay ($D_{sw}$) and Jitter ($J_{sd}$) to refine admission control.

---

## 📂 Project Structure

```
implementation/mrt_mqtt_v2/
├── common/                  # Shared Data Structures
│   ├── rt_attributes.py     # RTAttributes (FiTS), Link, Switch types
│   └── of_db.py             # Global In-Memory OF-DB (Topology & Flows)
│
├── sdn_controller/          # SDN Control Plane (Ryu)
│   ├── ryu_mrt_app.py       # Main Controller App (Topology, Rules, REST API)
│   ├── routing.py           # Routing Engine (Dijkstra, Steiner Tree, Centrality RP)
│   └── msdp.py              # MSDP Signaling for Inter-Broker Discovery
│
├── schedulability/          # Real-Time Analysis Engine
│   ├── analysis.py          # WCRT Analysis (HA & TA algorithms)
│   └── admission.py         # Admission Control Logic
│
├── ort_nm/                  # Optimized Real-Time Network Manager
│   └── ort_nm.py            # Intercepts MQTT, registers subscribers, notifies Controller
│
├── mqtt_clients/            # MQTT Components
│   ├── publisher.py         # RT-Aware Publisher (sends User Properties)
│   └── broker_agent.py      # Sidecar Agent for Multicast & Local Republishing
│
├── simulation/              # Experiments & Verification
│   ├── monitor.py           # Real-time Network Jitter/Delay Monitor
│   ├── experiment_setup.py  # Basic WCRT Validation Script
│   └── full_verification.py # Advanced Full-System Verification Script
│
└── README.md                # This Documentation
```

---

## ⚙️ Algorithms Implemented

### 1. Schedulability Analysis (`schedulability/analysis.py`)
We implement two response time analysis methods to guarantee $WCRT \le Deadline$:
*   **Holistic Approach (HA)**: Calculates iterative interference $w = C_i + \sum I(w)$ accounting for blocking and jitter.
*   **Trajectory Approach (TA)**: A tighter bound analysis that models packet trajectory hop-by-hop (Section IV.B of paper).

### 2. Delay-Aware Routing (`sdn_controller/routing.py`)
Cost function minimizes latency variance:
$$ Cost(L) = \frac{Prop + Switch + Proc}{1 - Utilization} $$
*   **Unicast**: Dijkstra's Algorithm.
*   **Multicast**: Steiner Tree Approximation.
*   **RP Selection**: Selects Broker based on network centrality (minimizing max distance to subscribers).

### 3. Advanced Multicast
*   **MSDP (`msdp.py`)**: Brokers exchange "Source Active" messages to discover topics in other domains.
*   **Broker Agent (`broker_agent.py`)**: Joins multicast groups on behalf of the broker and republishes traffic locally, verifying **Processing Delay ($T_{proc}$)**.

---

## 🛠️ Installation & Setup

### Prerequisites
*   Python 3.9+
*   Ryu SDN Controller
*   Mosquitto MQTT Broker
*   Mininet (for topology simulation)

### 1. Setup Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install ryu networkx paho-mqtt requests numpy
```

### 2. Start SDN Controller
The controller handles topology, routing, and flow admission.
```bash
ryu-manager sdn_controller/ryu_mrt_app.py
```

### 3. Start ORT-NM (Network Manager)
Connects to the MQTT Broker to intercept new flows.
```bash
python ort_nm/ort_nm.py
```

### 4. Start Real-Time Monitor
(Optional) Starts the background monitor for jitter measurements.
```bash
python simulation/monitor.py
```

---

## 🧪 Verification & Experiments

### System Validation
To verify that all components (Routing, Analysis, Multicast, QoS) work together:
```bash
python simulation/full_verification.py
```
**Expected Output:**
```
[Monitor] Network Monitor Started.
[Test] Creating Delay-Sensitive Flow...
Calculated Path Length: 2
WCRT (with Measured Jitter/Delay): 11.53ms
-> Flow ADMITTED.
```

### WCRT Model Validation
To validate the mathematical correctness of HA vs TA models:
```bash
python simulation/experiment_setup.py
```

---

## 📡 API Reference

### Register Flow (`POST /mrt/register_flow`)
Found in `ryu_mrt_app.py`. Called by ORT-NM when a Publisher announces a new Topic.
**Body:**
```json
{
  "topic": "sensor/temp",
  "src_ip": "10.0.0.1",
  "rt_attributes": {
    "qi": 1, "pi": 5, "ci": 2.0, "ti": 50, "di": 20, "bwi": "1Mbps"
  }
}
```

### Register Subscriber (`POST /mrt/register_subscriber`)
Called when a new subscriber joins a topic. Triggers Dynamic Multicast Tree updates.
**Body:**
```json
{
  "topic": "sensor/temp",
  "subscriber_ip": "10.0.0.5"
}
```

---
**Authors**: MRT-MQTT Research Team
