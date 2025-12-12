# MRT-MQTT Paper Compliance Report

This document maps the 16 Requirement Points to the specific implementation details.

## 1. ORT-NM & SRT
- **Req 1 (Strict MQTTv5)**: `ort_nm.py:extract_rt_attributes` now enforces keys `Ci, Pi, Ti, Di, BWi` exactly.
- **Req 2 (Subscriber Handling)**: `ort_nm.py:handle_subscribe_packet` added to intercept SUBSCRIBE.
- **Req 3 (SRT Structure)**: `rt_attributes.py:RTAttributes` updated with `broker_ips` (BAi), `dst_ips` (DSTi,k), and `multicast_group_id`.

## 2. Advanced Multicast
- **Req 4 (Group Tables)**: `ryu_mrt_app.py:_install_multicast_tree` uses `OFPGroupMod` (Type ALL) for fan-out.
- **Req 5 (RP Routing)**: `routing.py:select_optimal_rp` calculates Centrality. `ryu_mrt_app` routes to RP for QoS 1/2.
- **Req 13 (Grafting)**: `ryu_mrt_app.py:handle_new_subscriber` recalculates links and calls `_install_multicast_tree` to update groups.

## 3. Broker Workflows
- **Req 6 (Join/ACK)**: `broker_agent.py` implements `join_multicast_group` and `acknowledge_receipt`.
- **Req 8 (QoS Redirection)**: `publisher.py` supports `--multicast_dst` for QoS 0 (UDP) and standard Broker Connect for QoS 1/2.
- **Req 9 (Broker Logic)**: `broker_agent.py` acts as the edge relay.
- **Req 7 (MSDP)**: `ryu_mrt_app` broadcasts MSDP events on Flow Registration.

## 4. Advanced Analysis
- **Req 10 (Real Measurement)**: `monitor.py` now uses `scapy` to send ICMP probes and measure real RTT ($D_{sw}$).
- **Req 11 (Processing Delay)**: `analysis.py` adds `flow.processing_delay` ($T_{proc}$) to WCRT.
- **Req 12 (Multicast TA)**: `analysis.py:calculate_wcrt` uses `_get_max_branch_wcrt` to find determining path in Multicast Tree.

## 5. Automation
- **Req 14 (Atomic)**: Flow mods installed in batch per switch.
- **Req 15 (Automation)**: `simulation/experiment_runner.py` orchestrates Mininet, Controller, and Clients.
- **Req 16 (Logging)**: Runner generates `experiment_results.csv` and `wcrt_plot.png`.

---
**Status**: 100% Compliant.
