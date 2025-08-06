#!/usr/bin/env python3
"""
Demo test to show the enhanced CometBFT height exporter in action.
Shows individual endpoint polling, staggering, and aggregation.
"""

import time
import requests
import sys
import signal

def fetch_metrics():
    """Fetch and parse metrics from exporter"""
    try:
        response = requests.get("http://localhost:8000/metrics", timeout=3)
        response.raise_for_status()
        
        individual_heights = {}
        max_height = None
        response_times = {}
        healthy_count = 0
        paused_429_count = 0
        paused_error_count = 0
        
        for line in response.text.split('\n'):
            # Individual endpoint heights
            if 'blockchain_latest_block_height{' in line and 'node=' in line:
                # Extract node URL and height
                parts = line.split('"')
                if len(parts) >= 4:
                    node = parts[3]  # The URL between quotes
                    height = int(float(line.split()[-1]))
                    individual_heights[node] = height
            
            # Max aggregated height
            elif 'blockchain_latest_block_height_max{network="cosmoshub-4"}' in line:
                max_height = int(float(line.split()[-1]))
                
            # Response times
            elif 'blockchain_rpc_response_time_seconds{' in line:
                parts = line.split('"')
                if len(parts) >= 4:
                    node = parts[3]
                    resp_time = float(line.split()[-1])
                    response_times[node] = resp_time
            
            # Healthy endpoints count
            elif 'blockchain_healthy_endpoints_count{network="cosmoshub-4"}' in line:
                healthy_count = int(float(line.split()[-1]))
            
            # Paused endpoints counts
            elif 'blockchain_paused_endpoints_count{network="cosmoshub-4",reason="rate_limiting"}' in line:
                paused_429_count = int(float(line.split()[-1]))
            elif 'blockchain_paused_endpoints_count{network="cosmoshub-4",reason="errors"}' in line:
                paused_error_count = int(float(line.split()[-1]))
        
        return individual_heights, max_height, response_times, healthy_count, paused_429_count, paused_error_count
        
    except Exception as e:
        print(f"Error fetching metrics: {e}")
        return {}, None, {}, 0, 0, 0

def main():
    print("🚀 CometBFT Enhanced Height Exporter Demo")
    print("=" * 60)
    print("Showing individual endpoint polling, staggering, and aggregation")
    print("Press Ctrl+C to stop")
    print()
    
    running = True
    
    def signal_handler(signum, frame):
        nonlocal running
        running = False
        print("\n👋 Demo stopped")
    
    signal.signal(signal.SIGINT, signal_handler)
    
    iteration = 0
    while running:
        iteration += 1
        print(f"\n📊 Iteration {iteration} - {time.strftime('%H:%M:%S')}")
        
        individual, max_height, response_times, healthy_count, paused_429, paused_error = fetch_metrics()
        
        if not individual:
            print("⚠️  No metrics available yet (exporter might be starting up)")
            time.sleep(2)
            continue
        
        # Show individual endpoints
        total_endpoints = len(individual) + paused_429 + paused_error
        paused_info = f", Paused: {paused_429 + paused_error}" if (paused_429 + paused_error) > 0 else ""
        print(f"Endpoints (Total: {total_endpoints}, Active: {len(individual)}, Healthy: {healthy_count}{paused_info}):")
        
        if paused_429 + paused_error > 0:
            print(f"  ⏸️  Paused: {paused_429} rate-limited (429), {paused_error} other errors")
        heights = []
        for node, height in sorted(individual.items()):
            resp_time = response_times.get(node, 0)
            short_node = node.split('/')[-1] if '/' in node else node[-20:]
            print(f"  {short_node:25} | Height: {height:>9} | Response: {resp_time:5.2f}s")
            heights.append(height)
        
        # Show aggregation
        if heights:
            min_h, max_h = min(heights), max(heights)
            drift = max_h - min_h
            print(f"\nAggregation:")
            print(f"  Min Height: {min_h:>9}")
            print(f"  Max Height: {max_h:>9}  ← This is exported as aggregated metric")
            print(f"  Drift:      {drift:>9} blocks")
            
            if drift == 0:
                print("  Status:     ✅ Perfect sync!")
            elif drift <= 2:
                print("  Status:     ✅ Excellent sync")
            elif drift <= 5:
                print("  Status:     ⚠️  Good sync") 
            else:
                print("  Status:     ❌ Poor sync - check RPCs")
        
        # Show benefits of enhanced system
        if iteration == 1:
            registry_count = max(0, len(individual) - 3)  # Assume 3 manual endpoints
            print(f"\n💡 Benefits of Enhanced System:")
            print(f"   • Individual intervals: Fast endpoints (1s) + slower ones (2s+)")
            print(f"   • Staggered start: No thundering herd on RPC providers") 
            print(f"   • Real-time data: Always fresh height from fastest endpoints")
            print(f"   • Auto-discovery: Chain registry adds {registry_count} extra RPCs")
            print(f"   • Smart pausing: Auto-pause RPCs with 429s or persistent errors")
            print(f"   • Automatic recovery: Paused endpoints resume after cooldown period")
        
        time.sleep(3)  # Update every 3 seconds for demo
    
    return 0

if __name__ == "__main__":
    sys.exit(main())