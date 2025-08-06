#!/usr/bin/env python3
"""
Validation test script for the enhanced CometBFT height exporter.
Compares the exporter's aggregated results against a trusted reference RPC.
"""

import time
import requests
import threading
import json
from collections import defaultdict
from statistics import mean
import sys
import signal

# Configuration
REFERENCE_RPC = "https://cosmos-rpc.polkachu.com"  # Trusted public RPC
TEST_DURATION = 60  # seconds
POLL_INTERVAL = 1   # seconds
EXPORTER_METRICS_URL = "http://localhost:8000/metrics"

class ValidationTest:
    def __init__(self):
        self.running = False
        self.reference_data = []
        self.exporter_data = []
        self.start_time = None
        
    def fetch_reference_height(self):
        """Fetch height from reference RPC"""
        try:
            response = requests.get(f"{REFERENCE_RPC}/status", timeout=5)
            response.raise_for_status()
            data = response.json()
            height = int(data['result']['sync_info']['latest_block_height'])
            block_time = data['result']['sync_info']['latest_block_time']
            return height, block_time
        except Exception as e:
            print(f"Error fetching reference: {e}")
            return None, None
            
    def fetch_exporter_metrics(self):
        """Fetch aggregated height from exporter metrics"""
        try:
            response = requests.get(EXPORTER_METRICS_URL, timeout=5)
            response.raise_for_status()
            
            # Parse Prometheus metrics for cosmoshub-4 max height
            for line in response.text.split('\n'):
                if 'blockchain_latest_block_height_max{network="cosmoshub-4"}' in line:
                    height = float(line.split()[-1])
                    return int(height)
            return None
        except Exception as e:
            print(f"Error fetching exporter metrics: {e}")
            return None
    
    def reference_worker(self):
        """Worker thread to poll reference RPC every second"""
        print(f"Starting reference worker (polling {REFERENCE_RPC})")
        
        while self.running:
            timestamp = time.time()
            height, block_time = self.fetch_reference_height()
            
            if height:
                self.reference_data.append({
                    'timestamp': timestamp,
                    'height': height,
                    'block_time': block_time,
                    'elapsed': timestamp - self.start_time
                })
                print(f"REF  [{timestamp - self.start_time:6.1f}s] Height: {height}")
            
            time.sleep(POLL_INTERVAL)
            
    def exporter_worker(self):
        """Worker thread to poll exporter metrics every second"""
        print("Starting exporter worker (polling local metrics)")
        
        # Wait a bit for exporter to start up
        time.sleep(5)
        
        while self.running:
            timestamp = time.time()
            height = self.fetch_exporter_metrics()
            
            if height:
                self.exporter_data.append({
                    'timestamp': timestamp,
                    'height': height,
                    'elapsed': timestamp - self.start_time
                })
                print(f"EXP  [{timestamp - self.start_time:6.1f}s] Height: {height}")
            
            time.sleep(POLL_INTERVAL)
    
    def run_test(self):
        """Run the validation test"""
        print(f"🚀 Starting {TEST_DURATION}s validation test")
        print(f"Reference RPC: {REFERENCE_RPC}")
        print(f"Exporter URL:  {EXPORTER_METRICS_URL}")
        print("=" * 60)
        
        self.running = True
        self.start_time = time.time()
        
        # Start worker threads
        ref_thread = threading.Thread(target=self.reference_worker, daemon=True)
        exp_thread = threading.Thread(target=self.exporter_worker, daemon=True)
        
        ref_thread.start()
        exp_thread.start()
        
        # Run for specified duration
        try:
            time.sleep(TEST_DURATION)
        except KeyboardInterrupt:
            print("\n⚠️  Test interrupted by user")
        
        self.running = False
        print("\n🏁 Test completed, analyzing results...")
        
        # Wait for threads to finish
        ref_thread.join(timeout=2)
        exp_thread.join(timeout=2)
        
        self.analyze_results()
    
    def analyze_results(self):
        """Analyze and compare the collected data"""
        print("\n📊 ANALYSIS RESULTS")
        print("=" * 60)
        
        if not self.reference_data:
            print("❌ No reference data collected!")
            return
            
        if not self.exporter_data:
            print("❌ No exporter data collected!")
            return
        
        print(f"📈 Reference samples: {len(self.reference_data)}")
        print(f"📈 Exporter samples:  {len(self.exporter_data)}")
        
        # Extract heights
        ref_heights = [d['height'] for d in self.reference_data]
        exp_heights = [d['height'] for d in self.exporter_data]
        
        print(f"\n🎯 REFERENCE RPC ({REFERENCE_RPC})")
        print(f"   Min height: {min(ref_heights)}")
        print(f"   Max height: {max(ref_heights)}")
        print(f"   Height range: {max(ref_heights) - min(ref_heights)} blocks")
        
        print(f"\n🔧 EXPORTER AGGREGATED")
        print(f"   Min height: {min(exp_heights)}")
        print(f"   Max height: {max(exp_heights)}")
        print(f"   Height range: {max(exp_heights) - min(exp_heights)} blocks")
        
        # Compare alignment
        ref_max = max(ref_heights)
        exp_max = max(exp_heights)
        height_diff = abs(ref_max - exp_max)
        
        print(f"\n⚖️  COMPARISON")
        print(f"   Reference max height: {ref_max}")
        print(f"   Exporter max height:  {exp_max}")
        print(f"   Difference: {height_diff} blocks")
        
        if height_diff == 0:
            print("   ✅ PERFECT ALIGNMENT!")
        elif height_diff <= 1:
            print("   ✅ EXCELLENT (≤1 block difference)")
        elif height_diff <= 3:
            print("   ⚠️  GOOD (≤3 block difference)")
        else:
            print("   ❌ POOR (>3 block difference)")
        
        # Time-series comparison
        print(f"\n⏱️  TIME SERIES ANALYSIS")
        
        # Find overlapping time windows for comparison
        overlaps = 0
        perfect_matches = 0
        close_matches = 0
        
        for exp_sample in self.exporter_data:
            exp_time = exp_sample['timestamp']
            exp_height = exp_sample['height']
            
            # Find closest reference sample within 2 seconds
            closest_ref = None
            min_time_diff = float('inf')
            
            for ref_sample in self.reference_data:
                time_diff = abs(ref_sample['timestamp'] - exp_time)
                if time_diff < min_time_diff and time_diff <= 2:
                    min_time_diff = time_diff
                    closest_ref = ref_sample
            
            if closest_ref:
                overlaps += 1
                ref_height = closest_ref['height']
                height_diff = abs(exp_height - ref_height)
                
                if height_diff == 0:
                    perfect_matches += 1
                elif height_diff <= 1:
                    close_matches += 1
        
        if overlaps > 0:
            perfect_rate = (perfect_matches / overlaps) * 100
            close_rate = ((perfect_matches + close_matches) / overlaps) * 100
            
            print(f"   Overlapping samples: {overlaps}")
            print(f"   Perfect matches: {perfect_matches} ({perfect_rate:.1f}%)")
            print(f"   Close matches (≤1): {close_matches} ({close_rate:.1f}%)")
            
            if perfect_rate >= 80:
                print("   ✅ EXCELLENT time alignment")
            elif close_rate >= 90:
                print("   ✅ GOOD time alignment")
            else:
                print("   ⚠️  FAIR time alignment")
        
        print("\n" + "=" * 60)

def main():
    if len(sys.argv) > 1 and sys.argv[1] == '--help':
        print("Usage: python test_validator.py")
        print("Make sure the exporter is running with test-validation.yaml config")
        print("This will compare exporter results against bryanlabs reference RPC")
        return
    
    validator = ValidationTest()
    
    # Handle Ctrl+C gracefully
    def signal_handler(signum, frame):
        print("\n⚠️  Received interrupt signal")
        validator.running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        validator.run_test()
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())