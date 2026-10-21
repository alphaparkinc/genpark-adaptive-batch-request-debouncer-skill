"""Example usage for GenPark Adaptive Batch Request Debouncer."""
from client import AdaptiveBatchDebouncer
import time

def main():
    debouncer = AdaptiveBatchDebouncer(default_max_batch_size=3, default_max_wait_ms=200.0)
    print("Ingesting streaming search requests...")
    debouncer.add_request("search_coalesce", "req_101", {"query": "vector databases"}, priority=1.0)
    debouncer.add_request("search_coalesce", "req_102", {"query": "model context protocol"}, priority=2.5)

    print("Checking flush before size limit or timeout...")
    flushed = debouncer.flush_ready_batches(max_batch_size=3, max_wait_ms=1000.0)
    print(f"Flushed count (expected 0): {len(flushed)}")

    print("Adding 3rd request to trigger threshold...")
    debouncer.add_request("search_coalesce", "req_103", {"query": "agentic workflows"}, priority=5.0)
    flushed = debouncer.flush_ready_batches(max_batch_size=3, max_wait_ms=1000.0)
    print(f"Flushed count (expected 1): {len(flushed)}")
    print(f"First prioritized item in batch: {flushed[0]['items'][0]['request_id']}")

    metrics = debouncer.get_batch_metrics()
    print("Debouncer Metrics:", metrics)

if __name__ == "__main__":
    main()
