"""Adaptive Batch Request Debouncer.
100% Python Standard Library.
"""
import time
from typing import Dict, Any, List, Optional

class AdaptiveBatchDebouncer:
    """Coalesces and debounces incoming streaming requests into optimized micro-batches."""
    def __init__(self, default_max_batch_size: int = 10, default_max_wait_ms: float = 250.0):
        self.default_max_batch_size = default_max_batch_size
        self.default_max_wait_ms = default_max_wait_ms
        self.queues: Dict[str, List[Dict[str, Any]]] = {}
        self.queue_meta: Dict[str, Dict[str, Any]] = {}
        self.stats = {
            "total_requests": 0,
            "total_batches_flushed": 0,
            "total_items_flushed": 0
        }

    def add_request(self, batch_id: str, request_id: str, payload: Any, priority: float = 1.0) -> Dict[str, Any]:
        now = time.time() * 1000.0
        if batch_id not in self.queues:
            self.queues[batch_id] = []
            self.queue_meta[batch_id] = {
                "created_at_ms": now,
                "first_item_at_ms": now,
                "last_item_at_ms": now
            }

        item = {
            "request_id": request_id,
            "payload": payload,
            "priority": priority,
            "queued_at_ms": now
        }
        self.queues[batch_id].append(item)
        self.queue_meta[batch_id]["last_item_at_ms"] = now
        self.stats["total_requests"] += 1

        return {
            "status": "queued",
            "batch_id": batch_id,
            "request_id": request_id,
            "queue_depth": len(self.queues[batch_id]),
            "queued_at_ms": now
        }

    def flush_ready_batches(self, max_batch_size: Optional[int] = None, max_wait_ms: Optional[float] = None) -> List[Dict[str, Any]]:
        size_limit = max_batch_size or self.default_max_batch_size
        wait_limit = max_wait_ms or self.default_max_wait_ms
        now = time.time() * 1000.0

        flushed = []
        ready_batch_ids = []

        for b_id, items in list(self.queues.items()):
            if not items:
                continue
            meta = self.queue_meta.get(b_id, {})
            age = now - meta.get("first_item_at_ms", now)
            if len(items) >= size_limit or age >= wait_limit:
                ready_batch_ids.append(b_id)

        for b_id in ready_batch_ids:
            items = self.queues[b_id]
            items.sort(key=lambda x: (-x["priority"], x["queued_at_ms"]))
            chunk = items[:size_limit]
            self.queues[b_id] = items[size_limit:]

            batch_res = {
                "batch_id": b_id,
                "item_count": len(chunk),
                "items": chunk,
                "flushed_at_ms": now,
                "remaining_depth": len(self.queues[b_id])
            }
            if not self.queues[b_id]:
                del self.queues[b_id]
                del self.queue_meta[b_id]
            else:
                self.queue_meta[b_id]["first_item_at_ms"] = now

            self.stats["total_batches_flushed"] += 1
            self.stats["total_items_flushed"] += len(chunk)
            flushed.append(batch_res)

        return flushed

    def get_batch_metrics(self) -> Dict[str, Any]:
        flushed_batches = self.stats["total_batches_flushed"]
        flushed_items = self.stats["total_items_flushed"]
        avg_batch_size = (flushed_items / flushed_batches) if flushed_batches > 0 else 0.0
        return {
            "total_requests": self.stats["total_requests"],
            "total_batches_flushed": flushed_batches,
            "total_items_flushed": flushed_items,
            "average_batch_size": round(avg_batch_size, 2),
            "active_batch_queues": len(self.queues),
            "pending_requests": sum(len(q) for q in self.queues.values())
        }

    def clear_batches(self) -> Dict[str, Any]:
        count = sum(len(q) for q in self.queues.values())
        self.queues.clear()
        self.queue_meta.clear()
        return {"status": "cleared", "purged_requests": count}
