"""MCP Stdio Server for Adaptive Batch Request Debouncer.
100% Python Standard Library.
"""
import sys
import json
from client import AdaptiveBatchDebouncer

debouncer = AdaptiveBatchDebouncer()

def handle_rpc(line):
    try:
        req = json.loads(line)
    except Exception:
        return
    req_id = req.get("id")
    method = req.get("method")
    params = req.get("params", {})

    if method == "initialize":
        res = {
            "protocolVersion": "2024-11-05",
            "serverInfo": {"name": "genpark-adaptive-batch-request-debouncer-skill", "version": "1.0.1"},
            "capabilities": {"tools": {}}
        }
        send_response(req_id, res)
    elif method == "tools/list":
        tools = [
            {
                "name": "add_request",
                "description": "Ingest a request into the debouncer window",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "batch_id": {"type": "string"},
                        "request_id": {"type": "string"},
                        "payload": {"type": "object"},
                        "priority": {"type": "number", "default": 1.0}
                    },
                    "required": ["batch_id", "request_id", "payload"]
                }
            },
            {
                "name": "flush_ready_batches",
                "description": "Flush micro-batches satisfying size thresholds or wait timeouts",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "max_batch_size": {"type": "integer"},
                        "max_wait_ms": {"type": "number"}
                    }
                }
            },
            {
                "name": "get_batch_metrics",
                "description": "Retrieve debouncer throughput, queue depth, and compression metrics",
                "inputSchema": {"type": "object"}
            },
            {
                "name": "clear_batches",
                "description": "Purge all active request queues",
                "inputSchema": {"type": "object"}
            }
        ]
        send_response(req_id, {"tools": tools})
    elif method == "tools/call":
        name = params.get("name")
        args = params.get("arguments", {})
        if name == "add_request":
            out = debouncer.add_request(args.get("batch_id"), args.get("request_id"), args.get("payload"), args.get("priority", 1.0))
        elif name == "flush_ready_batches":
            out = debouncer.flush_ready_batches(args.get("max_batch_size"), args.get("max_wait_ms"))
        elif name == "get_batch_metrics":
            out = debouncer.get_batch_metrics()
        elif name == "clear_batches":
            out = debouncer.clear_batches()
        else:
            send_error(req_id, -32601, f"Unknown tool: {name}")
            return
        send_response(req_id, {"content": [{"type": "text", "text": json.dumps(out, indent=2)}]})

def send_response(req_id, result):
    sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": req_id, "result": result}) + "\n")
    sys.stdout.flush()

def send_error(req_id, code, message):
    sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}) + "\n")
    sys.stdout.flush()

def main():
    for line in sys.stdin:
        if line.strip():
            handle_rpc(line.strip())

if __name__ == "__main__":
    main()
