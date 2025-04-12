"""
Implements a Push Worker for MPCSFaaS using a ZMQ DEALER/ROUTER pattern.
The worker sends an initial READY message, processes incoming tasks via a process pool,
and periodically sends heartbeat messages.
"""

import argparse
import time
import threading
import multiprocessing
import zmq
from utils import serialize, deserialize, execute_task

def run_push_worker(num_processes, dispatcher_url, worker_id):
    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    socket.setsockopt_string(zmq.IDENTITY, worker_id)
    socket.connect(dispatcher_url)
    socket_lock = threading.Lock()

    pool = multiprocessing.Pool(processes=num_processes)

    # send initial READY registration
    with socket_lock:
        ready_msg = {"type": "READY", "worker_type": "push", "worker_id": worker_id}
        socket.send_json(ready_msg)
    print(f"push Worker {worker_id} registered with dispatcher at {dispatcher_url}")

    # heartbeat thread: send heartbeat messages periodically.
    def heartbeat():
        while True:
            time.sleep(2)
            with socket_lock:
                hb_msg = {"type": "HEARTBEAT", "worker_id": worker_id}
                socket.send_json(hb_msg)
                try:
                    ack = socket.recv_json(flags=zmq.NOBLOCK)
                    print(f"push Worker {worker_id} heartbeat ack:", ack)
                except zmq.Again:
                    pass
    hb_thread = threading.Thread(target=heartbeat, daemon=True)
    hb_thread.start()

    def process_and_send(task_data):
        task_id = task_data["task_id"]
        result_tuple = execute_task(task_id, task_data["fn_payload"], task_data["param_payload"])
        with socket_lock:
            result_msg = {
                "type": "RESULT",
                "task_id": result_tuple[0],
                "status": result_tuple[1],
                "result": result_tuple[2],
                "worker_id": worker_id
            }
            socket.send_json(result_msg)
            ack = socket.recv_json()
            print(f"push Worker {worker_id}: Result for task {task_id} acknowledged:", ack)

    print(f"push Worker {worker_id} started with {num_processes} processes. Waiting for tasks...")
    while True:
        with socket_lock:
            try:
                msg = socket.recv_json(flags=zmq.NOBLOCK)
            except zmq.Again:
                msg = None
        if msg is not None:
            if msg.get("type") == "TASK":
                print(f"Push Worker {worker_id}: Received task {msg['task_id']}")
                pool.apply_async(process_and_send, args=(msg,))
            else:
                print(f"Push Worker {worker_id}: Received unknown message:", msg)
        else:
            time.sleep(0.01)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Push Worker for MPCSFaaS")
    parser.add_argument("num_workers", type=int, help="Number of worker processes")
    parser.add_argument("dispatcher_url", type=str, help="Dispatcher URL (e.g., tcp://localhost:5555)")
    parser.add_argument("--id", type=str, default="worker-push-1", help="Worker ID (default: worker-push-1)")
    args = parser.parse_args()
    run_push_worker(args.num_workers, args.dispatcher_url, args.id)