"""
Implements Pull Worker using a ZMQ REQ/REP pattern.
The worker repeatedly request tasks from task dispatcher, execute task, and return result.
Tasks are executed concurrently using a process pool.
"""
import argparse
import time
import json
import threading
import multiprocessing
import zmq
import dill
import codecs
from utils import serialize, deserialize, execute_task

def run_pull_worker(num_processes, dispatcher_url):
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(dispatcher_url)
    socket_lock = threading.Lock()

    # create a process pool for concurrent execution.
    pool = multiprocessing.Pool(processes=num_processes)

    # Register with the dispatcher.
    with socket_lock:
        reg_msg = {"type": "REGISTER", "worker_type": "pull"}
        socket.send_json(reg_msg)
        reg_ack = socket.recv_json()
        print("Pull Worker registered with dispatcher:", reg_ack)

    def process_and_send(task_data):
        task_id = task_data["task_id"]
        result_tuple = execute_task(task_id, task_data["fn_payload"], task_data["param_payload"])
        with socket_lock:
            result_msg = {
                "type": "RESULT",
                "task_id": result_tuple[0],
                "status": result_tuple[1],
                "result": result_tuple[2]
            }
            socket.send_json(result_msg)
            ack = socket.recv_json()
            print(f"Pull Worker: Result for task {task_id} acknowledged:", ack)

    # print(f"Pull Worker started with {num_processes} processes. Dispatcher: {dispatcher_url}")
    while True:
        with socket_lock:
            req_msg = {"type": "REQUEST"}
            socket.send_json(req_msg)
            reply = socket.recv_json()
        if reply.get("type") == "TASK":
            print("Pull Worker: Received task", reply["task_id"])
            pool.apply_async(process_and_send, args=(reply,))
        elif reply.get("type") == "NO_TASK":
            time.sleep(0.01)
        else:
            print("Pull Worker: Received unexpected message:", reply)
            time.sleep(0.1)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull Worker for FaaS")
    parser.add_argument("num_workers", type=int, help="Number of worker processes")
    parser.add_argument("dispatcher_url", type=str, help="Dispatcher URL (e.g., tcp://localhost:5555)")
    args = parser.parse_args()
    run_pull_worker(args.num_workers, args.dispatcher_url)
