"""
Launch the task dispatcher in one of three modes:
  - local: Uses a multiprocessing pool to execute tasks locally.
  - pull: Uses a ZeroMQ REP socket to serve tasks on request.
  - push: Uses a ZeroMQ ROUTER socket to push tasks to waiting workers.

Usage:
    python3 task_dispatcher.py -m [local|pull|push] -p <port> -w <num_worker_processors>
"""
import zmq
import redis
import uuid
import json
import time
import argparse
from utils import serialize, deserialize
from multiprocessing import Pool
from queue import Queue
from threading import Thread

redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

#  perform deserialization and execute the function, return (task_id, status, result_payload)
def execute_task(task_id: str, fn_payload: str, param_payload: str):
    try:
        # Deserialize function and param payload
        fn = deserialize(fn_payload)
        params = deserialize(param_payload)
        args, kwargs = params
        # Execute the function
        result = fn(*args, **kwargs)
        return task_id, "COMPLETE", serialize(result)
    except Exception as e:
        err_result = f"Error executing task: {e}"
        return task_id, "FAILED", serialize(err_result)
    

def update_task(task_id, status, result):
    task_key = f"task:{task_id}"
    task_data = redis_client.get(task_key)
    if task_data:
        task_data = deserialize(task_data)
        task_data["status"] = status
        task_data["result"] = result
        redis_client.set(task_key, serialize(task_data))
    # print(f"Local mode: task {task_id} finished with {status} status")
    

def run_local_mode(num_workers):
    pubsub = redis_client.pubsub()
    pubsub.subscribe("Tasks")
    pool = Pool(processes=num_workers)

    for msg in pubsub.listen():
        # receive message from Tasks channle
        if msg["type"] != "message":
            continue
        task_id = msg["data"]
        print(f"Local Mode: Receive task id: {task_id}")
        # get task data from Redis
        task_key = f"task:{task_id}"
        task_data = redis_client.get(task_key)
        if not task_data:
            print(f"Local Mode:Task {task_id} not found in Redis")
            continue
        task_data = deserialize(task_data)
        # update the task status to RUNNING
        task_data["status"] = "RUNNING"
        redis_client.set(task_key, serialize(task_data))
        # Dispatch to a local worker process
        fn_payload = task_data["fn_payload"]
        param_payload = task_data["param_payload"]
        pool.apply_async(execute_task, args=(task_id, fn_payload, param_payload), callback=update_task)
    pool.close()
    pool.join()


# poll task with status "QUEUED" from Redis
def get_queued_task():
    for task_key in redis_client.scan_iter(match="task:*"):
        task_data = redis_client.get(task_key)
        if not task_data:
            continue
        task_data = deserialize(task_data)
        if task_data["status"] == "QUEUED":
            return task_data
    return None


# If a worker sends a request (JSON: {"type": "REQUEST"}), dispatcher polls Redis for a queued task
# If found, it updates status to RUNNING and sends it
# If a worker sends a result (JSON: {"type": "RESULT", ...}), dispatcher updates Redis and replies with an "ACK"
def handle_pull_mode(port):
    # setup the pull socket
    context = zmq.Context()
    pull_socket = context.socket(zmq.REP)
    pull_socket.bind(f"tcp://*:{port}")
    print(f"Pull Mode: listening on port {port}...")
    while True:
        message = pull_socket.recv()
        try:
            message = json.loads(message)
            # ------- requests a task -------
            if message["type"] == "REQUEST":
                task_data = get_queued_task()
                if task_data:
                    task_id = task_data["task_id"]
                    task_data["status"] = "RUNNING"
                    redis_client.set(f"task:{task_id}", serialize(task_data))
                    reply = {
                        "type": "TASK",
                        "task_id": task_id,
                        "fn_payload": task_data.get("fn_payload"),
                        "param_payload": task_data.get("param_payload")
                    }
                    # pull_socket.send(serialize(task_data))
                    print(f"Pull Mode: Sending task {task_id} to worker.")
                else:
                    reply = {
                        "type": "NO_TASK"
                    }
                    # pull_socket.send(b"")
                pull_socket.send(json.dumps(reply).encode())
            # ------- reply with an "ACK" -------
            elif message["type"] == "RESULT":
                task_id = message["task_id"]
                status = message["status"]
                result = message["result"]
                task_key = f"task:{task_id}"
                task_data = redis_client.get(task_key)
                if task_data:
                    task_data = deserialize(task_data)
                    task_data["status"] = status
                    task_data["result"] = result
                    redis_client.set(task_key, serialize(task_data))
                    print(f"Pull Mode: Task {task_id} finished with {status} status.")
                pull_socket.send(json.dumps({"type": "ACK"}).encode())
            else:
                pull_socket.send(b"")
        except Exception as e:
            print(f"Error in pull mode: {e}")


# Subscribe to "Tasks" channel, and enqueue incoming tasks into a thread-safe local queue.
# Workers (using DEALER sockets) send a "READY" message to show availability. 
# When both a queued task and an available worker exist, the dispatcher sends the task.
# If a worker sends back a result (type "RESULT"), the dispatcher updates Redis.
def handle_push_mode(port):
    task_queue = Queue()
    pubsub = redis_client.pubsub()
    pubsub.subscribe("Tasks")

    def redis_listener():
        for message in pubsub.listen():
            if message["type"] != "message":
                continue
            task_id = message["data"]
            print(f"Push Mode: Received task_id from Redis pubsub: {task_id}")
            task_key = f"task:{task_id}"
            task_data = redis_client.get(task_key)
            if task_data:
                task_data = json.loads(task_data)
                task_queue.put(task_data)

    listener_thread = Thread(target=redis_listener, daemon=True)
    listener_thread.start()
    # setup the push socket
    context = zmq.Context()
    push_socket = context.socket(zmq.ROUTER)
    push_socket.bind(f"tcp://*:{port}")
    print(f"Push Mode: listening on port {port}...")

    

if __name__ == '__main__':
    try:
        print("Task Dispatcher Service")
    except Exception as e:
        print(f"Error in main execution: {e}")