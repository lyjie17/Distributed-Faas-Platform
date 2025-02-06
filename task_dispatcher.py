import zmq
import redis
import uuid
import json
import time
import argparse
from utils import serialize, deserialize

redis_client = redis.Redis(host="localhost", port=6379)
context = zmq.Context()


#  perform deserialization and execute the function, return (task_id, status, result_payload)
def execute_task(task_id: uuid.UUID, fn_payload: str, param_payload: str):
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
    

if __name__ == '__main__':
    try:
        print("Task Dispatcher Service")
    except Exception as e:
        print(f"Error in main execution: {e}")