import zmq
import redis
import uuid
import json
import time
import argparse

redis_client = redis.Redis(host="localhost", port=6379)
context = zmq.Context()

if __name__ == '__main__':
    try:
        print("Task Dispatcher Service")
    except Exception as e:
        print(f"Error in main execution: {e}")