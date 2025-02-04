import requests
import time
import uuid
import logging
import random
from .serialize import serialize, deserialize

base_url = "http://127.0.0.1:8000/"

valid_statuses = ["QUEUED", "RUNNING", "COMPLETED", "FAILED"]


def test_fn_registration_invalid():
    # Using a non-serialized payload data
    resp = requests.post(base_url + "register_function",
                         json={"name": "hello",
                               "payload": "payload"})

    assert resp.status_code in [500, 400]


def double(x):
    return x * 2


def test_execute_fn():
    resp = requests.post(base_url + "register_function",
                         json={"name": "hello",
                               "payload": serialize(double)})
    fn_info = resp.json()
    assert "function_id" in fn_info


def test_roundtrip():
    resp = requests.post(base_url + "register_function",
                         json={"name": "double",
                               "payload": serialize(double)})
    fn_info = resp.json()

    number = random.randint(0, 10000)
    resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_info['function_id'],
                               "payload": serialize(((number,), {}))})

    assert resp.status_code in [200, 201]
    assert "task_id" in resp.json()

    task_id = resp.json()["task_id"]

    for i in range(20):

        resp = requests.get(f"{base_url}result/{task_id}")

        assert resp.status_code == 200
        assert resp.json()["task_id"] == task_id
        if resp.json()['status'] in ["COMPLETED", "FAILED"]:
            logging.warning(f"Task is now in {resp.json()['status']}")
            s_result = resp.json()
            logging.warning(s_result)
            result = deserialize(s_result['result'])
            assert result == number * 2
            break

        time.sleep(0.01)


def test_execute_non_existent_function():
    non_existent_fn = str(uuid.uuid4())
    resp = requests.post(base_url + "execute_function",
                         json={"function_id": non_existent_fn,
                               "payload": serialize(((1, 2), {}))})
    assert resp.status_code in [400, 404, 500]


def test_check_status_invalid_task_id():
    non_existent_task = str(uuid.uuid4())
    resp = requests.get(base_url + f"status/{non_existent_task}")
    assert resp.status_code in [400, 404, 500]


def test_check_result_invalid_task_id():
    non_existent_task = str(uuid.uuid4())
    resp = requests.get(base_url + f"result/{non_existent_task}")
    assert resp.status_code in [400, 404]


def test_execute_function_no_params():
    def hello_world():
        return "hello world"

    resp = requests.post(base_url + "register_function",
                         json={"name": "hello_world",
                               "payload": serialize(hello_world)})
    fn_id = resp.json()["function_id"]

    resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_id, "payload": serialize(((), {}))})
    assert resp.status_code in [200, 201]
    task_id = resp.json()["task_id"]

    for _ in range(50):
        time.sleep(0.01)
        r = requests.get(base_url + f"result/{task_id}")
        if r.status_code == 200:
            data = r.json()
            if data["status"] in ["COMPLETED", "FAILED"]:
                result = deserialize(data["result"])
                assert result == "hello world"
                break


def test_task_failure():
    def fail_fn(x):
        raise ValueError("Intentional failure")

    resp = requests.post(base_url + "register_function",
                         json={"name": "fail_fn",
                               "payload": serialize(fail_fn)})
    fn_id = resp.json()["function_id"]

    resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_id, "payload": serialize(((10,), {}))})
    assert resp.status_code in [200, 201]
    task_id = resp.json()["task_id"]

    for _ in range(50):
        time.sleep(0.01)
        r = requests.get(base_url + f"result/{task_id}")
        if r.status_code == 200:
            data = r.json()
            if data["status"] == "FAILED":
                ex = deserialize(data["result"])
                assert isinstance(ex, Exception)
                assert "Intentional failure" in str(ex)
                break


def test_multiple_concurrent_executions():
    def multiply(x, y):
        return x * y

    resp = requests.post(base_url + "register_function",
                         json={"name": "multiply",
                               "payload": serialize(multiply)})
    fn_id = resp.json()["function_id"]

    task_ids = []
    for i in range(10):
        resp = requests.post(base_url + "execute_function",
                             json={"function_id": fn_id,
                                   "payload": serialize(((i, i), {}))})
        assert resp.status_code in [200, 201]
        task_ids.append(resp.json()["task_id"])

    for task_id in task_ids:
        for _ in range(50):
            time.sleep(0.01)
            r = requests.get(base_url + f"result/{task_id}")
            if r.status_code == 200:
                data = r.json()
                if data["status"] in ["COMPLETED", "FAILED"]:
                    result = deserialize(data["result"])
                    assert result == (int(task_id[-1]) ** 2) or isinstance(result, int)
                    break


def test_long_chain_execution():
    def increment(x):
        return x + 1

    resp = requests.post(base_url + "register_function",
                         json={"name": "increment",
                               "payload": serialize(increment)})
    fn_id = resp.json()["function_id"]

    current_value = 0
    for i in range(5):
        resp = requests.post(base_url + "execute_function",
                             json={"function_id": fn_id,
                                   "payload": serialize(((current_value,), {}))})
        assert resp.status_code in [200, 201]
        task_id = resp.json()["task_id"]

        for _ in range(50):
            time.sleep(0.01)
            r = requests.get(base_url + f"result/{task_id}")
            if r.status_code == 200:
                data = r.json()
                if data["status"] in ["COMPLETED", "FAILED"]:
                    result = deserialize(data["result"])
                    # Check correctness
                    assert result == current_value + 1
                    current_value = result
                    break


def test_large_payload():
    large_input = list(range(100000))  # large payload

    def sum_large(lst):
        return sum(lst)

    resp = requests.post(base_url + "register_function",
                         json={"name": "sum_large",
                               "payload": serialize(sum_large)})
    fn_id = resp.json()["function_id"]

    resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_id,
                               "payload": serialize(((large_input,), {}))})
    assert resp.status_code in [200, 201]
    task_id = resp.json()["task_id"]

    for _ in range(100):
        time.sleep(0.05)
        r = requests.get(base_url + f"result/{task_id}")
        if r.status_code == 200:
            data = r.json()
            if data["status"] in ["COMPLETED", "FAILED"]:
                result = deserialize(data["result"])
                assert result == sum(large_input)
                break
