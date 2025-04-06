import dill
import codecs

def serialize(obj) -> str:
    return codecs.encode(dill.dumps(obj), "base64").decode()

def deserialize(obj: str):
    return dill.loads(codecs.decode(obj.encode(), "base64"))

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