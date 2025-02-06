from fastapi import FastAPI, HTTPException
import redis
from pydantic import BaseModel
import uuid
from utils import serialize, deserialize

# FastAPI setup
app = FastAPI()

# Redis setup
redis_client = redis.Redis(host="localhost", port=6379)

class RegisterFn(BaseModel):
    name: str
    payload: str

class RegisterFnRep(BaseModel):
    function_id: uuid.UUID

class ExecuteFnReq(BaseModel):
    function_id: uuid.UUID
    payload: str

class ExecuteFnRep(BaseModel):
    task_id: uuid.UUID

class TaskStatusRep(BaseModel):
    task_id: uuid.UUID
    status: str

class TaskResultRep(BaseModel):
    task_id: uuid.UUID
    status: str
    result: str

# REST API Endpoints
@app.post("/register_function", response_model=RegisterFnRep)
def register_function(fn: RegisterFn) -> RegisterFnRep:
    try:
        # initialize a function
        function_id = uuid.uuid4()
        function_body = {
            "name": fn.name,
            "payload": fn.payload
        }
        # store function in Redis
        redis_client.set(str(function_id), serialize(function_body))
        return {"function_id": function_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error registering function: {e}")
    

@app.post("/execute_function", response_model=ExecuteFnRep)
def execute_function(req: ExecuteFnReq) -> ExecuteFnRep:
    try:
        # retrieve function from Redis and check if it exists
        function_body = redis_client.get(str(req.function_id))
        if not function_body:
            raise HTTPException(status_code=404, detail="Function is not found")
        # deserialize function
        function_body = deserialize(function_body)
        # create a task and store it in Redis
        task_id = uuid.uuid4()
        task_data = {
            "function_id": str(req.function_id),
            "fn_payload": function_body["payload"],
            "param_payload": req.payload,
            "status": "QUEUED",
            "result": None
        }
        redis_client.set(str(task_id), serialize(task_data))
        # publish task to the Tasks channel
        redis_client.publish("Tasks", str(task_id))
        return {"task_id": task_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error executing function: {e}")


@app.get("/status/{task_id}", response_model=TaskStatusRep)
def get_task_status(task_id: uuid.UUID) -> TaskStatusRep:
    try:
        # retrieve task from Redis and check if it exists
        task_data = redis_client.get(str(task_id))
        if not task_data:
            raise HTTPException(status_code=404, detail="Task is not found")
        # deserialize task and return status
        task_data = deserialize(task_data)
        return {"task_id": task_id, "status": task_data["status"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving task status: {e}")
    

@app.get("/result/{task_id}", response_model=TaskResultRep)
def get_task_result(task_id: uuid.UUID) -> TaskResultRep:
    try:
        # retrieve task from Redis and check if it exists
        task_data = redis_client.get(str(task_id))
        if not task_data:
            raise HTTPException(status_code=404, detail="Task is not found")
        # deserialize task and return status and result
        task_data = deserialize(task_data)
        if task_data["result"] is None:
            raise HTTPException(status_code=400, detail="Result is not available yet")
        return {"task_id": task_id, "status": task_data["status"], "result": task_data["result"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving task result: {e}")