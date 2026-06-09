import os, json, uuid, time, asyncio
import pika
from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

app = FastAPI()

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://comfy:ChangeThisPassword@rabbitmq:5672/")
QUEUE_NAME = os.environ.get("QUEUE_NAME", "comfy_jobs")

JOBS = {}
JOB_COUNTER = 0
CLIENTS = set()


def now():
    return time.time()


def active_count():
    return len([
        j for j in JOBS.values()
        if j["status"] in ["queued", "sent_to_worker", "running", "saving"]
    ])


async def broadcast(event: dict):
    dead = []
    for ws in list(CLIENTS):
        try:
            await ws.send_json(event)
        except Exception:
            dead.append(ws)

    for ws in dead:
        CLIENTS.discard(ws)


def broadcast_safe(event: dict):
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(broadcast(event))
    except Exception:
        pass


def rabbit_publish(message: dict):
    params = pika.URLParameters(RABBITMQ_URL)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE_NAME, durable=True)
    ch.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
    )
    conn.close()


def make_prompt_item(job):
    return [
        job["number"],
        job["prompt_id"],
        job.get("workflow", {}),
        {},
        []
    ]


def push_ws(prompt_id=None, progress=None, status=None):
    broadcast_safe({
        "type": "status",
        "data": {
            "status": {
                "exec_info": {
                    "queue_remaining": active_count()
                }
            }
        }
    })

    if prompt_id and progress is not None:
        broadcast_safe({
            "type": "progress",
            "data": {
                "value": int(progress),
                "max": 100,
                "prompt_id": prompt_id,
                "node": "distributed-worker"
            }
        })

    if prompt_id:
        broadcast_safe({
            "type": "executing",
            "data": {
                "node": None if status == "completed" else "distributed-worker",
                "prompt_id": prompt_id
            }
        })


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    print("WEBSOCKET CONNECTING", flush=True)
    await websocket.accept()
    print("WEBSOCKET CONNECTED", flush=True)

    CLIENTS.add(websocket)

    await websocket.send_json({
        "type": "status",
        "data": {
            "status": {
                "exec_info": {
                    "queue_remaining": active_count()
                }
            }
        }
    })

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        print("WEBSOCKET DISCONNECTED", flush=True)
        CLIENTS.discard(websocket)
    except Exception as e:
        print(f"WEBSOCKET ERROR: {e}", flush=True)
        CLIENTS.discard(websocket)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "queue": QUEUE_NAME,
        "jobs_known": len(JOBS),
        "active": active_count(),
        "websocket_clients": len(CLIENTS),
    }


@app.get("/prompt")
def prompt_get():
    return {
        "status": "ok",
        "note": "Use POST /prompt to queue jobs"
    }


@app.post("/prompt")
async def prompt(request: Request):
    global JOB_COUNTER

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    JOB_COUNTER += 1
    prompt_id = str(uuid.uuid4())

    JOBS[prompt_id] = {
        "number": JOB_COUNTER,
        "prompt_id": prompt_id,
        "status": "queued",
        "status_text": "Queued in RabbitMQ",
        "progress": 0,
        "created_at": now(),
        "queued_at": now(),
        "started_at": None,
        "completed_at": None,
        "failed_at": None,
        "worker": None,
        "output": None,
        "error": None,
        "workflow": body,
        "events": [{
            "time": now(),
            "status": "queued",
            "message": "Job queued from ComfyUI master GUI",
            "progress": 0,
        }],
    }

    job = {
        "job_id": prompt_id,
        "prompt_id": prompt_id,
        "number": JOB_COUNTER,
        "workflow": body,
        "created_at": now(),
        "source": "comfy-master-gui",
    }

    try:
        rabbit_publish(job)
    except Exception as e:
        JOBS[prompt_id]["status"] = "failed"
        JOBS[prompt_id]["status_text"] = "Failed to publish to RabbitMQ"
        JOBS[prompt_id]["failed_at"] = now()
        JOBS[prompt_id]["error"] = str(e)
        push_ws(prompt_id, 0, "failed")
        raise HTTPException(status_code=503, detail=f"RabbitMQ publish failed: {e}")

    push_ws(prompt_id, 0, "queued")

    return JSONResponse({
        "prompt_id": prompt_id,
        "number": JOB_COUNTER,
        "node_errors": {}
    })


@app.get("/queue")
def queue_status():
    pending = []
    running = []

    for job in JOBS.values():
        item = make_prompt_item(job)

        if job["status"] in ["queued", "sent_to_worker"]:
            pending.append(item)

        if job["status"] in ["running", "saving"]:
            running.append(item)

    return {
        "queue_running": running,
        "queue_pending": pending,
    }


@app.post("/queue")
async def queue_action(request: Request):
    return {"status": "ok", "note": "Queue managed by RabbitMQ dispatcher"}


@app.get("/history")
def history(max_items: int = 64):
    out = {}

    for prompt_id, job in list(JOBS.items())[-max_items:]:
        if job["status"] not in ["completed", "failed"]:
            continue

        status_str = "success" if job["status"] == "completed" else "error"

        out[prompt_id] = {
            "prompt": make_prompt_item(job),
            "outputs": job.get("output") or {},
            "status": {
                "status_str": status_str,
                "completed": job["status"] == "completed",
                "messages": [
                    [e["status"], e["message"]]
                    for e in job.get("events", [])
                ],
            },
        }

    return out


@app.get("/history/{prompt_id}")
def history_prompt(prompt_id: str):
    job = JOBS.get(prompt_id)

    if not job or job["status"] not in ["completed", "failed"]:
        return {}

    status_str = "success" if job["status"] == "completed" else "error"

    return {
        prompt_id: {
            "prompt": make_prompt_item(job),
            "outputs": job.get("output") or {},
            "status": {
                "status_str": status_str,
                "completed": job["status"] == "completed",
                "messages": [
                    [e["status"], e["message"]]
                    for e in job.get("events", [])
                ],
            },
        }
    }


@app.get("/jobs")
def jobs():
    return JOBS


@app.get("/jobs/{prompt_id}")
def job_status(prompt_id: str):
    job = JOBS.get(prompt_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.post("/jobs/{prompt_id}/status")
async def update_job_status(prompt_id: str, request: Request):
    body = await request.json()
    return _set_status(
        prompt_id=prompt_id,
        status=body.get("status"),
        progress=body.get("progress"),
        worker=body.get("worker"),
        message=body.get("message"),
        output=body.get("output"),
        error=body.get("error"),
    )


def _set_status(prompt_id, status=None, progress=None, worker=None, message=None, output=None, error=None):
    job = JOBS.get(prompt_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if status:
        job["status"] = status

    if progress is not None:
        job["progress"] = int(progress)

    if worker:
        job["worker"] = worker

    if message:
        job["status_text"] = message

    if output:
        job["output"] = output

    if error:
        job["error"] = error

    if status == "running" and not job.get("started_at"):
        job["started_at"] = now()

    if status == "saving":
        job["saving_at"] = now()

    if status == "completed":
        job["completed_at"] = now()
        job["progress"] = 100

    if status == "failed":
        job["failed_at"] = now()

    job["events"].append({
        "time": now(),
        "status": job["status"],
        "message": message or job["status"],
        "worker": worker,
        "progress": job["progress"],
    })

    push_ws(prompt_id, job["progress"], job["status"])

    return {"ok": True, "job": job}


@app.get("/system_stats")
def system_stats():
    running = len([j for j in JOBS.values() if j["status"] in ["running", "saving"]])
    queued = len([j for j in JOBS.values() if j["status"] in ["queued", "sent_to_worker"]])
    completed = len([j for j in JOBS.values() if j["status"] == "completed"])
    failed = len([j for j in JOBS.values() if j["status"] == "failed"])

    return {
        "system": {
            "os": "docker",
            "ram_total": 0,
            "ram_free": 0,
        },
        "devices": [{
            "name": f"RabbitMQ Comfy Farm | queued={queued} running={running} completed={completed} failed={failed}",
            "type": "distributed",
            "index": 0,
            "vram_total": 0,
            "vram_free": 0,
            "torch_vram_total": 0,
            "torch_vram_free": 0,
        }]
    }
