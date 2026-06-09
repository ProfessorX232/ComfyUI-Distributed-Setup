import os
import json
import time
import itertools
import threading
import requests
import pika

RABBITMQ_URL = os.environ["RABBITMQ_URL"]
QUEUE_NAME = os.environ.get("QUEUE_NAME", "comfy_jobs")
API_URL = os.environ.get("COMFY_API_URL", "http://comfy-api:9000").rstrip("/")

COMFY_WORKERS = [
    w.strip().rstrip("/")
    for w in os.environ["COMFY_WORKERS"].split(",")
    if w.strip()
]

rr = itertools.cycle(COMFY_WORKERS)

print("Dispatcher starting", flush=True)
print(f"Queue: {QUEUE_NAME}", flush=True)
print(f"Workers: {COMFY_WORKERS}", flush=True)
print(f"API: {API_URL}", flush=True)


def api_status(master_id, status, worker=None, progress=0, message=None, output=None, error=None):
    try:
        requests.post(
            f"{API_URL}/jobs/{master_id}/status",
            json={
                "status": status,
                "worker": worker,
                "progress": progress,
                "message": message or status,
                "output": output,
                "error": error,
            },
            timeout=5,
        )
    except Exception as e:
        print(f"API status update failed for {master_id}: {e}", flush=True)


def get_json(url, timeout=3):
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"GET failed {url}: {e}", flush=True)
        return None


def pick_worker():
    checked = []

    for _ in range(len(COMFY_WORKERS)):
        worker = next(rr)
        q = get_json(f"{worker}/queue")

        if not q:
            checked.append({"worker": worker, "error": "queue unavailable"})
            continue

        running = len(q.get("queue_running", []))
        pending = len(q.get("queue_pending", []))
        checked.append({"worker": worker, "running": running, "pending": pending})

        if running == 0 and pending == 0:
            print(f"Selected free worker: {worker} checked={checked}", flush=True)
            return worker

    for worker in COMFY_WORKERS:
        if "3080" in worker:
            print(f"All workers busy, fallback to 3080: {worker} checked={checked}", flush=True)
            return worker

    fallback = next(rr)
    print(f"All workers busy, fallback: {fallback} checked={checked}", flush=True)
    return fallback


def monitor_job(master_id, worker, worker_prompt_id, timeout=7200):
    start = time.time()
    last_progress = 25

    print(f"Monitoring master_id={master_id} worker_prompt_id={worker_prompt_id}", flush=True)

    while time.time() - start < timeout:
        h = get_json(f"{worker}/history/{worker_prompt_id}", timeout=5)

        if h and worker_prompt_id in h:
            data = h[worker_prompt_id]
            status = data.get("status", {})
            status_str = status.get("status_str", "")
            completed = status.get("completed", False)

            if completed or status_str == "success":
                api_status(
                    master_id,
                    "completed",
                    worker=worker,
                    progress=100,
                    message=f"Completed on {worker}",
                    output=data.get("outputs", {}),
                )
                print(f"Completed master_id={master_id} on {worker}", flush=True)
                return

            if status_str == "error":
                api_status(
                    master_id,
                    "failed",
                    worker=worker,
                    progress=last_progress,
                    message=f"Failed on {worker}",
                    error=json.dumps(status)[:2000],
                )
                print(f"Failed master_id={master_id} on {worker}", flush=True)
                return

        q = get_json(f"{worker}/queue", timeout=3)
        q_text = json.dumps(q) if q else ""

        if worker_prompt_id in q_text:
            elapsed = time.time() - start
            estimated = min(90, 25 + int(elapsed / 10))

            if estimated > last_progress:
                last_progress = estimated
                api_status(
                    master_id,
                    "running",
                    worker=worker,
                    progress=estimated,
                    message=f"Running on {worker} ({estimated}%)",
                )
        else:
            for _ in range(6):
                h = get_json(f"{worker}/history/{worker_prompt_id}", timeout=5)
                if h and worker_prompt_id in h:
                    data = h[worker_prompt_id]
                    status = data.get("status", {})
                    if status.get("completed") or status.get("status_str") == "success":
                        api_status(
                            master_id,
                            "completed",
                            worker=worker,
                            progress=100,
                            message=f"Completed on {worker}",
                            output=data.get("outputs", {}),
                        )
                        print(f"Completed master_id={master_id} on {worker}", flush=True)
                        return
                time.sleep(2)

            api_status(
                master_id,
                "completed",
                worker=worker,
                progress=100,
                message=f"Worker queue finished on {worker}",
            )
            print(f"Assumed completed master_id={master_id} on {worker}", flush=True)
            return

        time.sleep(5)

    api_status(
        master_id,
        "failed",
        worker=worker,
        progress=last_progress,
        message=f"Timeout waiting for {worker_prompt_id} on {worker}",
        error="timeout",
    )
    print(f"Timeout master_id={master_id}", flush=True)


def callback(ch, method, properties, body):
    print(f"Received RabbitMQ message: {body[:300]}", flush=True)

    try:
        job = json.loads(body)
        master_id = job.get("prompt_id") or job.get("job_id")
        workflow = job["workflow"]

        worker = pick_worker()

        api_status(
            master_id,
            "sent_to_worker",
            worker=worker,
            progress=10,
            message=f"Sent to {worker}",
        )

        print(f"Sending master job {master_id} to {worker}", flush=True)

        r = requests.post(f"{worker}/prompt", json=workflow, timeout=60)
        print(f"Worker response {worker}: HTTP {r.status_code} {r.text[:500]}", flush=True)

        if not (200 <= r.status_code < 300):
            api_status(
                master_id,
                "failed",
                worker=worker,
                progress=0,
                message=f"Worker rejected job: HTTP {r.status_code}",
                error=r.text[:2000],
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        worker_prompt_id = r.json().get("prompt_id")

        if not worker_prompt_id:
            api_status(
                master_id,
                "failed",
                worker=worker,
                progress=0,
                message="Worker returned no prompt_id",
                error=r.text[:2000],
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        api_status(
            master_id,
            "running",
            worker=worker,
            progress=25,
            message=f"Running on {worker} as {worker_prompt_id}",
        )

        print(f"Tracking master_id={master_id} as worker_prompt_id={worker_prompt_id}", flush=True)

        # IMPORTANT: ACK immediately after successful submit.
        # This prevents RabbitMQ from redelivering the same job if dispatcher restarts.
        ch.basic_ack(delivery_tag=method.delivery_tag)

        t = threading.Thread(
            target=monitor_job,
            args=(master_id, worker, worker_prompt_id),
            daemon=True,
        )
        t.start()

    except Exception as e:
        print(f"Dispatcher callback exception: {e}", flush=True)
        try:
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            pass


while True:
    try:
        params = pika.URLParameters(RABBITMQ_URL)
        conn = pika.BlockingConnection(params)
        ch = conn.channel()
        ch.queue_declare(queue=QUEUE_NAME, durable=True)
        ch.basic_qos(prefetch_count=1)
        ch.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

        print("Comfy dispatcher waiting for jobs...", flush=True)
        ch.start_consuming()

    except Exception as e:
        print(f"RabbitMQ/dispatcher not ready: {repr(e)}", flush=True)
        time.sleep(5)
