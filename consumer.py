#!/usr/bin/env python3
"""
SQS consumer for ComfyUI video generation.

Polls SQS for ComfyUI API-format workflow JSON, submits to ComfyUI,
waits for completion, uploads output video to S3, cleans up, and loops.

Processes one message at a time.
"""

import boto3
import requests
import time
import os
import sys
import json
import glob
import signal

SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL")
S3_BUCKET = os.environ.get("S3_BUCKET")

SQS_REGION = "ca-central-1"

COMFYUI_URL = "http://127.0.0.1:18188"
COMFYUI_OUTPUT_DIR = os.path.join("/workspace", "ComfyUI", "output")
COMFYUI_INPUT_DIR = os.path.join("/workspace", "ComfyUI", "input")

S3_UPLOAD_PREFIX = "outputs/"

VISIBILITY_TIMEOUT = 1200

running = True


def handle_signal(signum, frame):
    global running
    print(f"\nReceived signal {signum}, finishing current job then exiting...")
    running = False


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def wait_for_comfyui():
    """Block until ComfyUI is reachable."""
    print("Waiting for ComfyUI to be ready...")
    while running:
        try:
            resp = requests.get(f"{COMFYUI_URL}/system_stats", timeout=5)
            if resp.status_code == 200:
                print("ComfyUI is ready.")
                return
        except requests.ConnectionError:
            pass
        time.sleep(3)


def download_s3_inputs(s3_client, workflow):
    """Download S3-linked files to the ComfyUI input dir and rewrite paths to filenames."""
    for node_id, node in workflow.items():
        inputs = node.get("inputs", {})
        for key, value in inputs.items():
            if isinstance(value, str) and value.startswith("s3://"):
                parts = value[5:].split("/", 1)
                bucket = parts[0]
                s3_key = parts[1]
                filename = os.path.basename(s3_key)
                local_path = os.path.join(COMFYUI_INPUT_DIR, filename)
                print(f"  Downloading s3://{bucket}/{s3_key} -> {local_path}")
                s3_client.download_file(bucket, s3_key, local_path)
                inputs[key] = filename


def submit_prompt(workflow):
    """Submit an API-format workflow to ComfyUI. Returns prompt_id."""
    resp = requests.post(
        f"{COMFYUI_URL}/prompt",
        json={"prompt": workflow},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    prompt_id = data["prompt_id"]
    print(f"  Submitted prompt {prompt_id}")
    return prompt_id


def wait_for_completion(prompt_id, timeout=600):
    """Poll ComfyUI /history until the prompt finishes. Returns history entry."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(f"{COMFYUI_URL}/history/{prompt_id}", timeout=10)
            history = resp.json()
            if prompt_id in history:
                status = history[prompt_id].get("status", {})
                if status.get("completed", False) or status.get("status_str") == "success":
                    print(f"  Prompt {prompt_id} completed.")
                    return history[prompt_id]
                if status.get("status_str") == "error":
                    raise RuntimeError(
                        f"Prompt {prompt_id} failed: {status.get('messages', '')}"
                    )
        except requests.ConnectionError:
            pass
        time.sleep(2)
    raise TimeoutError(f"Prompt {prompt_id} did not finish within {timeout}s")


def find_output_video(history_entry):
    """Extract the output video file path from ComfyUI history entry."""
    outputs = history_entry.get("outputs", {})
    for node_id, node_output in outputs.items():
        for key in ("gifs", "videos", "images"):
            for item in node_output.get(key, []):
                subfolder = item.get("subfolder", "")
                filename = item["filename"]
                if subfolder:
                    return os.path.join(COMFYUI_OUTPUT_DIR, subfolder, filename)
                return os.path.join(COMFYUI_OUTPUT_DIR, filename)
    return None


def upload_to_s3(s3_client, local_path):
    """Upload a file to S3 and return the S3 key."""
    filename = os.path.basename(local_path)
    s3_key = f"{S3_UPLOAD_PREFIX}{filename}"
    print(f"  Uploading {filename} -> s3://{S3_BUCKET}/{s3_key}")
    s3_client.upload_file(local_path, S3_BUCKET, s3_key)
    return s3_key


def process_message(s3_client, body):
    """Run the full pipeline: submit workflow, wait, upload, cleanup."""
    workflow = json.loads(body)

    download_s3_inputs(s3_client, workflow)

    # Unique noise seed for each generation
    workflow["178"]["inputs"]["noise_seed"] = int(time.time_ns())

    try:
        prompt_id = submit_prompt(workflow)
        history_entry = wait_for_completion(prompt_id, timeout=VISIBILITY_TIMEOUT - 60)

        video_path = find_output_video(history_entry)
        if not video_path:
            raise FileNotFoundError("No output video found after generation")
        if not os.path.isfile(video_path):
            raise FileNotFoundError(f"Expected file not found: {video_path}")

        s3_key = upload_to_s3(s3_client, video_path)
        return s3_key
    finally:
        for d in (COMFYUI_INPUT_DIR, COMFYUI_OUTPUT_DIR):
            for f in glob.glob(os.path.join(d, "*")):
                try:
                    os.remove(f)
                    print(f"  Cleaned up: {f}")
                except OSError:
                    pass


def main():
    if not SQS_QUEUE_URL:
        print("ERROR: SQS_QUEUE_URL must be set")
        sys.exit(1)
    if not S3_BUCKET:
        print("ERROR: S3_BUCKET must be set")
        sys.exit(1)

    sqs = boto3.client(
        "sqs",
        region_name=SQS_REGION,
    )

    s3 = boto3.client("s3")

    wait_for_comfyui()

    print(f"Consumer started. Polling {SQS_QUEUE_URL}")
    print(f"Will upload video to s3://{S3_BUCKET}/{S3_UPLOAD_PREFIX}")

    while running:
        try:
            resp = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                VisibilityTimeout=VISIBILITY_TIMEOUT,
            )
        except Exception as e:
            print(f"SQS receive error: {e}")
            time.sleep(5)
            continue

        messages = resp.get("Messages", [])
        if not messages:
            continue

        msg = messages[0]
        msg_id = msg["MessageId"]
        print(f"Received message {msg_id}")

        try:
            uploaded = process_message(s3, msg["Body"])
            sqs.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=msg["ReceiptHandle"],
            )
            print(f"Message {msg_id} processed and deleted. Uploaded: {uploaded}")
        except (json.JSONDecodeError, KeyError, AttributeError, TypeError) as e:
            print(f"Permanent failure for {msg_id}, deleting: {e}")
            sqs.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=msg["ReceiptHandle"],
            )
        except Exception as e:
            print(f"Transient error for {msg_id}, will retry: {e}")
            # Message will become visible again after VisibilityTimeout

    print("Consumer stopped.")


if __name__ == "__main__":
    main()
