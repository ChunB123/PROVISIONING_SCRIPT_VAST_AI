#!/usr/bin/env python3
"""
SQS consumer for ComfyUI video generation.

Polls SQS for ComfyUI API-format workflow JSON, submits to ComfyUI,
waits for completion, uploads output video to S3, cleans up, and loops.

Processes one message at a time.

Environment variables:
    SQS_ENDPOINT_URL   - SQS endpoint (e.g. ngrok URL)
    SQS_QUEUE_URL      - Full SQS queue URL
    SQS_REGION         - AWS region (default: us-east-1)
    SQS_ACCESS_KEY_ID  - AWS access key (default: test)
    SQS_SECRET_ACCESS_KEY - AWS secret key (default: test)
    COMFYUI_URL        - ComfyUI server URL (default: http://127.0.0.1:8188)
    COMFYUI_OUTPUT_DIR - ComfyUI output directory (default: $WORKSPACE/ComfyUI/output)
    S3_BUCKET          - S3 bucket for video uploads
    S3_PREFIX           - S3 key prefix (default: outputs/)
    VISIBILITY_TIMEOUT - SQS visibility timeout in seconds (default: 900)
"""

import boto3
import requests
import time
import os
import sys
import json
import glob
import signal

SQS_ENDPOINT_URL = os.environ.get("SQS_ENDPOINT_URL", "")
SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL", "")
SQS_REGION = os.environ.get("SQS_REGION", "us-east-1")
SQS_ACCESS_KEY_ID = os.environ.get("SQS_ACCESS_KEY_ID", "test")
SQS_SECRET_ACCESS_KEY = os.environ.get("SQS_SECRET_ACCESS_KEY", "test")

COMFYUI_URL = os.environ.get("COMFYUI_URL", "http://127.0.0.1:18188")
COMFYUI_OUTPUT_DIR = os.environ.get(
    "COMFYUI_OUTPUT_DIR",
    os.path.join(os.environ.get("WORKSPACE", "/workspace"), "ComfyUI", "output"),
)
COMFYUI_INPUT_DIR = os.environ.get(
    "COMFYUI_INPUT_DIR",
    os.path.join(os.environ.get("WORKSPACE", "/workspace"), "ComfyUI", "input"),
)

S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_PREFIX = os.environ.get("S3_PREFIX", "outputs/")

VISIBILITY_TIMEOUT = int(os.environ.get("VISIBILITY_TIMEOUT", "900"))

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


def find_output_videos(history_entry):
    """Extract output video file paths from ComfyUI history entry."""
    videos = []
    outputs = history_entry.get("outputs", {})
    for node_id, node_output in outputs.items():
        # VHS_VideoCombine puts results under "gifs" key
        for item in node_output.get("gifs", []):
            subfolder = item.get("subfolder", "")
            filename = item["filename"]
            if subfolder:
                path = os.path.join(COMFYUI_OUTPUT_DIR, subfolder, filename)
            else:
                path = os.path.join(COMFYUI_OUTPUT_DIR, filename)
            videos.append(path)
        # Standard SaveImage/SaveVideo uses "images" or "videos" key
        for key in ("videos", "images"):
            for item in node_output.get(key, []):
                subfolder = item.get("subfolder", "")
                filename = item["filename"]
                if subfolder:
                    path = os.path.join(COMFYUI_OUTPUT_DIR, subfolder, filename)
                else:
                    path = os.path.join(COMFYUI_OUTPUT_DIR, filename)
                if path not in videos:
                    videos.append(path)
    return videos


def upload_to_s3(s3_client, local_path):
    """Upload a file to S3 and return the S3 key."""
    filename = os.path.basename(local_path)
    s3_key = f"{S3_PREFIX}{filename}"
    print(f"  Uploading {filename} -> s3://{S3_BUCKET}/{s3_key}")
    s3_client.upload_file(local_path, S3_BUCKET, s3_key)
    return s3_key


def process_message(s3_client, body):
    """Run the full pipeline: submit workflow, wait, upload, cleanup."""
    workflow = json.loads(body)

    download_s3_inputs(s3_client, workflow)

    prompt_id = submit_prompt(workflow)
    history_entry = wait_for_completion(prompt_id, timeout=VISIBILITY_TIMEOUT - 60)

    videos = find_output_videos(history_entry)
    if not videos:
        print("  WARNING: No output videos found in history. Checking output dir...")
        videos = sorted(
            glob.glob(os.path.join(COMFYUI_OUTPUT_DIR, "LTX-2*.mp4")),
            key=os.path.getmtime,
        )
        if videos:
            videos = [videos[-1]]

    if not videos:
        raise FileNotFoundError("No output video found after generation")

    uploaded_keys = []
    for video_path in videos:
        if not os.path.isfile(video_path):
            print(f"  WARNING: Expected file not found: {video_path}")
            continue
        s3_key = upload_to_s3(s3_client, video_path)
        uploaded_keys.append(s3_key)
        os.remove(video_path)
        print(f"  Deleted local file: {video_path}")

    return uploaded_keys


def main():
    if not SQS_ENDPOINT_URL or not SQS_QUEUE_URL:
        print("ERROR: SQS_ENDPOINT_URL and SQS_QUEUE_URL must be set")
        sys.exit(1)
    if not S3_BUCKET:
        print("ERROR: S3_BUCKET must be set")
        sys.exit(1)

    sqs = boto3.client(
        "sqs",
        endpoint_url=SQS_ENDPOINT_URL,
        region_name=SQS_REGION,
        aws_access_key_id=SQS_ACCESS_KEY_ID,
        aws_secret_access_key=SQS_SECRET_ACCESS_KEY,
    )

    s3 = boto3.client("s3")

    wait_for_comfyui()

    print(f"Consumer started. Polling {SQS_QUEUE_URL}")
    print(f"Uploading to s3://{S3_BUCKET}/{S3_PREFIX}")

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
        except Exception as e:
            print(f"Error processing message {msg_id}: {e}")
            # Message will become visible again after VisibilityTimeout

    print("Consumer stopped.")


if __name__ == "__main__":
    main()
