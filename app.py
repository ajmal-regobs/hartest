import logging
import os
import uuid
from datetime import datetime, timezone

import boto3
from flask import Flask, jsonify, request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.route("/")
def hello():
    logger.info("GET / called")
    return jsonify({"message": "Hello, World!"})


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


# ---------------------------------------------------------------------------
# AWS clients (credentials via IRSA — no keys needed)
# ---------------------------------------------------------------------------

def get_s3():
    return boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))


def get_sqs():
    return boto3.client("sqs", region_name=os.environ.get("AWS_REGION", "us-east-1"))


def get_dynamo():
    return boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION", "us-east-1"))


# ---------------------------------------------------------------------------
# S3 routes
# ---------------------------------------------------------------------------

@app.route("/s3/upload", methods=["POST"])
def s3_upload():
    """Upload a text file to S3. Body JSON: {"key": "path/to/file.txt", "content": "hello"}"""
    bucket = os.environ["S3_BUCKET_NAME"]
    data = request.get_json(force=True)
    key = data.get("key") or f"hartest/{uuid.uuid4().hex}.txt"
    content = data.get("content", "")
    get_s3().put_object(Bucket=bucket, Key=key, Body=content.encode())
    logger.info("S3 upload bucket=%s key=%s", bucket, key)
    return jsonify({"bucket": bucket, "key": key})


@app.route("/s3/download")
def s3_download():
    """Download a file from S3. Query param: key"""
    bucket = os.environ["S3_BUCKET_NAME"]
    key = request.args.get("key", "")
    obj = get_s3().get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode()
    logger.info("S3 download bucket=%s key=%s", bucket, key)
    return jsonify({"bucket": bucket, "key": key, "content": content})


# ---------------------------------------------------------------------------
# SQS routes
# ---------------------------------------------------------------------------

@app.route("/sqs/send", methods=["POST"])
def sqs_send():
    """Send a message to SQS. Body JSON: {"message": "hello"}"""
    queue_url = os.environ["SQS_QUEUE_URL"]
    data = request.get_json(force=True)
    message = data.get("message", "")
    resp = get_sqs().send_message(QueueUrl=queue_url, MessageBody=message)
    logger.info("SQS send queue=%s message_id=%s", queue_url, resp["MessageId"])
    return jsonify({"message_id": resp["MessageId"], "message": message})


@app.route("/sqs/receive")
def sqs_receive():
    """Receive up to 5 messages from SQS (does not delete them)."""
    queue_url = os.environ["SQS_QUEUE_URL"]
    resp = get_sqs().receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=5,
        WaitTimeSeconds=2,
    )
    messages = [
        {"message_id": m["MessageId"], "body": m["Body"], "receipt_handle": m["ReceiptHandle"]}
        for m in resp.get("Messages", [])
    ]
    logger.info("SQS receive queue=%s count=%d", queue_url, len(messages))
    return jsonify({"messages": messages})


# ---------------------------------------------------------------------------
# DynamoDB routes
# ---------------------------------------------------------------------------

@app.route("/dynamo/put", methods=["POST"])
def dynamo_put():
    """Put an item into DynamoDB. Body JSON: {"pk": "some-id", ...extra fields}"""
    table_name = os.environ["DYNAMODB_TABLE_NAME"]
    item = request.get_json(force=True)
    if "pk" not in item:
        item["pk"] = uuid.uuid4().hex
    item["created_at"] = datetime.now(timezone.utc).isoformat()
    get_dynamo().Table(table_name).put_item(Item=item)
    logger.info("DynamoDB put table=%s pk=%s", table_name, item["pk"])
    return jsonify({"table": table_name, "pk": item["pk"]})


@app.route("/dynamo/get")
def dynamo_get():
    """Get an item from DynamoDB by pk. Query param: pk"""
    table_name = os.environ["DYNAMODB_TABLE_NAME"]
    pk = request.args.get("pk", "")
    resp = get_dynamo().Table(table_name).get_item(Key={"pk": pk})
    item = resp.get("Item")
    if not item:
        return jsonify({"error": "not found"}), 404
    logger.info("DynamoDB get table=%s pk=%s", table_name, pk)
    return jsonify(item)


if __name__ == "__main__":
    logger.info("Starting server on port 5000")
    app.run(host="0.0.0.0", port=5000)
