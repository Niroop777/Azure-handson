import azure.functions as func
from azure.storage.blob import BlobServiceClient
from urllib.parse import unquote
from PIL import Image
import io
import json
import uuid
import datetime
import os
import time

def main(msg: func.QueueMessage):
    start_time = time.time()

    # Read connection string
    conn_str = os.getenv("STORAGE_CONN")

    blob_service = BlobServiceClient.from_connection_string(conn_str)

    # Read message
    job = msg.get_json()
    blob_url = job["blobUrl"]
    sizes = job["sizes"]

    try:
        # Extract original blob filename
        filename = unquote(blob_url.split("/")[-1])

        # Read container names from settings
        input_container = os.environ.get("BlobContainers__Input", "uploads")
        output_container = os.environ.get("BlobContainers__Output", "resized")
        log_container = os.environ.get("BlobContainers__Logs", "function-logs")

        # Download original blob
        source_blob = blob_service.get_blob_client(
            container=input_container,
            blob=filename
        )

        img_bytes = source_blob.download_blob().readall()
        original_img = Image.open(io.BytesIO(img_bytes))

        resized_urls = []

        # Process each size
        for size in sizes:
            img_copy = original_img.copy()
            img_copy.thumbnail((size, size))

            buffer = io.BytesIO()
            img_copy.save(buffer, format="JPEG")
            buffer.seek(0)

            resized_blob = blob_service.get_blob_client(
                container=output_container,
                blob=f"{size}/{uuid.uuid4()}.jpg"
            )

            resized_blob.upload_blob(buffer, overwrite=True)
            resized_urls.append(resized_blob.url)

        # Prepare log JSON
        log_data = {
            "original": blob_url,
            "resized": resized_urls,
            "processing_time": time.time() - start_time,
            "status": "success",
            "timestamp": str(datetime.datetime.utcnow())
        }

        # Upload log entry
        log_blob = blob_service.get_blob_client(
            container=log_container,
            blob=f"ImageResizer/{datetime.date.today()}/{uuid.uuid4()}.json"
        )

        log_blob.upload_blob(json.dumps(log_data), overwrite=True)

    except Exception as e:
        # After 5 retries → log failure
        if msg.dequeue_count >= 5:
            error_log = {
                "original": blob_url,
                "error": str(e),
                "status": "failed-after-retries",
                "timestamp": str(datetime.datetime.utcnow())
            }

            log_blob = blob_service.get_blob_client(
                container=log_container,
                blob=f"ImageResizer/{datetime.date.today()}/{uuid.uuid4()}.json"
            )

            log_blob.upload_blob(json.dumps(error_log), overwrite=True)
            return

        # Requeue automatically
        raise e
