import azure.functions as func
from azure.storage.blob import BlobServiceClient
import os
import json

def main(req: func.HttpRequest, msg: func.Out[str]) -> func.HttpResponse:
    try:
        # Read file
        file = req.files.get('file')
        if not file:
            return func.HttpResponse("No file uploaded.", status_code=400)

        # Storage client
        blob_service = BlobServiceClient.from_connection_string(
            os.environ["STORAGE_CONN"]
        )

        # Read container name from settings
        input_container = os.environ.get("BlobContainers__Input", "uploads")

        container = blob_service.get_container_client(input_container)

        # Upload file
        blob_client = container.get_blob_client(file.filename)
        blob_client.upload_blob(file.stream, overwrite=True)

        blob_url = blob_client.url

        # Queue message body
        message = {
            "blobUrl": blob_url,
            "sizes": [320, 1024]   # required as per task
        }

        msg.set(json.dumps(message))

        return func.HttpResponse(
            f"Uploaded successfully & job queued: {blob_url}",
            status_code=200
        )

    except Exception as e:
        return func.HttpResponse(str(e), status_code=500)
