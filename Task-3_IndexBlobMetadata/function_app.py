import logging
import os
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient
from datetime import datetime

app = func.FunctionApp()

@app.event_grid_trigger(arg_name="azeventgrid")
def EventGridTrigger(azeventgrid: func.EventGridEvent):

    logging.info("Event received from Event Grid")

    try:

        data = azeventgrid.get_json()
        blob_url = data["url"]
        logging.info(f"Blob URL: {blob_url}")

        #  CONNECT TO BLOB STG
     
        blob_conn = os.environ["BLOB_CONN_STR"]
        blob_service = BlobServiceClient.from_connection_string(blob_conn)

        # container + blob name
        parts = blob_url.replace("https://", "").split("/", 2)
        container_name = parts[1]
        blob_name = parts[2]
        logging.info(f"Container: {container_name}, Blob: {blob_name}")

        blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)

        # Blob properties
        props = blob_client.get_blob_properties()

        size = props.size
        content_type = props.content_settings.content_type

    
        #  READ BLOB 
    
        title = None
        word_count = None

        if "text" in content_type or blob_name.endswith(".txt") or blob_name.endswith(".md"):
            try:
                blob_bytes = blob_client.download_blob().readall()
                blob_text = blob_bytes.decode("utf-8", errors="ignore")

                # Extract title:
            
                for line in blob_text.splitlines():
                    stripped = line.strip()
                    if stripped.startswith("# "):   
                        title = stripped[2:].strip()
                        break

                
                if not title:
                    title = blob_text.splitlines()[0].strip() if blob_text else "(empty file)"

                # Count words
                word_count = len(blob_text.split())

            except Exception as e:
                logging.error(f"Error reading blob content: {e}")
                title = None
                word_count = None
        else:
            logging.info("Blob is not a text file. Skipping content extraction.")

        
        # COSMOS DB
        
        document = {
            "id": blob_name,
            "blobName": blob_name,
            "container": container_name,
            "url": blob_url,
            "size": size,
            "contentType": content_type,
            "title": title,
            "wordCount": word_count,
            "uploadedOn": datetime.utcnow().isoformat()
        }

        logging.info(f"Document to insert: {document}")


        # INSERT TO COSMOS DB
        
        cosmos_client = CosmosClient(
            os.environ["COSMOS_URL"],
            credential=os.environ["COSMOS_KEY"]
        )

        database = cosmos_client.get_database_client(os.environ["COSMOS_DB"])
        container = database.get_container_client(os.environ["COSMOS_CONTAINER"])

        container.upsert_item(document)

        logging.info("Metadata inserted into Cosmos DB successfully.")

    except Exception as e:
        logging.error(f"Error: {e}")
        raise

    return None
