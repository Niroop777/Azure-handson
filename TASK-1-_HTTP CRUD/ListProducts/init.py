import azure.functions as func
import json
from cosmos_client import get_container

def main(req: func.HttpRequest) -> func.HttpResponse:
    container = get_container()
    items = list(container.query_items(
        query="SELECT * FROM c",
        enable_cross_partition_query=True
    ))

    return func.HttpResponse(
        json.dumps(items),
        mimetype="application/json"
    )
