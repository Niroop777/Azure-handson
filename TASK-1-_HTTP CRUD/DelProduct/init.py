# python
import azure.functions as func
import json
from cosmos_client import delete_item

def main(req: func.HttpRequest) -> func.HttpResponse:
    # accept id from route or query string
    id = req.route_params.get('id') or req.params.get('id')
    if not id:
        return func.HttpResponse("id required", status_code=400)

    try:
        item = delete_item(id)
        return func.HttpResponse(f"Item with id {id} deleted", status_code=200)
    except Exception as e:
        return func.HttpResponse(f"Error deleting item: {e}", status_code=500)