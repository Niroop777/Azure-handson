import azure.functions as func
import json
from cosmos_client import read_item

def main(req: func.HttpRequest) -> func.HttpResponse:
    id = req.route_params.get('id')
    if not id:
        return func.HttpResponse("id required", status_code=400)

    item = read_item(id)
    if not item:
        return func.HttpResponse("Not found", status_code=404)

    return func.HttpResponse(
        json.dumps(item),
        mimetype="application/json",
        status_code=200
    )
