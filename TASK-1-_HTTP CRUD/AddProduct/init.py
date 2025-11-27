import azure.functions as func
import json
from cosmos_client import get_container

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
    except:
        return func.HttpResponse("Invalid JSON", status_code=400)

    if not body or "id" not in body or not str(body.get("id")).strip():
        return func.HttpResponse("Missing 'id' in body", status_code=400)

    if "price" not in body:
        return func.HttpResponse("Missing 'price' in body", status_code=400)

    try:
        price = float(body["price"])
    except (ValueError, TypeError):
        return func.HttpResponse("Invalid 'price' value; must be numeric", status_code=400)

    body["price"] = price

    container = get_container()
    created = container.create_item(body)

    return func.HttpResponse(
        json.dumps(created),
        mimetype="application/json",
        status_code=201
    )