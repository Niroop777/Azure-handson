import azure.functions as func
import json
from cosmos_client import update_item

def main(req: func.HttpRequest) -> func.HttpResponse:
    # Prefer id from route (function.json route: products/{id}), fallback to body
    route_id = req.route_params.get('id')

    try:
        body = req.get_json()
    except Exception:
        body = None

    # Determine id: route param takes precedence
    if route_id:
        id = route_id
    elif body and "id" in body:
        id = body.get("id")
    else:
        return func.HttpResponse("Missing 'id' (in route or body)", status_code=400)

    # Build fields to update: prefer body content (except id)
    if not body:
        return func.HttpResponse("Missing JSON body with fields to update", status_code=400)

    # Remove id from updated fields if present
    updated_fields = {k: v for k, v in body.items() if k != "id"}

    if not updated_fields:
        return func.HttpResponse("No fields to update", status_code=400)

    try:
        updated_item = update_item(id, updated_fields)
    except Exception as e:
        # Log exception to function logs (Azure Functions will capture stdout/stderr)
        print(f"Error updating item {id}: {e}")
        return func.HttpResponse("Internal server error", status_code=500)

    if not updated_item:
        return func.HttpResponse("Not found", status_code=404)

    return func.HttpResponse(
        json.dumps(updated_item),
        mimetype="application/json",
        status_code=200
    )