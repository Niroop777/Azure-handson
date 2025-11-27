import os
from azure.cosmos import CosmosClient, PartitionKey, exceptions

COSMOS_CONNECTION = os.environ.get("COSMOS_CONN_STRING")
COSMOS_DB = os.environ.get("COSMOS_DB")
COSMOS_CONTAINER = os.environ.get("COSMOS_CONTAINER")

if not COSMOS_CONNECTION:
    raise Exception("COSMOS_CONNECTION env variable missing")

client = CosmosClient.from_connection_string(COSMOS_CONNECTION)

def get_container():
    db = client.create_database_if_not_exists(id=COSMOS_DB)
    container = db.create_container_if_not_exists(
        id=COSMOS_CONTAINER,
        partition_key=PartitionKey(path="/id"),
        offer_throughput=400
    )
    return container

def read_item(id):
    container = get_container()
    query = "SELECT * FROM c WHERE c.id = @id"
    items = list(container.query_items(
        query=query,
        parameters=[{"name": "@id", "value": id}],
        enable_cross_partition_query=True
    ))
    return items[0] if items else None

def delete_item(id):
    container= get_container()
    container.delete_item(item=id, partition_key=id)

def update_item(id, updated_fields):
    container = get_container()
    item = read_item(id)
    if not item:
        return None

    for key, value in updated_fields.items():
        item[key] = value

    container.replace_item(item=item, body=item)
    return item