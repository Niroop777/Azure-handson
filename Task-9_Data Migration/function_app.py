import logging
import os
import time
from typing import List, Dict, Any, Tuple

import azure.functions as func
from azure.cosmos import CosmosClient, exceptions as cosmos_exceptions
import pyodbc

app = func.FunctionApp()

def get_cosmos_container():
    cosmos_url = os.environ["COSMOS_URL"]
    cosmos_key = os.environ["COSMOS_KEY"]
    db_name = os.environ["COSMOS_DB"]
    container_name = os.environ["COSMOS_CONTAINER"]

    client = CosmosClient(cosmos_url, credential=cosmos_key)
    db = client.get_database_client(db_name)
    container = db.get_container_client(container_name)
    return container


def get_sql_connection():
    conn_str = os.environ["SQL_CONN_STR"]
    cnxn = pyodbc.connect(conn_str)
    cnxn.autocommit = False
    return cnxn


def iter_cosmos_batches(container, batch_size: int):
    """
    Iterate Cosmos items in pages. The SDK handles continuation tokens internally.
    Using by_page() gives us paging so we never load everything in memory.
    """
    pager = container.read_all_items(max_item_count=batch_size)
    for page in pager.by_page():
        docs = list(page)
        if not docs:
            break
        yield docs


def map_product(doc: Dict[str, Any]) -> Tuple[Tuple, List[Tuple]]:
    """
    Map a Cosmos document to:
      - one Products row: (id, name, price, category)
      - zero or more ProductTags rows: [(productId, tag), ...]
    """
    product_id = doc.get("id")
    name = doc.get("name")
    price = doc.get("price")
    category = doc.get("category")

    # Normalize price
    try:
        price_val = float(price) if price is not None else None
    except (TypeError, ValueError):
        price_val = None

    product_row = (product_id, name, price_val, category)

    tags_rows: List[Tuple[str, str]] = []
    tags = doc.get("tags") or []
    if isinstance(tags, list):
        for t in tags:
            tags_rows.append((product_id, str(t)))

    return product_row, tags_rows


def insert_batch_sql(
    cursor,
    products_rows: List[Tuple],
    tags_rows: List[Tuple],
    stats: Dict[str, int],
):
    """
    Insert batch into SQL using executemany. Avoid duplicates for Products by
    catching PK violations and skipping existing ids.
    """
    if products_rows:
        try:
            cursor.fast_executemany = True
            cursor.executemany(
                "INSERT INTO Products (id, name, price, category) VALUES (?, ?, ?, ?)",
                products_rows,
            )
            stats["products_inserted"] += len(products_rows)
        except pyodbc.IntegrityError:

            logging.warning("PK conflict in Products batch, inserting row-by-row.")
            for row in products_rows:
                try:
                    cursor.execute(
                        "INSERT INTO Products (id, name, price, category) VALUES (?, ?, ?, ?)",
                        row,
                    )
                    stats["products_inserted"] += 1
                except pyodbc.IntegrityError:
                    stats["products_skipped_existing"] += 1

    if tags_rows:
        cursor.fast_executemany = True
        cursor.executemany(
            "INSERT INTO ProductTags (productId, tag) VALUES (?, ?)",
            tags_rows,
        )
        stats["tags_inserted"] += len(tags_rows)


# HTTP-trigger migration func

@app.route(route="migrate-products", auth_level=func.AuthLevel.FUNCTION)
def migrate_products(req: func.HttpRequest) -> func.HttpResponse:
    """
    One-time migration endpoint.
    Call: POST /api/migrate-products?code=<function-key>
    """
    start_time = time.time()

    batch_size = int(os.getenv("BATCH_SIZE", "100"))

    stats = {
        "total_docs_read": 0,
        "products_inserted": 0,
        "products_skipped_existing": 0,
        "tags_inserted": 0,
        "doc_failures": 0,
    }

    logging.info(f"Starting Cosmos → SQL migration, batch_size={batch_size}")

    container = get_cosmos_container()
    cnxn = get_sql_connection()
    cursor = cnxn.cursor()

    try:
        for docs_batch in iter_cosmos_batches(container, batch_size):
            products_rows: List[Tuple] = []
            tags_rows: List[Tuple] = []

            for doc in docs_batch:
                stats["total_docs_read"] += 1
                try:
                    p_row, tags = map_product(doc)
                    products_rows.append(p_row)
                    tags_rows.extend(tags)
                except Exception as e:
                    stats["doc_failures"] += 1
                    logging.error(f"Failed to map document id={doc.get('id')}: {e}")

           
            try:
                insert_batch_sql(cursor, products_rows, tags_rows, stats)
                cnxn.commit()
            except Exception as e:
                cnxn.rollback()
                stats["doc_failures"] += len(docs_batch)
                logging.exception("SQL batch insert failed, rolling back: %s", e)

    except cosmos_exceptions.CosmosHttpResponseError as e:

        if e.status_code == 429:
            retry_ms = getattr(e, "retry_after_in_ms", 1000)
            logging.warning(f"Cosmos 429 throttling, sleeping {retry_ms} ms then aborting.")
            time.sleep(retry_ms / 1000.0)
        logging.exception("Cosmos error during migration: %s", e)
    except Exception as e:
        logging.exception("Unexpected error during migration: %s", e)
    finally:
        try:
            cursor.close()
            cnxn.close()
        except Exception:
            pass

    duration = time.time() - start_time
    logging.info(
        "Migration finished in %.1f s. Docs read=%d, products_inserted=%d, "
        "products_skipped_existing=%d, tags_inserted=%d, failures=%d",
        duration,
        stats["total_docs_read"],
        stats["products_inserted"],
        stats["products_skipped_existing"],
        stats["tags_inserted"],
        stats["doc_failures"],
    )

    report = {
        "durationSeconds": round(duration, 1),
        **stats,
    }

    return func.HttpResponse(
        body=str(report),
        status_code=200,
        mimetype="application/json",
    )
