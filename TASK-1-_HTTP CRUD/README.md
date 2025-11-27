# Azure Functions Product API

This project is an Azure Functions-based REST API for performing CRUD operations on products stored in Azure Cosmos DB. It includes functions to add, update, delete, list, and retrieve products.

## Project Structure

```
AzureFunctions_CRUD/
├── AddProduct/
│   └── __init__.py
├── UpdateProduct/
│   └── __init__.py
├── DeleteProduct/
│   └── __init__.py
├── GetProduct/
│   └── __init__.py
├── ListProducts/
│   └── __init__.py
├── cosmos_client.py
├── requirements.txt
├── local.settings.json
├── host.json
└── README.md
```

## Prerequisites

- Python 3.8+
- Azure Functions Core Tools
- Azure Cosmos DB account (or Emulator)

## Setup Instructions

### 1. Clone the Repository

```
git clone <your-repo-url>
cd AzureFunctions_CRUD
```

### 2. Install Python Dependencies

```
pip install -r requirements.txt
```

### 3. Configure local.settings.json

Update or create `local.settings.json`:

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "COSMOS_CONN_STRING": "<your-cosmos-connection-string>",
    "COSMOS_DB": "ProductsDB",
    "COSMOS_CONTAINER": "products"
  }
}
```

- Replace `<your-cosmos-connection-string>` with your actual Cosmos DB connection string.
- Ensure the database and container already exist in Cosmos DB.

### 4. Run Locally

```
func start
```

API Base URL:

```
http://localhost:7071/api/
```

## Test Endpoints

Use Postman, Thunder Client, or PowerShell.

### Add Product

```
POST http://localhost:7071/api/AddProduct
Content-Type: application/json

{
  "id": "101",
  "name": "Sample Product",
  "price": 1000
}
```

### Update Product

```
PUT http://localhost:7071/api/UpdateProduct/{id}
Content-Type: application/json

{
  "name": "Updated Name",
  "price": 1500
}
```

### Delete Product

```
DELETE http://localhost:7071/api/DeleteProduct/{id}
```

### Get Product

```
GET http://localhost:7071/api/GetProduct/{id}
```

### List All Products

```
GET http://localhost:7071/api/ListProducts
```

## Dependencies

Install with:

```
pip install -r requirements.txt
```

Main packages:

- `azure-functions`
- `azure-cosmos`

## Local Run Output

Below is a screenshot (terminal_output.png) showing the Azure Functions host running locally

![alt text](image.png)

## Function Endpoints

| Function      | Route                   | Method |
| ------------- | ----------------------- | ------ |
| AddProduct    | /api/AddProduct         | POST   |
| UpdateProduct | /api/UpdateProduct/{id} | PUT    |
| DeleteProduct | /api/DeleteProduct/{id} | DELETE |
| GetProduct    | /api/GetProduct/{id}    | GET    |
| ListProducts  | /api/ListProducts       | GET    |

## Notes

- Ensure Cosmos DB is accessible from your machine.
- Connection strings and secrets should be secured for production.
- The Cosmos DB container uses `/id` as the partition key.

---
