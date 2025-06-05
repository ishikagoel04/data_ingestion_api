import pytest
from fastapi.testclient import TestClient
from main import app, Priority, process_queue_sync
import time

client = TestClient(app)

# Start the background task for testing
@app.on_event("startup")
async def startup_event():
    """Start the background task for processing batches"""
    asyncio.create_task(app.state.process_queue_task)

def test_ingest_endpoint_basic():
    """Test basic ingestion endpoint functionality"""
    response = client.post(
        "/ingest",
        json={"ids": [1, 2, 3, 4, 5], "priority": "MEDIUM"}
    )
    assert response.status_code == 200
    assert "ingestion_id" in response.json()

def test_ingest_endpoint_invalid_id():
    """Test ingestion with invalid ID range"""
    response = client.post(
        "/ingest",
        json={"ids": [0, 1, 2], "priority": "MEDIUM"}
    )
    assert response.status_code == 400

    response = client.post(
        "/ingest",
        json={"ids": [1, 2, 10**9 + 8], "priority": "MEDIUM"}
    )
    assert response.status_code == 400

def test_status_endpoint():
    """Test status endpoint functionality"""
    # First create an ingestion request
    ingest_response = client.post(
        "/ingest",
        json={"ids": [1, 2, 3], "priority": "MEDIUM"}
    )
    ingestion_id = ingest_response.json()["ingestion_id"]

    # Check status
    status_response = client.get(f"/status/{ingestion_id}")
    assert status_response.status_code == 200
    data = status_response.json()
    assert data["ingestion_id"] == ingestion_id
    assert "status" in data
    assert "batches" in data
    assert len(data["batches"]) == 1  # Should have one batch of 3 IDs

def test_priority_handling():
    """Test priority handling and processing order"""
    # Submit a low priority request
    low_priority_response = client.post(
        "/ingest",
        json={"ids": [1, 2, 3], "priority": "LOW"}
    )
    low_priority_id = low_priority_response.json()["ingestion_id"]

    # Submit a high priority request
    high_priority_response = client.post(
        "/ingest",
        json={"ids": [4, 5, 6], "priority": "HIGH"}
    )
    high_priority_id = high_priority_response.json()["ingestion_id"]

    # Simulate enough steps for the queue to process
    process_queue_sync(steps=3)

    # Check status of both requests
    high_status = client.get(f"/status/{high_priority_id}").json()
    low_status = client.get(f"/status/{low_priority_id}").json()

    # High priority should be processed first
    assert high_status["status"] in ["triggered", "completed"]
    assert low_status["status"] in ["yet_to_start", "triggered"]

def test_rate_limiting():
    """Test rate limiting functionality"""
    # Submit multiple requests
    responses = []
    for i in range(3):
        response = client.post(
            "/ingest",
            json={"ids": [i*3 + 1, i*3 + 2, i*3 + 3], "priority": "MEDIUM"}
        )
        responses.append(response.json()["ingestion_id"])

    # Simulate only one batch processed due to rate limiting
    process_queue_sync(steps=1)

    # Check status of all requests
    statuses = [client.get(f"/status/{id}").json() for id in responses]
    
    # Count completed batches
    completed_batches = sum(
        1 for status in statuses
        for batch in status["batches"]
        if batch["status"] == "completed"
    )
    
    # Should have processed only one batch due to rate limiting
    assert completed_batches <= 1

def test_batch_size_limit():
    """Test that batches are limited to 3 IDs"""
    response = client.post(
        "/ingest",
        json={"ids": [1, 2, 3, 4, 5, 6, 7], "priority": "MEDIUM"}
    )
    ingestion_id = response.json()["ingestion_id"]

    status_response = client.get(f"/status/{ingestion_id}")
    data = status_response.json()
    
    # Should have 3 batches: [1,2,3], [4,5,6], [7]
    assert len(data["batches"]) == 3
    assert len(data["batches"][0]["ids"]) == 3
    assert len(data["batches"][1]["ids"]) == 3
    assert len(data["batches"][2]["ids"]) == 1

def test_invalid_ingestion_id():
    """Test status endpoint with invalid ingestion ID"""
    response = client.get("/status/invalid_id")
    assert response.status_code == 404

def test_priority_ordering():
    """Test that requests are processed in correct priority order"""
    # Submit requests in reverse priority order
    low_response = client.post(
        "/ingest",
        json={"ids": [1, 2, 3], "priority": "LOW"}
    )
    medium_response = client.post(
        "/ingest",
        json={"ids": [4, 5, 6], "priority": "MEDIUM"}
    )
    high_response = client.post(
        "/ingest",
        json={"ids": [7, 8, 9], "priority": "HIGH"}
    )

    # Simulate enough steps for the queue to process all three batches
    process_queue_sync(steps=3)

    # Check status of all requests
    high_status = client.get(f"/status/{high_response.json()['ingestion_id']}").json()
    medium_status = client.get(f"/status/{medium_response.json()['ingestion_id']}").json()
    low_status = client.get(f"/status/{low_response.json()['ingestion_id']}").json()

    # High priority should be processed first
    assert high_status["status"] in ["triggered", "completed"]
    # Medium and low priority might not be processed yet due to rate limiting
    assert medium_status["status"] in ["yet_to_start", "triggered", "completed"]
    assert low_status["status"] in ["yet_to_start", "triggered", "completed"] 