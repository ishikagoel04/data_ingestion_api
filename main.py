"""
Data Ingestion API System

This module implements a RESTful API for handling asynchronous data ingestion requests
with priority-based processing and rate limiting. The system processes requests in batches
and maintains a queue of pending batches that are processed based on priority levels.

Key Features:
- Priority-based processing (HIGH, MEDIUM, LOW)
- Rate limiting (configurable time between batches)
- Batch processing (configurable batch size)
- Asynchronous request handling
- Status tracking for ingestion requests

Author: Your Name
Date: 2024
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional
from enum import Enum
import uuid
import asyncio
from datetime import datetime
import time
from collections import defaultdict
import heapq
import os

# Initialize FastAPI application
app = FastAPI(
    title="Data Ingestion API",
    description="A RESTful API for handling asynchronous data ingestion requests with priority-based processing",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Root endpoint
@app.get("/", include_in_schema=False)
async def root():
    """Redirect root to API documentation"""
    return RedirectResponse(url="/docs")

# Priority levels for ingestion requests
class Priority(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

# Request model for data ingestion
class IngestionRequest(BaseModel):
    ids: List[int] = Field(..., description="List of IDs to process")
    priority: Priority = Field(..., description="Priority level of the request")

# Response models for API endpoints
class BatchStatus(BaseModel):
    batch_id: str
    ids: List[int]
    status: str

class StatusResponse(BaseModel):
    ingestion_id: str
    status: str
    batches: List[BatchStatus]

class IngestionResponse(BaseModel):
    ingestion_id: str

# In-memory storage for requests and status
ingestion_requests = {}  # Stores ingestion requests
batch_statuses = defaultdict(list)  # Stores status of each batch
processing_queue = []  # Queue for pending batches
last_processed_time = 0  # Timestamp of last processed batch

# Configuration constants
BATCH_SIZE = 3  # Maximum number of IDs per batch
RATE_LIMIT_SECONDS = 5  # Minimum time between batch processing

# Priority weights (lower number = higher priority)
PRIORITY_WEIGHTS = {
    Priority.HIGH: 0,
    Priority.MEDIUM: 1,
    Priority.LOW: 2
}

async def process_batch(batch_ids: List[int], batch_id: str, ingestion_id: str):
    """
    Process a batch of IDs with simulated API calls.
    
    Args:
        batch_ids: List of IDs to process
        batch_id: Unique identifier for the batch
        ingestion_id: Unique identifier for the ingestion request
    """
    try:
        # Update batch status to triggered
        for batch in batch_statuses[ingestion_id]:
            if batch.batch_id == batch_id:
                batch.status = "triggered"
                break

        # Simulate API calls for each ID
        for id in batch_ids:
            await asyncio.sleep(1)  # Simulate API call delay
            response = {"id": id, "data": "processed"}
            print(f"Processed ID {id}: {response}")

        # Update batch status to completed
        for batch in batch_statuses[ingestion_id]:
            if batch.batch_id == batch_id:
                batch.status = "completed"
                break

    except Exception as e:
        print(f"Error processing batch {batch_id}: {str(e)}")

async def process_queue():
    """
    Process the queue of batches respecting rate limits.
    This function runs continuously in the background.
    """
    global last_processed_time
    
    while True:
        if processing_queue:
            current_time = time.time()
            time_since_last_batch = current_time - last_processed_time
            
            if time_since_last_batch >= RATE_LIMIT_SECONDS:
                # Sort queue by priority and timestamp
                processing_queue.sort(key=lambda x: (x[0][0], x[0][1]))
                
                # Get the highest priority batch
                priority_info, batch_info = processing_queue.pop(0)
                batch_ids, batch_id, ingestion_id = batch_info
                
                await process_batch(batch_ids, batch_id, ingestion_id)
                last_processed_time = time.time()
        
        await asyncio.sleep(0.1)  # Reduced sleep time for more responsive processing

def process_queue_sync(steps=1):
    """
    Synchronous version of process_queue for testing purposes.
    Simulates the passage of time to process batches.
    
    Args:
        steps: Number of batches to process
    """
    global last_processed_time
    for _ in range(steps):
        if processing_queue:
            # Simulate that enough time has passed for the rate limit
            last_processed_time -= RATE_LIMIT_SECONDS
            current_time = time.time()
            time_since_last_batch = current_time - last_processed_time
            if time_since_last_batch >= RATE_LIMIT_SECONDS:
                processing_queue.sort(key=lambda x: (x[0][0], x[0][1]))
                priority_info, batch_info = processing_queue.pop(0)
                batch_ids, batch_id, ingestion_id = batch_info
                # Simulate processing synchronously
                for batch in batch_statuses[ingestion_id]:
                    if batch.batch_id == batch_id:
                        batch.status = "triggered"
                        break
                for id in batch_ids:
                    pass
                for batch in batch_statuses[ingestion_id]:
                    if batch.batch_id == batch_id:
                        batch.status = "completed"
                        break
                last_processed_time = time.time()

# FastAPI event handlers
@app.on_event("startup")
async def startup_event():
    """Start the background task for processing batches"""
    app.state.process_queue_task = asyncio.create_task(process_queue())

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up background tasks on shutdown"""
    if hasattr(app.state, 'process_queue_task'):
        app.state.process_queue_task.cancel()
        try:
            await app.state.process_queue_task
        except asyncio.CancelledError:
            pass

# API endpoints
@app.post("/ingest", response_model=IngestionResponse)
async def ingest_data(request: IngestionRequest):
    """
    Submit data for ingestion.
    
    Args:
        request: IngestionRequest containing IDs and priority level
        
    Returns:
        IngestionResponse containing the ingestion ID
        
    Raises:
        HTTPException: If any ID is out of valid range
    """
    # Validate IDs
    for id in request.ids:
        if not (1 <= id <= 10**9 + 7):
            raise HTTPException(status_code=400, detail=f"ID {id} is out of valid range (1 to 10^9+7)")

    # Generate unique ingestion ID
    ingestion_id = str(uuid.uuid4())
    
    # Store the request
    ingestion_requests[ingestion_id] = {
        "ids": request.ids,
        "priority": request.priority,
        "created_time": datetime.now()
    }
    
    # Split IDs into batches
    batches = [request.ids[i:i + BATCH_SIZE] for i in range(0, len(request.ids), BATCH_SIZE)]
    
    # Create batch statuses and add to processing queue
    for batch in batches:
        batch_id = str(uuid.uuid4())
        batch_status = BatchStatus(
            batch_id=batch_id,
            ids=batch,
            status="yet_to_start"
        )
        batch_statuses[ingestion_id].append(batch_status)
        
        # Add to processing queue with priority
        priority_weight = PRIORITY_WEIGHTS[request.priority]
        processing_queue.append((
            (priority_weight, time.time()),
            (batch, batch_id, ingestion_id)
        ))
    
    return IngestionResponse(ingestion_id=ingestion_id)

@app.get("/status/{ingestion_id}", response_model=StatusResponse)
async def get_status(ingestion_id: str):
    """
    Check the status of an ingestion request.
    
    Args:
        ingestion_id: Unique identifier for the ingestion request
        
    Returns:
        StatusResponse containing the current status and batch details
        
    Raises:
        HTTPException: If ingestion_id is not found
    """
    if ingestion_id not in ingestion_requests:
        raise HTTPException(status_code=404, detail="Ingestion ID not found")
    
    batches = batch_statuses[ingestion_id]
    
    # Determine overall status
    status_counts = defaultdict(int)
    for batch in batches:
        status_counts[batch.status] += 1
    
    if status_counts["completed"] == len(batches):
        overall_status = "completed"
    elif status_counts["yet_to_start"] == len(batches):
        overall_status = "yet_to_start"
    else:
        overall_status = "triggered"
    
    return StatusResponse(
        ingestion_id=ingestion_id,
        status=overall_status,
        batches=batches
    )

# Error handlers
@app.exception_handler(404)
async def not_found_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={"message": "The requested resource was not found"}
    )

@app.exception_handler(500)
async def server_error_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={"message": "Internal server error occurred"}
    )

if __name__ == "__main__":
    import uvicorn
    import socket
    
    def get_local_ip():
        try:
            # Get the local IP address
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            s.close()
            return local_ip
        except:
            return "127.0.0.1"
    
    port = int(os.getenv("PORT", 8000))
    local_ip = get_local_ip()
    
    print("\n" + "="*50)
    print(f"🚀 Server is running!")
    print(f"📝 API Documentation: http://{local_ip}:{port}/docs")
    print(f"🌐 Local URL: http://{local_ip}:{port}")
    print("="*50 + "\n")
    
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True) 