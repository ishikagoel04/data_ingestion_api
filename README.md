# Data Ingestion API System

A robust RESTful API system for handling asynchronous data ingestion requests with priority-based processing and rate limiting.

## Features

- **Priority-based Processing**: Supports HIGH, MEDIUM, and LOW priority levels
- **Rate Limiting**: Processes batches with configurable rate limits
- **Batch Processing**: Automatically splits large requests into manageable batches
- **Status Tracking**: Real-time status monitoring for ingestion requests
- **Asynchronous Processing**: Non-blocking request handling
- **Input Validation**: Ensures data integrity with comprehensive validation

## Screenshots

### API Documentation
![API Documentation](images/api_docs.png)
*Interactive API documentation using Swagger UI*

### Example Request
![Example Request](images/example_request.png)
*Submitting a data ingestion request*

### Status Check
![Status Check](images/status_check.png)
*Checking the status of an ingestion request*

## Project Structure

```
data_ingestion_api/
│
├── main.py            # FastAPI application and core logic
├── test_main.py       # Test suite
├── requirements.txt   # Project dependencies
├── README.md         # Project documentation
└── images/           # Screenshots and documentation images
    ├── api_docs.png
    ├── example_request.png
    └── status_check.png
```

## Prerequisites

- Python 3.8 or higher
- pip (Python package installer)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/data_ingestion_api.git
cd data_ingestion_api
```

2. Create and activate a virtual environment:
```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/Mac
python -m venv venv
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Running the Application

Start the server:
```bash
uvicorn main:app --reload
```

The API will be available at: http://localhost:8000

## API Documentation

Once the server is running, you can access the interactive API documentation at:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

### Endpoints

#### 1. Submit Data for Ingestion
```http
POST /ingest
```

Request Body:
```json
{
    "ids": [1, 2, 3, 4, 5],
    "priority": "HIGH"  // "HIGH", "MEDIUM", or "LOW"
}
```

Response:
```json
{
    "ingestion_id": "uuid-string"
}
```

#### 2. Check Ingestion Status
```http
GET /status/{ingestion_id}
```

Response:
```json
{
    "ingestion_id": "uuid-string",
    "status": "yet_to_start|triggered|completed",
    "batches": [
        {
            "batch_id": "uuid-string",
            "ids": [1, 2, 3],
            "status": "yet_to_start|triggered|completed"
        }
    ]
}
```

## Running Tests

Run the test suite:
```bash
pytest test_main.py -v
```

## Configuration

Key configuration parameters in `main.py`:
- `BATCH_SIZE`: Maximum number of IDs per batch (default: 3)
- `RATE_LIMIT_SECONDS`: Minimum time between batch processing (default: 5 seconds)

## Priority Processing

The system processes requests based on priority levels:
1. HIGH priority requests are processed first
2. MEDIUM priority requests are processed next
3. LOW priority requests are processed last

Within each priority level, requests are processed in FIFO (First In, First Out) order.

## Rate Limiting

The system enforces a rate limit to prevent overwhelming the processing system:
- Only one batch is processed every `RATE_LIMIT_SECONDS` (default: 5 seconds)
- Rate limiting is applied across all priority levels
- The system maintains a queue of pending batches

## Error Handling

The API handles various error cases:
- Invalid ID ranges (must be between 1 and 10^9 + 7)
- Invalid priority levels
- Non-existent ingestion IDs
- Rate limit violations

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Author

Your Name
- GitHub: [Your GitHub Profile]
- Email: your.email@example.com

## Acknowledgments

- FastAPI for the excellent web framework
- Pydantic for data validation
- Uvicorn for the ASGI server 