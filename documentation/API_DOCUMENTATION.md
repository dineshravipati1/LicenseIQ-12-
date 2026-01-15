# Licence IQ Research Platform - API Documentation

## API Overview
The Licence IQ Research Platform provides a comprehensive REST API for contract management, AI-powered document analysis, user management, and analytics.

**Base URL:** `/api`  
**Authentication:** Session-based with cookies  
**Content-Type:** `application/json`

## Authentication Endpoints

### POST /api/register
Register a new user account.

**Request Body:**
```json
{
  "username": "string (required)",
  "password": "string (required, min 6 chars)",
  "email": "string (required, valid email)",
  "firstName": "string (optional)",
  "lastName": "string (optional)",
  "role": "enum: owner|admin|editor|viewer|auditor (optional, default: viewer)"
}
```

**Response:** `201 Created`
```json
{
  "id": "uuid",
  "username": "string",
  "email": "string",
  "firstName": "string",
  "lastName": "string",
  "role": "string",
  "isActive": true,
  "createdAt": "timestamp",
  "updatedAt": "timestamp"
}
```

**Error Responses:**
- `400 Bad Request` - Validation errors or username already exists
- `500 Internal Server Error` - Server error

### POST /api/login
Authenticate user and create session.

**Request Body:**
```json
{
  "username": "string (required)",
  "password": "string (required)"
}
```

**Response:** `200 OK`
```json
{
  "id": "uuid",
  "username": "string",
  "email": "string",
  "role": "string",
  "isActive": true
}
```

**Error Responses:**
- `401 Unauthorized` - Invalid credentials
- `400 Bad Request` - Missing required fields

### POST /api/logout
End user session.

**Response:** `200 OK`
```json
{
  "message": "Logged out successfully"
}
```

### GET /api/user
Get current authenticated user information.

**Response:** `200 OK`
```json
{
  "id": "uuid",
  "username": "string",
  "email": "string",
  "firstName": "string",
  "lastName": "string",
  "role": "string",
  "isActive": true,
  "createdAt": "timestamp",
  "updatedAt": "timestamp"
}
```

**Error Responses:**
- `401 Unauthorized` - Not authenticated

## Contract Management Endpoints

### GET /api/contracts
List all contracts with pagination and filtering.

**Query Parameters:**
- `page`: number (default: 1)
- `limit`: number (default: 10, max: 100)
- `status`: enum (pending|processing|analyzed|failed)
- `contractType`: string
- `search`: string (search in filename and type)

**Response:** `200 OK`
```json
{
  "contracts": [
    {
      "id": "uuid",
      "originalName": "string",
      "fileSize": "number",
      "fileType": "string",
      "contractType": "string",
      "status": "enum",
      "priority": "enum",
      "uploadedBy": "uuid",
      "processingTime": "number",
      "createdAt": "timestamp",
      "updatedAt": "timestamp",
      "analysis": {
        "confidence": "string",
        "summary": "string"
      }
    }
  ],
  "total": "number",
  "page": "number",
  "limit": "number",
  "totalPages": "number"
}
```

### GET /api/contracts/:id
Get specific contract with full analysis details.

**Path Parameters:**
- `id`: uuid (required)

**Response:** `200 OK`
```json
{
  "id": "uuid",
  "originalName": "string",
  "filePath": "string",
  "fileSize": "number",
  "fileType": "string",
  "contractType": "string",
  "status": "enum",
  "priority": "enum",
  "uploadedBy": "uuid",
  "processingTime": "number",
  "createdAt": "timestamp",
  "updatedAt": "timestamp",
  "analysis": {
    "id": "uuid",
    "summary": "string",
    "keyTerms": [
      {
        "type": "string",
        "description": "string",
        "confidence": "number",
        "location": "string"
      }
    ],
    "riskAnalysis": [
      {
        "level": "enum: high|medium|low",
        "title": "string",
        "description": "string"
      }
    ],
    "insights": [
      {
        "type": "string",
        "title": "string",
        "description": "string"
      }
    ],
    "confidence": "string",
    "processingTime": "number",
    "createdAt": "timestamp"
  }
}
```

**Error Responses:**
- `404 Not Found` - Contract not found
- `403 Forbidden` - No permission to view contract

### POST /api/contracts/upload
Upload a new contract file for processing.

**Request:** `multipart/form-data`
- `file`: File (required) - PDF or DOCX file, max 10MB
- `contractType`: string (optional)
- `priority`: enum (optional) - low|normal|high|urgent

**Response:** `201 Created`
```json
{
  "id": "uuid",
  "originalName": "string",
  "fileSize": "number",
  "fileType": "string",
  "contractType": "string",
  "status": "pending",
  "priority": "string",
  "uploadedBy": "uuid",
  "createdAt": "timestamp"
}
```

**Error Responses:**
- `400 Bad Request` - Invalid file type or size
- `413 Payload Too Large` - File exceeds size limit
- `401 Unauthorized` - Not authenticated

### PUT /api/contracts/:id/reprocess
Reprocess contract with AI analysis.

**Path Parameters:**
- `id`: uuid (required)

**Response:** `200 OK`
```json
{
  "message": "Contract queued for reprocessing",
  "contractId": "uuid",
  "status": "processing"
}
```

**Error Responses:**
- `404 Not Found` - Contract not found
- `403 Forbidden` - No permission to reprocess
- `409 Conflict` - Contract already processing

### DELETE /api/contracts/:id
Delete a contract and all associated data.

**Path Parameters:**
- `id`: uuid (required)

**Response:** `200 OK`
```json
{
  "message": "Contract deleted successfully",
  "contractId": "uuid"
}
```

**Error Responses:**
- `404 Not Found` - Contract not found
- `403 Forbidden` - No permission to delete
- `409 Conflict` - Contract currently processing

### GET /api/contracts/:id/export
Export contract analysis as PDF report.

**Path Parameters:**
- `id`: uuid (required)

**Query Parameters:**
- `format`: enum (pdf|json) - default: pdf

**Response:** `200 OK`
- Content-Type: `application/pdf` or `application/json`
- File download or JSON data

## Analytics Endpoints

### GET /api/analytics/metrics
Get dashboard metrics and statistics.

**Response:** `200 OK`
```json
{
  "totalContracts": "number",
  "processing": "number",
  "analyzed": "number",
  "failed": "number",
  "averageProcessingTime": "number",
  "averageConfidence": "number",
  "contractsByType": {
    "employment": "number",
    "service": "number",
    "nda": "number",
    "other": "number"
  },
  "contractsByStatus": {
    "pending": "number",
    "processing": "number",
    "analyzed": "number",
    "failed": "number"
  },
  "riskDistribution": {
    "high": "number",
    "medium": "number",
    "low": "number"
  },
  "recentActivity": [
    {
      "contractId": "uuid",
      "originalName": "string",
      "status": "string",
      "createdAt": "timestamp"
    }
  ]
}
```

### GET /api/analytics/reports
Generate detailed analytics reports.

**Query Parameters:**
- `dateFrom`: date (ISO 8601)
- `dateTo`: date (ISO 8601)
- `groupBy`: enum (day|week|month)
- `contractType`: string (optional filter)

**Response:** `200 OK`
```json
{
  "period": {
    "from": "date",
    "to": "date"
  },
  "summary": {
    "totalContracts": "number",
    "totalProcessingTime": "number",
    "averageConfidence": "number",
    "successRate": "number"
  },
  "trends": [
    {
      "date": "date",
      "contracts": "number",
      "averageProcessingTime": "number",
      "averageConfidence": "number"
    }
  ],
  "typeBreakdown": {
    "employment": "number",
    "service": "number",
    "nda": "number"
  }
}
```

## User Management Endpoints

### GET /api/users
List all users (Admin+ only).

**Query Parameters:**
- `page`: number (default: 1)
- `limit`: number (default: 10)
- `role`: enum filter
- `search`: string (search username/email)

**Response:** `200 OK`
```json
{
  "users": [
    {
      "id": "uuid",
      "username": "string",
      "email": "string",
      "firstName": "string",
      "lastName": "string",
      "role": "string",
      "isActive": "boolean",
      "createdAt": "timestamp",
      "lastLogin": "timestamp"
    }
  ],
  "total": "number",
  "page": "number",
  "totalPages": "number"
}
```

### PUT /api/users/:id
Update user information (Admin+ only).

**Path Parameters:**
- `id`: uuid (required)

**Request Body:**
```json
{
  "email": "string (optional)",
  "firstName": "string (optional)",
  "lastName": "string (optional)",
  "role": "enum (optional)",
  "isActive": "boolean (optional)"
}
```

**Response:** `200 OK`
```json
{
  "id": "uuid",
  "username": "string",
  "email": "string",
  "firstName": "string",
  "lastName": "string",
  "role": "string",
  "isActive": "boolean",
  "updatedAt": "timestamp"
}
```

### DELETE /api/users/:id
Delete user account (Owner only).

**Path Parameters:**
- `id`: uuid (required)

**Response:** `200 OK`
```json
{
  "message": "User deleted successfully",
  "userId": "uuid"
}
```

## Error Response Format

All API endpoints return errors in a consistent format:

```json
{
  "error": {
    "code": "string",
    "message": "string",
    "details": "object (optional)"
  }
}
```

### Common Error Codes
- `VALIDATION_ERROR` - Request validation failed
- `AUTHENTICATION_REQUIRED` - User not authenticated
- `INSUFFICIENT_PERMISSIONS` - User lacks required permissions
- `RESOURCE_NOT_FOUND` - Requested resource not found
- `DUPLICATE_RESOURCE` - Resource already exists
- `PROCESSING_ERROR` - File or AI processing error
- `RATE_LIMIT_EXCEEDED` - Too many requests
- `SERVER_ERROR` - Internal server error

## Rate Limiting

API endpoints are rate-limited to prevent abuse:
- **General API**: 100 requests per 15 minutes per IP
- **File Upload**: 10 uploads per hour per user
- **AI Processing**: 5 concurrent processing jobs per user

Rate limit headers:
- `X-RateLimit-Limit`: Request limit
- `X-RateLimit-Remaining`: Remaining requests
- `X-RateLimit-Reset`: Reset time (Unix timestamp)

## Webhooks (Future Enhancement)

Webhook endpoints for real-time notifications:

### POST /api/webhooks/register
Register webhook endpoint for contract processing events.

**Request Body:**
```json
{
  "url": "string (required)",
  "events": ["array of event types"],
  "secret": "string (optional)"
}
```

### Webhook Events
- `contract.uploaded` - New contract uploaded
- `contract.processing` - Processing started
- `contract.analyzed` - Analysis completed
- `contract.failed` - Processing failed
- `contract.deleted` - Contract deleted

## SDK Examples

### JavaScript/Node.js
```javascript
// Contract upload example
const formData = new FormData();
formData.append('file', fileBlob);
formData.append('contractType', 'employment');

const response = await fetch('/api/contracts/upload', {
  method: 'POST',
  credentials: 'include',
  body: formData
});

const result = await response.json();
console.log('Contract uploaded:', result);
```

### Python
```python
import requests

# Login example
login_data = {
    'username': 'admin',
    'password': 'admin123'
}

session = requests.Session()
response = session.post('/api/login', json=login_data)
user = response.json()

# Get contracts
contracts_response = session.get('/api/contracts')
contracts = contracts_response.json()
```

### cURL
```bash
# Login
curl -X POST /api/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}' \
  -c cookies.txt

# Upload contract
curl -X POST /api/contracts/upload \
  -b cookies.txt \
  -F "file=@contract.pdf" \
  -F "contractType=service"

# Get contract analysis
curl -X GET /api/contracts/{id} \
  -b cookies.txt \
  -H "Accept: application/json"
```

## Testing the API

### Postman Collection
Import the provided Postman collection for easy API testing with pre-configured requests and environment variables.

### Test Data
Use the following test accounts for API testing:
- **Owner**: admin/admin123
- **Admin**: owner/owner123  
- **Editor**: editor/editor123
- **Viewer**: viewer/viewer123
- **Auditor**: auditor/auditor123