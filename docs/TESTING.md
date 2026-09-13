# CloudMart API Testing Guide

### 1. Happy Path (Order Confirmation)
- **Endpoint**: POST http://localhost:8083/api/orders
- **Payload**:
\\\json
{
  "productId": 1,
  "quantity": 1,
  "totalPrice": 79999.00,
  "username": "admin"
}
\\\
- Result: Saga orchestrates events -> Status becomes CONFIRMED.

### 2. Failure Path (Compensating Rollback)
- Increase totalPrice > 200,000 to trigger payment failure.
- Result: Order status automatically transitions to CANCELLED.
