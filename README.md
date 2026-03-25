# Ride Sharing Backend (AWS Lambda)

This project is a serverless backend for a ride-sharing platform built using AWS Lambda and REST APIs.

## Features
- User authentication and session handling
- Ride creation and discovery
- Ride request and matching flow
- CO₂ savings tracking based on shared journeys
  
## Architecture
- AWS Lambda for backend logic
- API Gateway for routing requests
- SQL database for persistent storage
  
## Example Data Flow
- User logs in → authentication Lambda validates token
- User creates a ride → data validated and stored in database
- Other users query available rides → filtered results returned
- Ride requests handled asynchronously between users
  
## Notes

This repository is a simplified reconstruction of a university industry project (in collaboration with Amazon).
Original deployment and infrastructure (AWS configuration, API Gateway, etc.) are not publicly accessible.
