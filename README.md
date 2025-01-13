# Redis Go
A simple Redis server implementation in Go, built as part of a learning exercise or for lightweight, custom Redis-like use cases.
This project leverages **Go's networking** and **concurrency features** to support basic Redis commands, aiming to provide a functional yet minimal Redis-like server.
## Features
- **Basic Commands**
    - `PING` - Test connectivity with the server.
    - `SET` - Store a key-value pair in memory.
    - `GET` - Retrieve the value for a given key.

## Prerequisites
- Go **v1.23** or higher.
- Redis CLI or any Redis client for testing.

## Installation
1. Clone the repository:
``` bash
   git clone <repository-url>
```
1. Navigate to the project directory:
``` bash
   cd redis-gp
```
1. Build the project:
``` bash
   go build -o redis-go .
```
## Usage
1. Start the server:
``` bash
   ./redis-go
```
1. Connect to the server using the Redis CLI:
``` bash
   redis-cli
```
## Testing Commands
- **PING**:
``` bash
  > PING
  +PONG
```
- **SET**:
``` bash
  > SET key value
  +OK
```
- **GET**:
``` bash
  > GET key
  "value"
```
## Project Goals
This project is designed to:
1. Explore Go's capabilities in building a custom server.
2. Understand concurrency patterns and networking in Go.
3. Serve as a practical example for custom lightweight Redis-like use cases.
