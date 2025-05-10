# Redis Go
A simple Redis server implementation in Go, built as part of a learning exercise or for lightweight, custom Redis-like use cases.
This project leverages **Go'store networking** and **concurrency features** to support basic Redis commands, aiming to provide a functional yet minimal Redis-like server.
## Features
- **Basic Commands**
    - `PING` - Test connectivity with the server. Responds with `PONG`.
    - `ECHO` - Responds with the argument passed.
    - `SET` - Stores a key-value pair in memory with optional expiration.
    - `GET` - Retrieves the value for a given key. Returns `nil` if the key does not exist.
    - `CONFIG` - Retrieve or set server and environment configuration.
    - `KEYS` - Fetches keys matching a pattern (currently supports `*` wildcard).
    - `INFO` - Provides server information.
    - `REPLCONF` - Acknowledge and synchronize replica configuration.
      - `GETACK` - Replica synchronisation
      - `ACK` -  Replica synchronisation
    - `PSYNC` - Implements full resynchronization for replica servers.
    - `COMMAND` for documentation purposes.
      - `DOCS`- Server documentation. Currently just returns Welcome
    - `WAIT` - Replica consistency - This command blocks the current client until all the previous write commands are successfully transferred and acknowledged by at least the number of replicas you specify in the numreplicas argument.
    - `TYPE` - Returns the data type of the value stored at a key. Returns "none" if the key does not exist.
    - `XADD` - Adds an entry to a stream. Takes a key, an ID, and field-value pairs.
    - `XRANGE` - Returns the stream entries with IDs matching the specified range.
    - `XREAD` - Reads from one or more streams, with optional blocking behavior if no items are available.


## Prerequisites
- Go **v1.23** or higher.
- Redis CLI or any Redis client for testing.

## Installation
1. Clone the repository::
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
### CONFIG:
Retrieve configurations:
```bash
> CONFIG GET dir
1) "dir"
2) "/path/to/directory"
```

### KEYS:
```bash
> KEYS *
1) "key1"
2) "key2"
```

### INFO:
```bash
> INFO replication
"Master replication information"
```

### REPLCONF:
```bash
> REPLCONF ACK 0
+OK
```

### PSYNC:
```bash
> PSYNC
1) "FULLRESYNC <master-replid> <master-offset>"
2) "<data-dump>"
```

### COMMAND:
```bash
> COMMAND docs
"Welcome"
```

### TYPE:
```bash
> TYPE mykey
"string"
```

### XADD:
```bash
> XADD mystream * field1 value1 field2 value2
"1698766401000-0"
```

### XRANGE:
```bash
> XRANGE mystream - +
1) 1) "1698766401000-0"
   2) 1) "field1"
      2) "value1"
      3) "field2"
      4) "value2"
```

### XREAD:
```bash
> XREAD COUNT 2 STREAMS mystream 0
1) 1) "mystream"
   2) 1) 1) "1698766401000-0"
         2) 1) "field1"
            2) "value1"
            3) "field2"
            4) "value2"
```

## Replication
This server supports a basic implementation of redis' **master server replication**, allowing replicas to synchronize with the master for data consistency.
The server supports **replica synchronization and replica command acknowledgment** to ensure consistency and coordination between the master server and its replicas. Replication is implemented to allow replicas to stay synchronized with the master server, especially for critical commands and state updates. The commands related to replica synchronization include:



## Project Goals
This project is designed to:
1. Explore Go'store capabilities in building a custom server.
2. Understand concurrency patterns and networking in Go.
3. Serve as a practical example for custom lightweight Redis-like use cases.
