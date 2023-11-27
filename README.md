# Requirements

Python 3.11+

# Installation

```bash
pip install poetry
```

# Install Client

```bash
cd client && poetry shell && poetry install
```

# Install Server

```bash
cd client && poetry shell && poetry install
```

# Usage

## server

```bash
cd server && poetry shell && python server.py --directory <directory_to_write_to>
--port <port>
```

## client

```bash
cd client && poetry shell && python monitory.py --directory <directory_to_monitor>
--url http://<server_ip:port>/upload
--debounce <debounce_time_in_seconds>
```
