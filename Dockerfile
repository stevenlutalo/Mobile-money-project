# Build a Docker image for mmoney nodes.
# Start with a slim Python base (smaller than 'python:3.11' which includes extra stuff).
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Install system dependencies needed for LevelDB (plyvel)
# build-essential: C compiler for compiling plyvel from source
# libsnappy-dev: compression library for LevelDB
RUN apt-get update && apt-get install -y \
    build-essential \
    libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy project files into the container
COPY . /app

# Install Python dependencies
# This installs all packages from requirements.txt (rpyc, etcd3, plyvel, etc.)
RUN pip install --no-cache-dir -r requirements.txt

# Expose the default node port (clients and other nodes connect here)
EXPOSE 18861

# Default command: run the node server
# (docker-compose.yml can override this per node)
CMD ["python", "services/node_server.py"]
