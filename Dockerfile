# Build a Docker image for mmoney nodes.
# Start with a slim Python base (smaller than 'python:3.11' which includes extra stuff).
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Copy project files into the container
COPY . /app

# Set Python path so modules can be imported from /app
ENV PYTHONPATH=/app

# Install Python dependencies
# This installs all packages from requirements.txt (rpyc, etcd3, PyJWT, cryptography, etc.)
RUN pip install --no-cache-dir -r requirements.txt

# Expose the default node port (clients and other nodes connect here)
EXPOSE 18861

# Default command: run the node server
# (docker-compose.yml can override this per node)
CMD ["python", "services/node_server.py"]
