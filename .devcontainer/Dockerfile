FROM python:3.11-slim

# Install dependencies
RUN apt-get update && apt-get install -y curl

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Set Poetry path
ENV PATH="/root/.local/bin:$PATH"

# Create working directory
WORKDIR /workspace


