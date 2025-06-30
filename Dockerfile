# Base image with Python and slim packages
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .

# Use pip to install requirements
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files into the container
COPY . .

# Run the ETL script when container starts
CMD ["python", "etl_script.py"]
