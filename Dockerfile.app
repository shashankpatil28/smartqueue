# In Dockerfile.app

# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the dependencies file and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
COPY ./app /app

# Command to run the app when the container launches
# --reload will auto-restart the server on code changes
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]