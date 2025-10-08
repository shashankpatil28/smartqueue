# In app/core/config.py

import os

# Reads the Redis host from the environment variable we set in docker-compose.yml
# Defaults to 'localhost' if the variable is not set
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = 6379