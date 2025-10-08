# In app/main.py

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

from .core.config import REDIS_HOST, REDIS_PORT

# A lifespan manager is the modern way in FastAPI to handle startup/shutdown logic
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Code to run on startup
    print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
    try:
        app.state.redis = await redis.from_url(
            f"redis://{REDIS_HOST}:{REDIS_PORT}",
            decode_responses=True
        )
        await app.state.redis.ping()
        print("Successfully connected to Redis.")
    except Exception as e:
        print(f"Could not connect to Redis: {e}")
        app.state.redis = None
    
    yield
    
    # Code to run on shutdown
    if app.state.redis:
        await app.state.redis.close()
        print("Redis connection closed.")

# Create the FastAPI app instance with the lifespan manager
app = FastAPI(title="SmartQueue API", version="1.0", lifespan=lifespan)

@app.get("/")
def read_root():
    """
    Root endpoint for a simple health check.
    """
    return {"message": "SmartQueue API is running"}

@app.get("/test-redis")
async def test_redis_connection():
    """
    Endpoint to verify the connection to Redis is alive.
    """
    if not app.state.redis:
        raise HTTPException(status_code=503, detail="Redis connection not available")
    
    try:
        # The PING command is a standard way to check Redis connectivity
        await app.state.redis.ping()
        return {"status": "success", "message": "Redis connection is healthy"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to communicate with Redis: {str(e)}")