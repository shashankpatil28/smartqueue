# In app/schemas.py

import uuid
from pydantic import BaseModel, Field
from typing import Dict, Any

class Task(BaseModel):
    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_name: str
    priority: int = Field(default=5, ge=0, le=10)
    payload: Dict[str, Any] = Field(default_factory=dict)
    
    # --- ADD THE NEW FIELDS BELOW ---
    
    # Maximum number of times to retry the task
    max_retries: int = 3
    
    # Current attempt number (starts at 0 for the first run)
    attempts: int = 0