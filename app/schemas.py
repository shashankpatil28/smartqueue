# In app/schemas.py

import uuid
from pydantic import BaseModel, Field
from typing import Dict, Any

class Task(BaseModel):
    # Generates a unique ID for each task automatically
    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    
    # The name of the function the worker should execute
    task_name: str
    
    # Priority level for the task. We'll use this later for the priority scheduler.
    priority: int = Field(default=5, ge=0, le=10) # Range from 0 (lowest) to 10 (highest)
    
    # A dictionary to hold any arguments for the task function
    payload: Dict[str, Any] = Field(default_factory=dict)