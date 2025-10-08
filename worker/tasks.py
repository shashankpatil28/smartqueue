# In worker/tasks.py

import time
import random

def send_welcome_email(payload: dict):
    """
    Simulates sending a welcome email to a new user. 📧
    """
    email = payload.get("email", "no-email-provided")
    name = payload.get("name", "User")
    print(f"Starting task: send_welcome_email to {email}")
    
    # Simulate I/O-bound work like a network request
    time.sleep(random.uniform(1, 3)) 
    
    print(f"✅ Task complete: Welcome email sent to {name} at {email}")
    return {"status": "success", "email": email}

def process_payment(payload: dict):
    """
    Simulates processing a payment. 💳
    """
    user_id = payload.get("user_id", "N/A")
    amount = payload.get("amount", 0)
    print(f"Starting task: process_payment for user {user_id}")
    
    if amount <= 0:
        print(f"❌ Task failed: Invalid payment amount ${amount} for user {user_id}")
        raise ValueError("Payment amount must be positive.")
        
    # Simulate a mix of CPU/IO work
    time.sleep(random.uniform(0.5, 2))
    
    print(f"✅ Task complete: Payment of ${amount} processed for user {user_id}")
    return {"status": "success", "user_id": user_id, "amount_processed": amount}


# A registry to map task_name strings to the actual functions
TASK_REGISTRY = {
    "send_welcome_email": send_welcome_email,
    "process_payment": process_payment,
}