from datetime import datetime, timezone
import uuid

def utc_now():
    return datetime.now(timezone.utc)

def generate_uuid():
    return str(uuid.uuid4())
