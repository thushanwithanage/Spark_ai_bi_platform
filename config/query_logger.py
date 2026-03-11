import json
import os
from datetime import datetime
from config.file_path import LOG_DIR
import uuid

def log_query(question, sql, status, execution_time=None, message=None):
    os.makedirs(LOG_DIR, exist_ok=True)

    today = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(LOG_DIR, f"query_logs_{today}.json")

    log_entry = {
        "query_id": str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "question": question,
        "sql": sql,
        "status": status,
        "execution_time": execution_time,
        "message": message
    }

    # Append to file or create new file
    if os.path.exists(log_file):
        with open(log_file, "r+") as f:
            logs = json.load(f)
            logs.append(log_entry)
            f.seek(0)
            json.dump(logs, f, indent=2)
    else:
        with open(log_file, "w") as f:
            json.dump([log_entry], f, indent=2)