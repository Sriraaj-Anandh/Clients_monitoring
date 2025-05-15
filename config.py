import os
from dotenv import load_dotenv

load_dotenv()


CLOUD_DB_CONFIG = {
    "host": os.getenv("CLOUD_DB_HOST"),
    "user": os.getenv("CLOUD_DB_USER"),
    "password": os.getenv("CLOUD_DB_PASSWORD"),
    "database": os.getenv("CLOUD_DB_NAME"),
    "port": int(os.getenv("CLOUD_DB_PORT", 3306)),
    "cursorclass": None  
}

def load_projects():
    """Load multiple projects with individual DB credentials from .env."""
    count = int(os.getenv("PROJECT_COUNT", 0))
    projects = []

    for i in range(1, count + 1):
        prefix = f"PROJECT_{i}_"

        name = os.getenv(f"{prefix}NAME")
        db_name = os.getenv(f"{prefix}DB_NAME")
        tables = os.getenv(f"{prefix}TABLES", "").split(",")

        db_host = os.getenv(f"{prefix}DB_HOST")
        db_user = os.getenv(f"{prefix}DB_USER")
        db_password = os.getenv(f"{prefix}DB_PASSWORD")
        db_port = int(os.getenv(f"{prefix}DB_PORT", 3306))
        db_type = os.getenv(f"{prefix}DB_TYPE", "mysql").lower()

        if not (name and db_name and db_host and db_user and db_password and tables):
            continue  # skip incomplete project config

        project_config = {
            "project_name": name,
            "tables": [t.strip() for t in tables if t.strip()],
            "db_type": db_type,
            "db_config": {
                "host": db_host,
                "user": db_user,
                "password": db_password,
                "database": db_name,
                "port": db_port,
                "cursorclass": None  # Will be set later
            }
        }

        projects.append(project_config)

    return projects

