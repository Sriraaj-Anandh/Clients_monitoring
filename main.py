import pymysql
import psycopg2
import psycopg2.extras
import hashlib
import time
from datetime import datetime
from collections import defaultdict

from config import CLOUD_DB_CONFIG, load_projects

# Load project metadata
PROJECTS = {}
for proj in load_projects():
    project_name = proj["project_name"]
    PROJECTS[project_name] = {
        "db_type": proj.get("db_type", "mysql"),  # default to MySQL
        "db_name": proj["db_config"]["database"],
        "tables": proj["tables"],
        "db_host": proj["db_config"]["host"],
        "db_user": proj["db_config"]["user"],
        "db_password": proj["db_config"]["password"],
        "db_port": proj["db_config"]["port"]
    }

# Track table fingerprints
fingerprints = {}

def compute_fingerprint(rows):
    hasher = hashlib.sha256()
    for row in rows:
        for value in row.values():
            hasher.update(str(value).encode())
    return hasher.hexdigest()

def fetch_rows(cursor, table, db_type):
    if db_type == "postgres":
        cursor.execute(f'SELECT * FROM "{table}"')
    else:
        cursor.execute(f"SELECT * FROM {table}")
    return cursor.fetchall()

def get_local_connection(project_data):
    if project_data['db_type'] == "postgres":
        return psycopg2.connect(
            host=project_data['db_host'],
            user=project_data['db_user'],
            password=project_data['db_password'],
            dbname=project_data['db_name'],
            port=project_data['db_port'],
            cursor_factory=psycopg2.extras.RealDictCursor
        )
    else:  # MySQL
        return pymysql.connect(
            host=project_data['db_host'],
            user=project_data['db_user'],
            password=project_data['db_password'],
            database=project_data['db_name'],
            port=project_data['db_port'],
            cursorclass=pymysql.cursors.DictCursor
        )

def monitor_tables():
    cloud_conn = pymysql.connect(
        **{**CLOUD_DB_CONFIG, "cursorclass": pymysql.cursors.DictCursor}
    )

    try:
        while True:
            now = datetime.utcnow()

            for project_name, project_data in PROJECTS.items():
                total_update_count = 0
                top_user_map = defaultdict(int)
                table_update_data = []

                local_conn = get_local_connection(project_data)

                with local_conn.cursor() as cursor:
                    for table in project_data['tables']:
                        rows = fetch_rows(cursor, table, project_data['db_type'])
                        fingerprint = compute_fingerprint(rows)

                        key = f"{project_name}_{table}"
                        if fingerprints.get(key) != fingerprint:
                            fingerprints[key] = fingerprint
                            update_count = len(rows)
                            total_update_count += update_count

                            last_updated = now

                            top_user = None
                            if rows:
                                sample_row = rows[0]
                                for field in ["incharge_name", "technician", "user", "created_by"]:
                                    if field in sample_row:
                                        top_user = sample_row[field]
                                        break

                            if top_user:
                                top_user_map[top_user] += update_count

                            table_update_data.append({
                                "table_name": table,
                                "update_count": update_count,
                                "last_updated": last_updated,
                                "top_user": top_user,
                                "top_user_count": update_count if top_user else 0,
                                "day": now.date(),
                                "weekday": now.weekday(),
                                "month": now.month
                            })

                local_conn.close()

                if table_update_data:
                    total_users = len(top_user_map)
                    top_user = max(top_user_map, key=top_user_map.get) if top_user_map else None
                    top_user_count = top_user_map[top_user] if top_user else 0

                    project_slug = project_name.lower().replace(" ", "_")
                    cloud_table_name = f"update_metrics_{project_slug}"

                    with cloud_conn.cursor() as cloud_cursor:
                        cloud_cursor.execute(f"""
                            CREATE TABLE IF NOT EXISTS {cloud_table_name} (
                                id INT AUTO_INCREMENT PRIMARY KEY,
                                project_name VARCHAR(255) NOT NULL,
                                table_name VARCHAR(255) NOT NULL,
                                update_count INT NOT NULL,
                                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                top_user VARCHAR(255),
                                top_user_count INT,
                                total_users INT NOT NULL,
                                detected_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                day DATE,
                                weekday INT,
                                month INT
                            );
                        """)

                        for data in table_update_data:
                            cloud_cursor.execute(f"""
                                INSERT INTO {cloud_table_name} (
                                    project_name,
                                    table_name,
                                    update_count,
                                    last_updated,
                                    top_user,
                                    top_user_count,
                                    total_users,
                                    detected_timestamp,
                                    day,
                                    weekday,
                                    month
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (
                                project_name,
                                data["table_name"],
                                data["update_count"],
                                data["last_updated"],
                                data["top_user"],
                                data["top_user_count"],
                                total_users,
                                now,
                                data["day"],
                                data["weekday"],
                                data["month"]
                            ))

                    cloud_conn.commit()

            time.sleep(30)

    finally:
        cloud_conn.close()

if __name__ == "__main__":
    monitor_tables()
