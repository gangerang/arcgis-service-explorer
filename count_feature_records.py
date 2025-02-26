import csv
import sqlite3
import requests
import datetime
import json
from urllib.parse import urljoin

# Helper functions for date/time.
def get_current_timestamp():
    return datetime.datetime.now().isoformat()

def get_current_date():
    return datetime.date.today().isoformat()

def create_counts_tables(conn):
    """
    Creates the counts and count_runs tables.
    The counts table now includes an 'active' field.
    """
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS counts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            layer_url TEXT,
            record_count INTEGER,
            timestamp TEXT,
            active INTEGER
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS count_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            server_url TEXT,
            run_date TEXT,
            start_timestamp TEXT,
            end_timestamp TEXT
        )
    ''')
    conn.commit()

def load_servers_for_counts(file_path="servers.csv"):
    """
    Loads server info from a CSV file with headers:
      url,short_name,description,revisit_days,count_revisit_days,count_to_process
    Only returns rows where count_to_process is truthy.
    """
    servers = []
    truthy = {"y", "t", "true", "1"}
    try:
        with open(file_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                count_to_process = row.get("count_to_process", "").strip().lower()
                if count_to_process in truthy:
                    url = row.get("url", "").strip()
                    short_name = row.get("short_name", "").strip()
                    description = row.get("description", "").strip()
                    try:
                        count_revisit_days = int(row.get("count_revisit_days", "0").strip())
                    except:
                        count_revisit_days = 0
                    if url:
                        servers.append((url, short_name, description, count_revisit_days))
        return servers
    except FileNotFoundError:
        print("Error: servers.csv not found. Please create the CSV file with appropriate headers.")
        return []

def should_process_server_count(conn, server_url, count_revisit_days):
    """
    Determines if a server should be processed for counts.
    If count_revisit_days is 0, process unconditionally.
    Otherwise, check the last count run date (calendar days).
    """
    if count_revisit_days == 0:
        return True
    cur = conn.cursor()
    cur.execute("SELECT run_date FROM count_runs WHERE server_url = ? ORDER BY run_date DESC LIMIT 1", (server_url,))
    row = cur.fetchone()
    if row is None:
        return True
    last_run_date = datetime.datetime.strptime(row[0], "%Y-%m-%d").date()
    today = datetime.date.today()
    diff = (today - last_run_date).days
    return diff >= count_revisit_days

def start_count_run(conn, server_url):
    """
    Records the start of a count run.
    """
    now = get_current_timestamp()
    today = get_current_date()
    cur = conn.cursor()
    cur.execute('''
         INSERT INTO count_runs (server_url, run_date, start_timestamp)
         VALUES (?, ?, ?)
    ''', (server_url, today, now))
    conn.commit()
    return cur.lastrowid

def end_count_run(conn, run_id):
    """
    Records the end of a count run.
    """
    now = get_current_timestamp()
    cur = conn.cursor()
    cur.execute('''
         UPDATE count_runs SET end_timestamp = ? WHERE run_id = ?
    ''', (now, run_id))
    conn.commit()

def get_feature_layer_record_count(layer_url):
    """
    Queries the feature layer's /query endpoint for record count.
    """
    query_url = urljoin(layer_url.rstrip("/") + "/", "query")
    params = {
        "where": "1=1",
        "returnCountOnly": "true",
        "f": "json"
    }
    try:
        response = requests.get(query_url, params=params, timeout=45)
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            print(f"Error from {layer_url}: {data['error']}")
            return 0
        return data.get("count", 0)
    except Exception as e:
        print(f"Error querying {layer_url}: {e}")
        return 0

def insert_count(conn, layer_url, record_count):
    """
    Inserts a count record into the counts table.
    First, marks any previous active count for the same layer as inactive.
    Then inserts the new record with active flag set to 1.
    """
    now = get_current_timestamp()
    cur = conn.cursor()
    # Mark previous active records as inactive.
    cur.execute('''
        UPDATE counts SET active = 0 WHERE layer_url = ? AND active = 1
    ''', (layer_url,))
    conn.commit()
    # Insert the new count record.
    cur.execute('''
         INSERT INTO counts (layer_url, record_count, timestamp, active)
         VALUES (?, ?, ?, 1)
    ''', (layer_url, record_count, now))
    conn.commit()

def get_feature_layers_for_server(conn, server_url):
    """
    Retrieves feature layers for a server from the resources table.
    We assume that a feature layer is a resource of type 'layer'
    with metadata that includes a "capabilities" property containing "Query"
    and that it has associated field records (from the fields table).
    """
    cur = conn.cursor()
    cur.execute("""
       SELECT r.url, r.metadata
       FROM resources r
       WHERE r.server_url = ?
         AND r.type = 'layer'
         AND r.active = 1
         AND lower(json_extract(r.metadata, '$.capabilities')) LIKE '%query%'
         AND EXISTS (SELECT 1 FROM fields f WHERE f.resource_url = r.url)
    """, (server_url,))
    return cur.fetchall()


def main():
    servers = load_servers_for_counts()
    if not servers:
        print("No servers to process for counts. Exiting.")
        return

    conn = sqlite3.connect("arcgis_metadata.db")
    create_counts_tables(conn)

    for server in servers:
        server_url, short_name, description, count_revisit_days = server
        if not should_process_server_count(conn, server_url, count_revisit_days):
            print(f"Skipping count processing for {server_url} as count_revisit_days requirement not met.")
            continue
        run_id = start_count_run(conn, server_url)
        layers = get_feature_layers_for_server(conn, server_url)
        print(f"Processing {len(layers)} feature layers for server: {server_url}")
        for layer in layers:
            layer_url, metadata = layer
            count = get_feature_layer_record_count(layer_url)
            print(f"Layer: {layer_url} -> {count} records")
            insert_count(conn, layer_url, count)
        end_count_run(conn, run_id)

    conn.close()
    print("Count processing complete. Data saved to arcgis_metadata.db")

if __name__ == "__main__":
    main()
