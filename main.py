import csv
import requests
import sqlite3
import json
import datetime
from urllib.parse import urljoin

# Helper functions for current date/time
def get_current_timestamp():
    return datetime.datetime.now().isoformat()

def get_current_date():
    return datetime.date.today().isoformat()

def create_tables(conn):
    """
    Creates the SQLite tables with updated schema including versioning and processing runs.
    """
    c = conn.cursor()
    # Servers table
    c.execute('''
        CREATE TABLE IF NOT EXISTS servers (
            url TEXT PRIMARY KEY,
            short_name TEXT,
            description TEXT
        )
    ''')
    # Resources table with versioning
    c.execute('''
        CREATE TABLE IF NOT EXISTS resources (
            url TEXT,
            type TEXT,
            subtype TEXT,
            parent_url TEXT,
            server_url TEXT,
            accessible INTEGER,
            metadata TEXT CHECK(json_valid(metadata)),
            name TEXT,
            description TEXT,
            start_timestamp TEXT,
            end_timestamp TEXT,
            active INTEGER,
            PRIMARY KEY (url, start_timestamp)
        )
    ''')
    # Fields table with versioning
    c.execute('''
        CREATE TABLE IF NOT EXISTS fields (
            resource_url TEXT,
            field_name TEXT,
            field_type TEXT,
            alias TEXT,
            start_timestamp TEXT,
            end_timestamp TEXT,
            active INTEGER,
            PRIMARY KEY (resource_url, field_name, start_timestamp)
        )
    ''')
    # Domains table (no versioning required)
    c.execute('''
        CREATE TABLE IF NOT EXISTS domains (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            resource_url TEXT,
            field_name TEXT,
            domain_code TEXT,
            domain_value TEXT,
            FOREIGN KEY(resource_url) REFERENCES resources(url)
        )
    ''')
    # Processing runs table: records each server processing run.
    c.execute('''
        CREATE TABLE IF NOT EXISTS processing_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            server_url TEXT,
            run_date TEXT,
            start_timestamp TEXT,
            end_timestamp TEXT,
            FOREIGN KEY(server_url) REFERENCES servers(url)
        )
    ''')
    conn.commit()

def insert_server(conn, url, short_name, description):
    """Inserts or updates a server record."""
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO servers (url, short_name, description)
        VALUES (?, ?, ?)
    ''', (url, short_name, description))
    conn.commit()

def update_resource(conn, url, res_type, res_subtype, parent_url, server_url, accessible, metadata, name, description):
    """
    Versioned update for a resource record.
    If an active record exists and metadata is unchanged, do nothing.
    Otherwise, end-date the active record and insert a new record.
    """
    now = get_current_timestamp()
    cur = conn.cursor()
    new_metadata_str = json.dumps(metadata, sort_keys=True)
    cur.execute("SELECT metadata FROM resources WHERE url = ? AND active = 1", (url,))
    row = cur.fetchone()
    if row is not None:
        old_metadata = row[0]
        if old_metadata == new_metadata_str:
            # Unchanged: no update required.
            return
        else:
            # End-date the old record.
            cur.execute("UPDATE resources SET end_timestamp = ?, active = 0 WHERE url = ? AND active = 1", (now, url))
            conn.commit()
    # Insert new record with active flag = 1.
    cur.execute('''
         INSERT INTO resources (url, type, subtype, parent_url, server_url, accessible, metadata, name, description, start_timestamp, end_timestamp, active)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 1)
    ''', (url, res_type, res_subtype, parent_url, server_url, int(accessible), new_metadata_str, name, description, now))
    conn.commit()

def update_field(conn, resource_url, field):
    """
    Versioned update for a field record.
    """
    now = get_current_timestamp()
    cur = conn.cursor()
    new_field_type = field.get("type")
    new_alias = field.get("alias")
    cur.execute("SELECT field_type, alias FROM fields WHERE resource_url = ? AND field_name = ? AND active = 1", 
                (resource_url, field.get("name")))
    row = cur.fetchone()
    if row is not None:
        old_field_type, old_alias = row
        if old_field_type == new_field_type and old_alias == new_alias:
            return
        else:
            cur.execute("UPDATE fields SET end_timestamp = ?, active = 0 WHERE resource_url = ? AND field_name = ? AND active = 1",
                        (now, resource_url, field.get("name")))
            conn.commit()
    cur.execute('''
         INSERT INTO fields (resource_url, field_name, field_type, alias, start_timestamp, end_timestamp, active)
         VALUES (?, ?, ?, ?, ?, NULL, 1)
    ''', (resource_url, field.get("name"), new_field_type, new_alias, now))
    conn.commit()

def insert_domain(conn, resource_url, field_name, code, value):
    """
    Inserts a domain record with separate code and value fields.
    """
    c = conn.cursor()
    c.execute('''
        INSERT INTO domains (resource_url, field_name, domain_code, domain_value)
        VALUES (?, ?, ?, ?)
    ''', (resource_url, field_name, str(code), value))
    conn.commit()

def process_field_domain(conn, resource_url, field):
    """
    Processes a field's domain (if available) by inserting each coded value into the domains table.
    """
    domain = field.get("domain")
    if domain and "codedValues" in domain:
        for cv in domain["codedValues"]:
            code = cv.get("code")
            name_val = cv.get("name")
            insert_domain(conn, resource_url, field.get("name"), code, name_val)

def fetch_json(url):
    """
    Fetches JSON data from a URL using the ArcGIS REST API format.
    """
    try:
        response = requests.get(url, params={'f': 'json'}, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data, True
    except Exception as e:
        print(f"Failed to fetch {url}: {e}")
        return None, False

def classify_resource(url, data, parent_url):
    """
    Determines the type and subtype based on the URL and data.
    Rules:
      - If there's no parent, it's a server.
      - If the data contains folders or services, it's a folder.
      - If the URL ends with 'Server', it's a service with subtype equal to the last segment.
      - Otherwise, it's unknown.
    """
    if parent_url is None:
        return "server", None
    if data and ("folders" in data or "services" in data):
        return "folder", None
    if url.endswith("Server"):
        subtype = url.split("/")[-1]
        return "service", subtype
    return "unknown", None

def crawl(url, conn, server_url, parent_url=None, visited=None):
    """
    Recursively crawls an ArcGIS REST endpoint.
    Determines name and description from metadata and handles domains and versioning.
    """
    if visited is None:
        visited = set()
    if url in visited:
        return
    visited.add(url)
    print(f"Crawling: {url}")
    
    data, accessible = fetch_json(url)
    res_type, res_subtype = classify_resource(url, data, parent_url)
    
    resource_name = None
    resource_description = None
    if res_type == "service" and data:
        resource_name = data.get("mapName") or data.get("name")
        resource_description = data.get("serviceDescription")
    elif res_type == "layer" and data:
        resource_name = data.get("name")
        resource_description = data.get("description")
        res_subtype = data.get("type")
    
    if data is None:
        data = {}
    update_resource(conn, url, res_type, res_subtype, parent_url, server_url, accessible, data, resource_name, resource_description)
    
    if not accessible or data is None:
        return
    
    # Crawl folders
    if "folders" in data:
        for folder in data["folders"]:
            folder_url = urljoin(url + "/", folder)
            crawl(folder_url, conn, server_url, url, visited)
    
    # Crawl services.
    if "services" in data:
        for service in data["services"]:
            service_name_field = service.get("name")
            service_type_field = service.get("type")  # typically ends with 'Server'
            current_folder = url.rstrip("/").split("/")[-1]
            if current_folder.lower() not in ["rest", "services"]:
                if service_name_field.startswith(current_folder + "/"):
                    service_name_field = service_name_field[len(current_folder) + 1:]
            service_url = urljoin(url + "/", f"{service_name_field}/{service_type_field}")
            # Remove the update_resource call here; just recursively crawl if not already visited.
            if service_url not in visited:
                crawl(service_url, conn, server_url, url, visited)

    
    # Process layers if available.
    if "layers" in data:
        for layer in data["layers"]:
            layer_id = layer.get("id")
            layer_url = urljoin(url + "/", str(layer_id))
            layer_data, accessible_layer = fetch_json(layer_url)
            if accessible_layer and layer_data:
                l_name = layer_data.get("name")
                l_description = layer_data.get("description")
                l_subtype = layer_data.get("type")
                update_resource(conn, layer_url, "layer", l_subtype, url, server_url, True, layer_data, l_name, l_description)
                for field in (layer_data.get("fields") or []):
                    update_field(conn, layer_url, field)
                    process_field_domain(conn, layer_url, field)
            else:
                update_resource(conn, layer_url, "layer", None, url, server_url, accessible_layer, layer, None, None)
    
    # Process tables similarly.
    if "tables" in data:
        for table in data["tables"]:
            table_id = table.get("id")
            table_url = urljoin(url + "/", str(table_id))
            table_data, accessible_table = fetch_json(table_url)
            if accessible_table and table_data:
                t_name = table_data.get("name")
                t_description = table_data.get("description")
                update_resource(conn, table_url, "table", None, url, server_url, True, table_data, t_name, t_description)
                for field in (table_data.get("fields") or []):
                    update_field(conn, table_url, field)
                    process_field_domain(conn, table_url, field)
            else:
                update_resource(conn, table_url, "table", None, url, server_url, accessible_table, table, None, None)


def start_processing_run(conn, server_url):
    """
    Inserts a new processing run record for a server.
    """
    now = get_current_timestamp()
    today = get_current_date()
    cur = conn.cursor()
    cur.execute('''
         INSERT INTO processing_runs (server_url, run_date, start_timestamp)
         VALUES (?, ?, ?)
    ''', (server_url, today, now))
    conn.commit()
    return cur.lastrowid

def end_processing_run(conn, run_id):
    """
    Updates a processing run record with the end timestamp.
    """
    now = get_current_timestamp()
    cur = conn.cursor()
    cur.execute('''
         UPDATE processing_runs SET end_timestamp = ? WHERE run_id = ?
    ''', (now, run_id))
    conn.commit()

def should_process_server(conn, server_url, revisit_days):
    """
    Determines whether a server should be processed based on revisit_days.
    If revisit_days is 0, process unconditionally.
    Otherwise, checks the last run date for the server.
    """
    if revisit_days == 0:
        return True
    cur = conn.cursor()
    cur.execute("SELECT run_date FROM processing_runs WHERE server_url = ? ORDER BY run_date DESC LIMIT 1", (server_url,))
    row = cur.fetchone()
    if row is None:
        return True
    last_run_date = datetime.datetime.strptime(row[0], "%Y-%m-%d").date()
    today = datetime.date.today()
    diff = (today - last_run_date).days
    return diff >= revisit_days

def load_servers(file_path="servers.csv"):
    """
    Loads server info from a CSV file with headers: 
    url,short_name,description,revisit_days,to_process.
    Only returns servers where to_process is truthy.
    """
    servers = []
    truthy = {"y", "t", "true", "1"}
    try:
        with open(file_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                to_process = row.get("to_process", "").strip().lower()
                if to_process in truthy:
                    url = row.get("url", "").strip()
                    short_name = row.get("short_name", "").strip()
                    description = row.get("description", "").strip()
                    try:
                        revisit_days = int(row.get("revisit_days", "0").strip())
                    except:
                        revisit_days = 0
                    if url:
                        servers.append((url, short_name, description, revisit_days))
        return servers
    except FileNotFoundError:
        print("Error: servers.csv not found. Please create the CSV file with headers: url,short_name,description,revisit_days,to_process")
        return []

def main():
    servers = load_servers()
    if not servers:
        print("No servers to process. Exiting.")
        return

    conn = sqlite3.connect("arcgis_metadata.db")
    create_tables(conn)

    for server in servers:
        server_url, short_name, description, revisit_days = server
        if not should_process_server(conn, server_url, revisit_days):
            print(f"Skipping {server_url} as revisit_days requirement not met.")
            continue
        insert_server(conn, server_url, short_name, description)
        run_id = start_processing_run(conn, server_url)
        crawl(server_url, conn, server_url)
        end_processing_run(conn, run_id)

    conn.close()
    print("Crawling complete. Data saved to arcgis_metadata.db")

if __name__ == "__main__":
    main()
