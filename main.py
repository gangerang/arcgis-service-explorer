import csv
import requests
import sqlite3
import json
from urllib.parse import urljoin

def create_tables(conn):
    """
    Creates the SQLite tables with the updated schema.
    The resources table now includes 'subtype', 'name', and 'description' columns.
    Also creates a new 'domains' table with separate fields for the code and value.
    """
    c = conn.cursor()
    # Table for servers.
    c.execute('''
        CREATE TABLE IF NOT EXISTS servers (
            url TEXT PRIMARY KEY,
            short_name TEXT,
            description TEXT
        )
    ''')
    # Resources table.
    c.execute('''
        CREATE TABLE IF NOT EXISTS resources (
            url TEXT PRIMARY KEY,
            type TEXT,
            subtype TEXT,
            parent_url TEXT,
            server_url TEXT,
            accessible INTEGER,
            metadata TEXT CHECK(json_valid(metadata)),
            name TEXT,
            description TEXT,
            FOREIGN KEY(server_url) REFERENCES servers(url)
        )
    ''')
    # Fields table.
    c.execute('''
        CREATE TABLE IF NOT EXISTS fields (
            resource_url TEXT,
            field_name TEXT,
            field_type TEXT,
            alias TEXT,
            PRIMARY KEY (resource_url, field_name),
            FOREIGN KEY(resource_url) REFERENCES resources(url)
        )
    ''')
    # Domains table: each row stores one code-value pair.
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
    conn.commit()

def insert_server(conn, url, short_name, description):
    """Inserts or updates a server record."""
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO servers (url, short_name, description)
        VALUES (?, ?, ?)
    ''', (url, short_name, description))
    conn.commit()

def insert_resource(conn, url, res_type, res_subtype, parent_url, server_url, accessible, metadata, name=None, description=None):
    """Inserts or updates a resource record into the database."""
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO resources (url, type, subtype, parent_url, server_url, accessible, metadata, name, description)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (url, res_type, res_subtype, parent_url, server_url, int(accessible), json.dumps(metadata), name, description))
    conn.commit()

def insert_field(conn, resource_url, field):
    """Inserts or updates a field record associated with a resource."""
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO fields (resource_url, field_name, field_type, alias)
        VALUES (?, ?, ?, ?)
    ''', (resource_url, field.get('name'), field.get('type'), field.get('alias')))
    conn.commit()

def insert_domain(conn, resource_url, field_name, code, value):
    """
    Inserts a domain record with separate fields for code and value.
    resource_url is typically the layer (or table) URL.
    """
    c = conn.cursor()
    c.execute('''
        INSERT INTO domains (resource_url, field_name, domain_code, domain_value)
        VALUES (?, ?, ?, ?)
    ''', (resource_url, field_name, str(code), value))
    conn.commit()

def process_field_domain(conn, resource_url, field):
    """
    Checks if a field has a domain with coded values.
    If found, iterates through the codedValues list and inserts each code/value pair into the domains table.
    """
    domain = field.get("domain")
    if domain and "codedValues" in domain:
        for cv in domain["codedValues"]:
            code = cv.get("code")
            name = cv.get("name")
            insert_domain(conn, resource_url, field.get("name"), code, name)

def fetch_json(url):
    """Fetches JSON data from a URL using the ArcGIS REST API format."""
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
    - If there's no parent, this is the server (type 'server', subtype None).
    - If the endpoint returns folders or services (and has a parent), classify as folder.
    - If the URL ends with 'Server', classify as service; subtype is the last URL segment.
    - Otherwise, default to 'unknown'.
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
    Recursively crawls an ArcGIS REST endpoint and classifies resources.
    Determines 'name' and 'description' based on the top level of the metadata.
    Also processes domain information for any fields that include a domain.
    """
    if visited is None:
        visited = set()
    if url in visited:
        return
    visited.add(url)
    print(f"Crawling: {url}")
    
    data, accessible = fetch_json(url)
    
    # Classify the resource.
    res_type, res_subtype = classify_resource(url, data, parent_url)
    
    # Initialize name and description.
    resource_name = None
    resource_description = None

    # For type service, extract from metadata.
    if res_type == "service" and data:
        resource_name = data.get("mapName") or data.get("name")
        resource_description = data.get("serviceDescription")
    # For type layer, extract from metadata and override subtype.
    elif res_type == "layer" and data:
        resource_name = data.get("name")
        resource_description = data.get("description")
        res_subtype = data.get("type")
    
    # Insert the current resource.
    insert_resource(conn, url, res_type, res_subtype, parent_url, server_url, accessible, data if data else {}, resource_name, resource_description)
    
    if not accessible or data is None:
        return

    # Crawl folders.
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
            service_res_type, service_res_subtype = classify_resource(service_url, service, url)
            s_name = service.get("mapName") or service.get("name")
            s_description = service.get("serviceDescription")
            insert_resource(conn, service_url, service_res_type, service_res_subtype, url, server_url, True, service, s_name, s_description)
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
                insert_resource(conn, layer_url, "layer", l_subtype, url, server_url, True, layer_data, l_name, l_description)
                fields = layer_data.get("fields")
                if fields:
                    for field in fields:
                        insert_field(conn, layer_url, field)
                        process_field_domain(conn, layer_url, field)
            else:
                insert_resource(conn, layer_url, "layer", None, url, server_url, accessible_layer, layer)

    # Process tables similarly.
    if "tables" in data:
        for table in data["tables"]:
            table_id = table.get("id")
            table_url = urljoin(url + "/", str(table_id))
            table_data, accessible_table = fetch_json(table_url)
            if accessible_table and table_data:
                t_name = table_data.get("name")
                t_description = table_data.get("description")
                insert_resource(conn, table_url, "table", None, url, server_url, True, table_data, t_name, t_description)
                if "fields" in table_data:
                    for field in table_data["fields"]:
                        insert_field(conn, table_url, field)
                        process_field_domain(conn, table_url, field)
            else:
                insert_resource(conn, table_url, "table", None, url, server_url, accessible_table, table)

def load_servers(file_path="servers.csv"):
    """
    Load server information from a CSV file with headers.
    Expected header: url,short_name,description,to_process
    Only rows where the 'to_process' field indicates truthiness will be returned.
    Truth values (case-insensitive): y, t, true, 1
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
                    if url:  # Only process rows with a valid URL.
                        servers.append((url, short_name, description))
        return servers
    except FileNotFoundError:
        print(f"Error: {file_path} not found. Please create the CSV file with headers: url,short_name,description,to_process")
        return []

def main():
    # Load servers from CSV.
    servers = load_servers()

    if not servers:
        print("No servers to process. Exiting.")
        return

    # Connect to (or create) the SQLite database.
    conn = sqlite3.connect("arcgis_metadata.db")
    create_tables(conn)

    # Insert each server into the servers table and crawl its endpoints.
    for server in servers:
        server_url, short_name, description = server
        insert_server(conn, server_url, short_name, description)
        crawl(server_url, conn, server_url)

    conn.close()
    print("Crawling complete. Data saved to arcgis_metadata.db")

if __name__ == "__main__":
    main()
