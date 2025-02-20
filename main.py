import requests
import sqlite3
import json
from urllib.parse import urljoin

def create_tables(conn):
    """Creates the SQLite tables to store resource metadata and fields."""
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS resources (
            url TEXT PRIMARY KEY,
            type TEXT,
            parent_url TEXT,
            accessible INTEGER,
            metadata TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS fields (
            resource_url TEXT,
            field_name TEXT,
            field_type TEXT,
            alias TEXT,
            PRIMARY KEY (resource_url, field_name)
        )
    ''')
    conn.commit()

def insert_resource(conn, url, res_type, parent_url, accessible, metadata):
    """Inserts or updates a resource record into the database."""
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO resources (url, type, parent_url, accessible, metadata)
        VALUES (?, ?, ?, ?, ?)
    ''', (url, res_type, parent_url, int(accessible), json.dumps(metadata)))
    conn.commit()

def insert_field(conn, resource_url, field):
    """Inserts or updates a field record associated with a resource."""
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO fields (resource_url, field_name, field_type, alias)
        VALUES (?, ?, ?, ?)
    ''', (resource_url, field.get('name'), field.get('type'), field.get('alias')))
    conn.commit()

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

def crawl(url, conn, parent_url=None, visited=None):
    """Recursively crawls an ArcGIS REST endpoint."""
    if visited is None:
        visited = set()
    if url in visited:
        return
    visited.add(url)
    print(f"Crawling: {url}")
    
    data, accessible = fetch_json(url)
    # Determine resource type based on expected keys
    res_type = "unknown"
    if data:
        if "folders" in data or "services" in data:
            res_type = "server_root"
        elif "layers" in data or "tables" in data:
            res_type = "service"
    # Save the current resource in the database.
    insert_resource(conn, url, res_type, parent_url, accessible, data if data else {})
    
    if not accessible or data is None:
        return

    # Crawl folders
    if "folders" in data:
        for folder in data["folders"]:
            folder_url = urljoin(url + "/", folder)
            crawl(folder_url, conn, url, visited)

    # Crawl services
    if "services" in data:
        for service in data["services"]:
            service_name = service.get("name")
            service_type = service.get("type")
            # Construct the URL for the service. This assumes the typical pattern:
            # <base_url>/<service_name>/<service_type>
            service_url = urljoin(url + "/", f"{service_name}/{service_type}")
            crawl(service_url, conn, url, visited)

    # Process layers if available. Some services provide layer metadata directly.
    if "layers" in data:
        for layer in data["layers"]:
            layer_id = layer.get("id")
            # Assuming the layer can be reached by appending its id.
            layer_url = urljoin(url + "/", str(layer_id))
            insert_resource(conn, layer_url, "layer", url, True, layer)
            # If the layer has field metadata, store it.
            if "fields" in layer:
                for field in layer["fields"]:
                    insert_field(conn, layer_url, field)

    # Process tables similarly.
    if "tables" in data:
        for table in data["tables"]:
            table_id = table.get("id")
            table_url = urljoin(url + "/", str(table_id))
            insert_resource(conn, table_url, "table", url, True, table)
            if "fields" in table:
                for field in table["fields"]:
                    insert_field(conn, table_url, field)

def main():
    # List of base ArcGIS REST endpoints to start crawling.
    servers = [
        "https://www.lmbc.nsw.gov.au/arcgis/rest/services"
        # Add additional server URLs as needed.
    ]
    # Connect to (or create) the SQLite database.
    conn = sqlite3.connect("arcgis_metadata.db")
    create_tables(conn)
    
    for server in servers:
        crawl(server, conn)
    
    conn.close()
    print("Crawling complete. Data saved to arcgis_metadata.db")

if __name__ == "__main__":
    main()
