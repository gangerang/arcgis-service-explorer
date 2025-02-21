import requests
import sqlite3
import json
from urllib.parse import urljoin

# Define a mapping for service classification.
SERVICE_TYPE_MAPPING = {
    "MapServer": "map_service",
    "FeatureServer": "feature_service",
    "ImageServer": "image_service",
    "GeometryServer": "geometry_service",
    "GPServer": "geoprocessing_service"
}

def create_tables(conn):
    """
    Creates the SQLite tables to store server, resource metadata and fields.
    """
    c = conn.cursor()
    # New table to store servers.
    c.execute('''
        CREATE TABLE IF NOT EXISTS servers (
            url TEXT PRIMARY KEY,
            short_name TEXT,
            description TEXT
        )
    ''')
    # Modified resources table to include a server_url field.
    c.execute('''
        CREATE TABLE IF NOT EXISTS resources (
            url TEXT PRIMARY KEY,
            type TEXT,
            parent_url TEXT,
            server_url TEXT,
            accessible INTEGER,
            metadata TEXT CHECK(json_valid(metadata)),
            FOREIGN KEY(server_url) REFERENCES servers(url)
        )
    ''')
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
    conn.commit()

def insert_server(conn, url, short_name, description):
    """Inserts or updates a server record."""
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO servers (url, short_name, description)
        VALUES (?, ?, ?)
    ''', (url, short_name, description))
    conn.commit()

def insert_resource(conn, url, res_type, parent_url, server_url, accessible, metadata):
    """Inserts or updates a resource record into the database."""
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO resources (url, type, parent_url, server_url, accessible, metadata)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (url, res_type, parent_url, server_url, int(accessible), json.dumps(metadata)))
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

def crawl(url, conn, server_url, parent_url=None, visited=None):
    """
    Recursively crawls an ArcGIS REST endpoint and classifies services.
    The server_url parameter remains the same throughout the recursion,
    so every resource gets linked back to its originating server.
    """
    if visited is None:
        visited = set()
    if url in visited:
        return
    visited.add(url)
    print(f"Crawling: {url}")
    
    data, accessible = fetch_json(url)
    # Determine resource type based on returned keys.
    res_type = "unknown"
    if data:
        if "folders" in data or "services" in data:
            res_type = "server_root"
        elif "layers" in data or "tables" in data:
            res_type = "service"
    
    # Save the current resource with an associated server_url.
    insert_resource(conn, url, res_type, parent_url, server_url, accessible, data if data else {})
    
    if not accessible or data is None:
        return

    # Crawl folders.
    if "folders" in data:
        for folder in data["folders"]:
            folder_url = urljoin(url + "/", folder)
            crawl(folder_url, conn, server_url, url, visited)

    # Crawl services and classify them.
    if "services" in data:
        for service in data["services"]:
            service_name = service.get("name")
            service_type = service.get("type")
            # Classify the service using the mapping.
            service_res_type = SERVICE_TYPE_MAPPING.get(service_type, "unknown_service")
            # Adjust the service name if needed.
            current_folder = url.rstrip("/").split("/")[-1]
            if current_folder.lower() not in ["rest", "services"]:
                if service_name.startswith(current_folder + "/"):
                    service_name = service_name[len(current_folder) + 1:]
            # Construct the service URL.
            service_url = urljoin(url + "/", f"{service_name}/{service_type}")
            # Insert the service resource with the same server_url.
            insert_resource(conn, service_url, service_res_type, url, server_url, True, service)
            crawl(service_url, conn, server_url, url, visited)

    # Process layers if available.
    if "layers" in data:
        for layer in data["layers"]:
            layer_id = layer.get("id")
            layer_url = urljoin(url + "/", str(layer_id))
            layer_data, accessible = fetch_json(layer_url)
            if accessible and layer_data:
                insert_resource(conn, layer_url, "layer", url, server_url, True, layer_data)
                fields = layer_data.get("fields")
                if fields:
                    for field in fields:
                        insert_field(conn, layer_url, field)
            else:
                insert_resource(conn, layer_url, "layer", url, server_url, accessible, layer)

    # Process tables similarly.
    if "tables" in data:
        for table in data["tables"]:
            table_id = table.get("id")
            table_url = urljoin(url + "/", str(table_id))
            table_data, accessible = fetch_json(table_url)
            if accessible and table_data:
                insert_resource(conn, table_url, "table", url, server_url, True, table_data)
                if "fields" in table_data:
                    for field in table_data["fields"]:
                        insert_field(conn, table_url, field)
            else:
                insert_resource(conn, table_url, "table", url, server_url, accessible, table)

def load_servers(file_path="servers.txt"):
    """
    Load server information from a text file.
    Expected format per line: url,short_name,description
    For example:
    https://sampleserver6.arcgisonline.com/arcgis/rest,SampleServer,A sample ArcGIS server.
    """
    servers = []
    try:
        with open(file_path, "r") as file:
            for line in file:
                if line.strip():
                    parts = line.strip().split(',')
                    if len(parts) >= 3:
                        url = parts[0].strip()
                        short_name = parts[1].strip()
                        description = ','.join(parts[2:]).strip()  # in case description has commas
                        servers.append((url, short_name, description))
                    else:
                        print(f"Skipping line (not enough fields): {line}")
        return servers
    except FileNotFoundError:
        print(f"Error: {file_path} not found. Please create the file with one server per line in the format: url,short_name,description")
        return []

def main():
    # Load servers from file
    servers = load_servers()

    if not servers:
        print("No servers found. Exiting.")
        return

    # Connect to (or create) the SQLite database.
    conn = sqlite3.connect("arcgis_metadata.db")
    create_tables(conn)

    # Insert each server into the servers table and then crawl.
    for server in servers:
        server_url, short_name, description = server
        insert_server(conn, server_url, short_name, description)
        # Pass the server_url down into crawl so all resources can be linked back.
        crawl(server_url, conn, server_url)

    conn.close()
    print("Crawling complete. Data saved to arcgis_metadata.db")

if __name__ == "__main__":
    main()
