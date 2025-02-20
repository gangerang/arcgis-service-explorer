# ArcGIS Service Metadata Scraper

This Python script is designed to crawl one or more ESRI ArcGIS REST endpoints and extract metadata about folders, services, layers, and tables. The script recursively traverses the server structure—capturing details like fields, update information, and other available metadata—and stores everything in a SQLite database for easy searching and analysis.

## Features

- **Recursive Crawling:**  
  Traverses ArcGIS REST endpoints by exploring folders and services.
- **Service Classification:**  
  Differentiates between service types such as MapServer, FeatureServer, ImageServer, GeometryServer, and GPServer.
- **Detailed Metadata Extraction:**  
  Retrieves complete metadata (including fields) by making additional requests for layers and tables when needed.
- **Handling Sub-Layers:**  
  Processes group layers and their sub-layers, preserving their hierarchical relationship.
- **SQLite Storage:**  
  Saves all metadata in a SQLite database (`arcgis_metadata.db`) with tables for resources and fields.

## Prerequisites

- **Python 3.6+**
- **Python Libraries:**  
  - `requests`  
  - `sqlite3` (included in the Python standard library)

The script expects the ArcGIS REST endpoints to return JSON data using the parameter `f=json`.

## Installation

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/your-username/arcgis-service-scraper.git
   cd arcgis-service-scraper
   ```

2. **(Optional) Set Up a Virtual Environment:**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Required Packages:**

   ```bash
   pip install requests
   ```

## Usage

1. **Configure the Servers:**  
   Edit the `servers` list in `main.py` to include the ArcGIS REST endpoints you want to crawl. For example:

   ```python
   servers = [
       "https://www.lmbc.nsw.gov.au/arcgis/rest/services",
       "https://sampleserver6.arcgisonline.com/arcgis/rest/services"
   ]
   ```

2. **Run the Script:**

   ```bash
   python main.py
   ```

3. **Database Output:**  
   The script creates a SQLite database file named `arcgis_metadata.db` with two main tables:
   - **resources:** Stores metadata for each resource (folders, services, layers, and tables).
   - **fields:** Stores detailed field information for layers and tables.

## Troubleshooting

- **Duplicate Folder Paths:**  
  If you see duplicate folder names (e.g., `/BV/BV/`), check the logic used to remove duplicate prefixes from service names. The script attempts to adjust service names by stripping duplicate folder names before constructing URLs.

- **Missing Field Data:**  
  Ensure that full metadata is being fetched for layers and tables. The script makes an extra request to each layer/table URL to retrieve details, including the `"fields"` key.

- **Sub-Layers:**  
  The script processes each layer listed in the service metadata. In cases where a layer is a group layer (with a non-negative `"subLayerIds"` array), both the group layer and its individual sub-layers are fetched and stored. Hierarchical relationships can be inferred from the `"parentLayerId"` value in the metadata.

## Contributing

Contributions are welcome! If you have suggestions, bug reports, or improvements, please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [ArcGIS REST API Documentation](https://developers.arcgis.com/rest/)
- [Requests Library Documentation](https://docs.python-requests.org/)
- [SQLite3 Documentation](https://docs.python.org/3/library/sqlite3.html)