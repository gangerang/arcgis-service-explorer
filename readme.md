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
   Create a file named `servers.txt` and list the ArcGIS REST endpoints you want to crawl, one per line. For example:

   ```
   https://sampleserver6.arcgisonline.com/arcgis/rest/services
   ```

2. **Run the Script:**

   ```bash
   python main.py
   ```

3. **Database Output:**  
   The script creates a SQLite database file named `arcgis_metadata.db` with two main tables:
   - **resources:** Stores metadata for each resource (folders, services, layers, and tables).
   - **fields:** Stores detailed field information for layers and tables.

## Contributing

Contributions are welcome! If you have suggestions, bug reports, or improvements, please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [ArcGIS REST API Documentation](https://developers.arcgis.com/rest/)
- [Requests Library Documentation](https://docs.python-requests.org/)
- [SQLite3 Documentation](https://docs.python.org/3/library/sqlite3.html)

