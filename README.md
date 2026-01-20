# ES/OS Data Manager

A web-based management tool for Elasticsearch and OpenSearch clusters. Easily transfer data between clusters, import/export data, view indices, and download index data.

## Features

- **Data Transfer Between Clusters** - Seamlessly migrate data from one Elasticsearch/OpenSearch cluster to another
- **Data Import/Export** - Import data from JSON files or export index data to JSON/ZIP formats
- **View Indices** - Browse and inspect all indices across your clusters with detailed metadata
- **Download Index** - Download complete index data as JSON or compressed ZIP files

## Tech Stack

- **Backend:** Java/Kotlin
- **Frontend:** Web UI
- **Supported Systems:** Elasticsearch 7.x/8.x, OpenSearch 1.x/2.x

## Requirements

- Java 17+ (Temurin recommended)
- Elasticsearch 7.x+ or OpenSearch 1.x+
- Modern web browser (Chrome, Firefox, Safari, Edge)

## Installation

### Using JAR

```bash
# Download the latest release
wget https://github.com/yourusername/es-os-data-manager/releases/latest/download/es-os-data-manager.jar

# Run the application
java -jar es-os-data-manager.jar
Using Docker

docker pull yourusername/es-os-data-manager:latest
docker run -p 8080:8080 yourusername/es-os-data-manager:latest
Build from Source

git clone https://github.com/yourusername/es-os-data-manager.git
cd es-os-data-manager
./gradlew build
java -jar build/libs/es-os-data-manager.jar
Configuration

Create an application.yml file or set environment variables:

server:
  port: 8080

clusters:
  source:
    url: http://localhost:9200
    username: elastic
    password: changeme
  target:
    url: http://localhost:9201
    username: elastic
    password: changeme

export:
  formats:
    - json
    - zip
  max-docs-per-file: 10000
Environment Variables

Variable	Description	Default
SERVER_PORT	Application port	8080
SOURCE_CLUSTER_URL	Source cluster URL	http://localhost:9200
TARGET_CLUSTER_URL	Target cluster URL	http://localhost:9201
ES_USERNAME	Elasticsearch username	-
ES_PASSWORD	Elasticsearch password	-
Usage

1. Data Transfer Between Clusters

Transfer indices from a source cluster to a target cluster.

Navigate to Transfer tab
Select source cluster and target cluster
Choose indices to transfer
Configure transfer options (batch size, mappings, settings)
Click Start Transfer
2. Data Import/Export

Export:

Go to Export tab
Select cluster and index
Choose format (JSON or ZIP)
Apply optional query filters
Click Export
Import:

Go to Import tab
Select target cluster and index
Upload JSON file
Configure import options
Click Import
3. View Indices

Navigate to Indices tab
Select cluster from dropdown
View list of all indices with:
Index name
Document count
Storage size
Health status
Number of shards/replicas
4. Download Index

Go to Download tab
Select cluster and index
Choose download format:
JSON - Single JSON file with all documents
ZIP - Compressed archive (recommended for large indices)
Click Download
API Endpoints

Method	Endpoint	Description
GET	/api/clusters	List configured clusters
GET	/api/clusters/{id}/indices	List indices for a cluster
GET	/api/indices/{index}/stats	Get index statistics
POST	/api/transfer	Start data transfer
POST	/api/export	Export index data
POST	/api/import	Import data to index
GET	/api/download/{index}	Download index as file
Screenshots

Dashboard

Dashboard

Index Browser

Index Browser

Data Transfer

Data Transfer

Troubleshooting

Connection Issues

# Test cluster connectivity
curl -X GET "http://localhost:9200/_cluster/health"
Memory Issues

For large indices, increase JVM heap:

java -Xmx4g -jar es-os-data-manager.jar
SSL/TLS Connections

clusters:
  source:
    url: https://localhost:9200
    ssl:
      verify: true
      truststore: /path/to/truststore.jks
Contributing

Fork the repository
Create a feature branch (git checkout -b feature/amazing-feature)
Commit your changes (git commit -m 'Add amazing feature')
Push to the branch (git push origin feature/amazing-feature)
Open a Pull Request
License

This project is licensed under the MIT License - see the LICENSE file for details.

Support

Create an Issue for bug reports
Start a Discussion for questions
Acknowledgments

Elasticsearch
OpenSearch

---

**Copy the content above** and save it as `README.md` in your project root.

Would you like me to:
1. Add more sections (e.g., Architecture, Performance Tips)?
2. Modify any existing sections?
3. Add badges (build status, version, license)?
