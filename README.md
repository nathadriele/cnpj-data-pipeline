## CNPJ Data Pipeline

### Overview
This repository contains a data pipeline designed to download, process, and store information about commercial establishments registered with the Brazilian Federal Revenue (RFB). The script is part of a larger project aimed at analyzing and integrating data on Brazilian companies, enabling the creation of a scalable and efficient data lake for use in various data engineering applications.

### Problem Description
Brazilian companies are registered with the Federal Revenue with a variety of information, including CNPJ identifiers, names, addresses, and economic activity codes (CNAE). These data are vital for economic analyses, compliance, and other big data applications. However, the volume and structure of the data, distributed across multiple files and encoded in different formats, pose significant challenges for efficient ingestion and processing.

### Objective
The objective of this pipeline is to automate the download, processing, and storage of data on establishments from the Federal Revenue. This process is carried out in several stages, including file decompression, data cleaning, chunk processing for memory optimization, and final data export to an AWS S3 bucket. This pipeline was developed to be scalable, efficient, and reproducible, facilitating integration with other data engineering systems.

### Prerequisites
- Python 3.8 or higher
- Mage.ai
- AWS CLI configured with appropriate credentials
- Pandas, Requests, and other dependencies listed in `requirements.txt`
- Permissions to access and write to the S3 bucket

### Installation

1. Clone this repository:

```py
git clone https://github.com/nathadriele/cnpj-data-pipeline.git
cd cnpj-data-pipeline
```

2. Create and activate a virtual environment:

```py
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
```

3. Install the dependencies:

```py
pip install -r requirements.txt
```

4. Configure Mage.ai:

- Follow the instructions in `Mage.ai` Documentation to set up the environment.

Update the `io_config.yaml` configuration file with your `S3` credentials and the bucket where the data will be stored.

### Code Structure

- download_and_extract_file(url: str) -> BytesIO:
    - Function responsible for downloading and extracting the .zip files from the Federal Revenue website, returning the contained CSV file.

- process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    - Cleans the data by removing unwanted characters and filling in missing values in critical columns.

- export_to_s3(df: pd.DataFrame, bucket_name: str, object_key: str) -> None:
    - Exports the processed DataFrame to the configured S3 bucket in CSV format.

### How the Script Works

- `Download`: The script downloads the compressed files from the Federal Revenue in a loop that iterates over the different establishment files (Estabelecimentos 0 to 9).
- `Decompression and Reading`: Each .zip file is decompressed and read in chunks to avoid excessive memory consumption.
- `Processing`: The resulting DataFrame is cleaned of inconsistencies, such as unwanted apostrophes and null values in critical fields.
- `Export to S3`: The processed data is exported in CSV format to an S3 bucket, organized with a directory structure based on processing date and time.
- `Logging and Error Handling`: The script includes detailed logging and error handling to ensure that failures are properly recorded and the process continues, minimizing disruptions.











