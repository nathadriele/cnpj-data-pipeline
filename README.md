## CNPJ Data ETL Pipeline

### Overview
The CNPJ Data ETL Pipeline is an automated data pipeline designed to handle the extraction, transformation, and loading (ETL) of data related to commercial establishments registered with the Brazilian Federal Revenue (RFB). This project facilitates the efficient management and analysis of large datasets, making them accessible for various data engineering applications.

### Problem Description
Brazil’s commercial establishment data, registered with the Federal Revenue, is essential for economic analysis, compliance, and big data applications. However, the data is spread across multiple files, encoded in different formats, and contains inconsistencies, making the ingestion and processing of these datasets challenging. The complexity and volume of the data necessitate an automated, scalable, and reliable pipeline.

### Objective
The pipeline's primary objective is to automate the download, processing, and storage of establishment data from the Federal Revenue. The process includes file decompression, data cleaning, chunk processing for memory optimization, and exporting the cleaned data to an AWS S3 bucket. The pipeline is designed to be scalable, efficient, and reproducible, ensuring seamless integration with other data engineering systems.

### Prerequisites
- `Python`: Version 3.8 or higher.
- `Mage.ai`: Data pipeline management tool.
- `AWS CLI`: Configured with appropriate credentials for accessing S3.
- `Python Libraries`: Pandas, Requests, Boto3, and other dependencies listed in `requirements.txt`.
- `S3 Bucket`: Access to an AWS S3 bucket for storing the processed data.

### Installation
1. Clone this repository:

```py
git clone https://github.com/nathadriele/cnpj-data-pipeline.git
cd cnpj_data_etl_pipeline
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

```py
- cnpj_data_etl_pipeline/: Root directory containing all essential components of the ETL pipeline.
     - data_loaders/: Contains the main script responsible for ETL operations.
          - cnpj_data_pipeline_script.py: Script managing all stages of the pipeline, from downloading data from the Federal Revenue to uploading it to AWS S3, including chunked data processing for memory optimization.
- pipelines/: Contains pipeline configurations that define how and when the pipeline should run.
     - metadata.yaml: Defines execution blocks, execution configuration, and pipeline dependencies.
     - triggers.yaml: Defines the scheduling and triggers that control the automated pipeline execution, such as execution intervals and triggering conditions.
- io_config.yaml: Configuration file that defines AWS credentials and the S3 bucket where processed data will be stored.
- requirements.txt: Lists all the libraries and dependencies needed to run the project, allowing easy installation via pip install -r requirements.txt.
```

### How the Script Works
- `Download`: The script downloads the compressed files from the Federal Revenue in a loop that iterates over the different establishment files (Estabelecimentos 0 to 9).
- `Decompression and Reading`: Each .zip file is decompressed and read in chunks to avoid excessive memory consumption.
- `Processing`: The resulting DataFrame is cleaned of inconsistencies, such as unwanted apostrophes and null values in critical fields.
- `Export to S3`: The processed data is exported in CSV format to an S3 bucket, organized with a directory structure based on processing date and time.
- `Logging and Error Handling`: The script includes detailed logging and error handling to ensure that failures are properly recorded and the process continues, minimizing disruptions.

### Best Practices
- `Modularity`: Functions are separated by responsibility, making the code easier to maintain and scale.
- `Efficiency`: The use of chunks in processing CSV files ensures efficient memory usage, making the pipeline suitable for large volumes of data.
- `Security`: Access to AWS resources is managed via external configuration, keeping credentials out of the source code.
- `Error Handling`: The pipeline includes robust error handling mechanisms and detailed logging, ensuring that failures do not interrupt the global process.
- `Reproducibility`: The pipeline is designed to be easily reproducible in other environments, facilitating its implementation in different projects and contexts.

### Contribution to Data Engineering
This project demonstrates the importance of automated data pipelines in handling and processing large public datasets efficiently. It highlights best practices in error handling, logging, and secure handling of configuration files and credentials.
