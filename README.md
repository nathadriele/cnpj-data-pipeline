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
- Pandas, Requests, and other dependencies listed in "requirements.txt"
- Permissions to access and write to the S3 bucket
