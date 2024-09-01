from mage_ai.io.config import ConfigFileLoader
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.s3 import S3

from zipfile import ZipFile
from datetime import datetime
from io import BytesIO
from os import path, getenv
import requests
import pandas as pd
import gc
import logging

# Configure logging for the module.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define constants for URL and file handling.
BASE_URL = 'https://dadosabertos.rfb.gov.br/CNPJ/'
RF_FILE = 'Estabelecimentos'
FILE_EXT = ".zip"
ENCODING = "ISO-8859-1"
CHUNKSIZE = 1500000
BUCKET_NAME = getenv('BUCKET_NAME', 'your-data-lake')
CONFIG_PATH = path.join(get_repo_path(), 'io_config.yaml')
CONFIG_PROFILE = 'default'
REQUEST_TIMEOUT = 10
MAX_RETRIES = 3

# Define the column names and data types for the dataframe.
COLUMNS = [
    "cnpj", "cnpj_dv", "identificador", "nome_fantasia", "situacao", 
    "data_situacao", "motivo_situacao", "nome_cidade_exterior", "pais", 
    "data_inicio", "cnae_principal", "cnae_secundario", "tipo_logradouro", 
    "logradouro", "numero", "complemento", "bairro", "cep", "uf", 
    "municipio", "ddd1", "telefone1", "ddd2", "telefone2", "ddd_fax", 
    "fax", "email", "situacao_especial", "data_situacao_especial"
]

DTYPES = {
    "cnpj": "string", "cnpj_dv": "string", "identificador": "string", 
    "nome_fantasia": "string", "situacao": "string", "data_situacao": "string", 
    "motivo_situacao": "string", "nome_cidade_exterior": "string", 
    "pais": "string", "data_inicio": "string", "cnae_principal": "string", 
    "cnae_secundario": "string", "tipo_logradouro": "string", "logradouro": "string", 
    "numero": "string", "complemento": "string", "bairro": "string", 
    "cep": "string", "uf": "string", "municipio": "string", 
    "ddd1": "string", "telefone1": "string", "ddd2": "string", 
    "telefone2": "string", "ddd_fax": "string", "fax": "string", 
    "email": "string", "situacao_especial": "string", 
    "data_situacao_especial": "string"
}

def download_and_extract_file(url: str, retries: int = MAX_RETRIES) -> BytesIO:
    """
    Download a ZIP file from a URL and extract its content.

    Args:
        url (str): The URL to download the ZIP file from.
        retries (int): Number of retries in case of failure.

    Returns:
        BytesIO: A file-like object containing the extracted file.
    """
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            zipfile = ZipFile(BytesIO(response.content))
            return zipfile.open(zipfile.namelist()[0])
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt == retries - 1:
                logger.error("All download attempts failed.")
                raise
            continue

def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df['nome_fantasia'] = df['nome_fantasia'].fillna("").str.replace("'", "")
    df['cnae_secundario'] = df['cnae_secundario'].str.replace("'", "")
    return df

def export_to_s3(df: pd.DataFrame, bucket_name: str, object_key: str) -> None:
    S3.with_config(ConfigFileLoader(CONFIG_PATH, CONFIG_PROFILE)).export(
        df, bucket_name, object_key, "csv"
    )

def process_and_export_chunk(csv_file, part_num: int) -> None:
    for df in pd.read_csv(
        csv_file, encoding=ENCODING, delimiter=";", names=COLUMNS, 
        chunksize=CHUNKSIZE, dtype=DTYPES, index_col=0, iterator=True
    ):
        df = process_dataframe(df)
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        object_key = f'receita-federal/estabelecimentos/estabelecimentos_{timestamp}.csv'
        export_to_s3(df, BUCKET_NAME, object_key)
        del df
        gc.collect()
    logger.info(f"Successfully processed part {part_num}")

@data_loader
def load_data(*args, **kwargs) -> None:
    try:
        for i in range(10):
            url = f"{BASE_URL}{RF_FILE}{i}{FILE_EXT}"
            csv_file = download_and_extract_file(url)
            process_and_export_chunk(csv_file, i)
    
    except Exception as e:
        logger.error(f"Failed to load data: {str(e)}")
        raise

@test
def test_output(output=None, *args) -> None:
    if output is None:
        logger.warning("No output provided for testing.")
    else:
        logger.info("Output test passed.")
