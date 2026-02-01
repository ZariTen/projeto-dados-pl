import os

# Caminhos Base
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Caminhos do Data Lake
LANDING_ZONE = os.path.join(DATA_DIR, "landing_temp")
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
SILVER_DIR = os.path.join(DATA_DIR, "silver")
GOLD_DIR = os.path.join(DATA_DIR, "gold")

# URLs dos Datasets (PBH)
URL_MCO = "https://ckan.pbh.gov.br/dataset/da1f99d3-3dd6-4bdb-8ca4-91f94da93eff/resource/eebe35cb-aad8-4289-9d28-8a69f51ff484/download/mco-07-2020.csv"
URL_GPS = "https://dados.pbh.gov.br/datastore/dump/88101fac-7200-4476-8c4f-09e1663c435e?bom=True"
URL_LINHAS = "https://dados.pbh.gov.br/datastore/dump/150bddd0-9a2c-4731-ade9-54aa56717fb6?bom=True"