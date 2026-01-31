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
URL_MCO = "https://dados.pbh.gov.br/dataset/mapa-de-controle-operacional-mco-consolidado-a-partir-de-abril-de-2022/resource/123b7a8a-ceb1-4f8c-9ec6-9ce76cdf9aab/download/mco_consolidado.csv"
URL_GPS = "https://dados.pbh.gov.br/datastore/dump/88101fac-7200-4476-8c4f-09e1663c435e?format=json"