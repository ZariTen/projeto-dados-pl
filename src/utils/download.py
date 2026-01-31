import os
import requests
from fake_useragent import UserAgent
from src.config import LANDING_ZONE

def download_file(url: str, local_filename: str) -> str:
    """
    Baixa um arquivo da URL e salva na Landing Zone temporária.
    
    Args:
        url (str): Endereço do arquivo.
        local_filename (str): Nome do arquivo no disco.

    Returns:
        str: Caminho completo do arquivo salvo.
    """
    os.makedirs(LANDING_ZONE, exist_ok=True)
    local_path = os.path.join(LANDING_ZONE, local_filename)

    if os.path.exists(local_path):
        return local_path
    
    ua = UserAgent()
    headers = {'User-Agent': ua.random}

    try:
        with requests.get(url, headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return local_path
    except Exception as e:
        print(f"Erro ao baixar o arquivo: {e}")
        raise e