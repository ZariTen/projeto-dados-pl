import os
import logging
import requests
import time
from fake_useragent import UserAgent
from src.config import LANDING_ZONE

logger = logging.getLogger(__name__)

def download_file(url: str, local_filename: str, max_retries: int = 3, backoff_factor: float = 0.5) -> str:
    """
    Baixa um arquivo da URL com retry logic e salva na Landing Zone temporária.
    
    Args:
        url (str): Endereço do arquivo.
        local_filename (str): Nome do arquivo no disco.
        max_retries (int): Número máximo de tentativas de download. Default: 3.
        backoff_factor (float): Fator de backoff exponencial em segundos. Default: 0.5.

    Returns:
        str: Caminho completo do arquivo salvo.
        
    Raises:
        Exception: Se falhar após todas as tentativas.
    """
    os.makedirs(LANDING_ZONE, exist_ok=True)
    local_path = os.path.join(LANDING_ZONE, local_filename)

    ua = UserAgent()
    
    for attempt in range(max_retries):
        try:
            headers = {'User-Agent': ua.random}
            logger.info(f"Iniciando download: {url} (tentativa {attempt + 1}/{max_retries})")
            
            with requests.get(url, headers=headers, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(local_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            
            logger.info(f"Download concluído com sucesso: {local_path}")
            return local_path
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Erro no download (tentativa {attempt + 1}/{max_retries}): {e}")
            
            if attempt < max_retries - 1:
                wait_time = backoff_factor * (2 ** attempt)
                logger.info(f"Aguardando {wait_time}s antes de tentar novamente...")
                time.sleep(wait_time)
            else:
                logger.error(f"Falha após {max_retries} tentativas: {url}")
                raise Exception(f"Erro ao baixar o arquivo após {max_retries} tentativas: {e}")
