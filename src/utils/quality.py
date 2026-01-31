from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def sanitize_columns(df: DataFrame) -> DataFrame:
    """
    Padroniza nomes de colunas: minúsculas, sem espaços nas pontas,
    e substitui espaços internos por underscores.
    """
    new_columns = [
        F.col(c).alias(c.strip().lower().replace(" ", "_")) 
        for c in df.columns
    ]
    return df.select(*new_columns)