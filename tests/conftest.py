import pytest
import sys
from pathlib import Path
from pyspark.sql import SparkSession

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture(scope="session")
def spark():
    """
    Fixture que cria uma SparkSession para os testes.
    """
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-pyspark-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    
    yield spark
    
    spark.stop()
