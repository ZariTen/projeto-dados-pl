import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from unittest.mock import patch
from src.utils.quality import (
    sanitize_columns,
    count_nulls_per_column,
    count_duplicates,
)


class TestSanitizeColumns:
    """Testes para a função sanitize_columns"""
    
    def test_sanitize_columns_lowercase(self, spark):
        """Testa se os nomes das colunas são convertidos para minúsculas"""
        data = [("John", 25), ("Jane", 30)]
        df = spark.createDataFrame(data, ["NAME", "AGE"])
        
        result = sanitize_columns(df)
        
        assert "name" in result.columns
        assert "age" in result.columns
        assert "NAME" not in result.columns
        assert "AGE" not in result.columns
    
    def test_sanitize_columns_strip_spaces(self, spark):
        """Testa se espaços nas pontas são removidos"""
        data = [("John", 25)]
        df = spark.createDataFrame(data, ["  Name  ", " Age "])
        
        result = sanitize_columns(df)
        
        assert "name" in result.columns
        assert "age" in result.columns
    
    def test_sanitize_columns_replace_internal_spaces(self, spark):
        """Testa se espaços internos são substituídos por underscores"""
        data = [("John", 25, "New York")]
        df = spark.createDataFrame(data, ["First Name", "Age", "City Name"])
        
        result = sanitize_columns(df)
        
        assert "first_name" in result.columns
        assert "age" in result.columns
        assert "city_name" in result.columns
    
    def test_sanitize_columns_combined(self, spark):
        """Testa combinação de transformações"""
        data = [("John", 25)]
        df = spark.createDataFrame(data, ["  First NAME  ", " LAST name "])
        
        result = sanitize_columns(df)
        
        assert "first_name" in result.columns
        assert "last_name" in result.columns
    
    def test_sanitize_columns_preserves_data(self, spark):
        """Testa se os dados são preservados após sanitização"""
        data = [("John", 25), ("Jane", 30)]
        df = spark.createDataFrame(data, ["NAME", "AGE"])
        
        result = sanitize_columns(df)
        result_data = result.collect()
        
        assert len(result_data) == 2
        assert result_data[0]["name"] == "John"
        assert result_data[0]["age"] == 25


class TestCountNullsPerColumn:
    """Testes para a função count_nulls_per_column"""
    
    def test_count_nulls_no_nulls(self, spark, capsys):
        """Testa contagem quando não há valores nulos"""
        data = [("John", 25), ("Jane", 30)]
        df = spark.createDataFrame(data, ["name", "age"])
        
        result = count_nulls_per_column(df, "bronze", "test_table")
        
        assert result["name"] == 0
        assert result["age"] == 0
        
        captured = capsys.readouterr()
        assert "valores nulos" not in captured.out
    
    def test_count_nulls_with_nulls(self, spark, capsys):
        """Testa contagem quando há valores nulos"""
        data = [("John", 25), (None, 30), ("Jane", None)]
        df = spark.createDataFrame(data, ["name", "age"])
        
        result = count_nulls_per_column(df, "bronze", "test_table")
        
        assert result["name"] == 1
        assert result["age"] == 1
        
        captured = capsys.readouterr()
        assert "Coluna 'name' da tabela test_table da camada bronze possui 1 valores nulos" in captured.out
        assert "Coluna 'age' da tabela test_table da camada bronze possui 1 valores nulos" in captured.out
    
    def test_count_nulls_all_nulls(self, spark, capsys):
        """Testa contagem quando todos os valores são nulos"""
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ])
        data = [(None, None), (None, None)]
        df = spark.createDataFrame(data, schema)
        
        result = count_nulls_per_column(df, "silver", "test_table")
        
        assert result["name"] == 2
        assert result["age"] == 2
    
    def test_count_nulls_batch_size(self, spark):
        """Testa contagem com diferentes tamanhos de lote"""
        data = [("a", "b", "c", "d", "e", "f")]
        df = spark.createDataFrame(data, ["col1", "col2", "col3", "col4", "col5", "col6"])
        
        result = count_nulls_per_column(df, "bronze", "test_table", batch_size=2)
        
        assert len(result) == 6
        assert all(count == 0 for count in result.values())
    
    def test_count_nulls_mixed_types(self, spark):
        """Testa contagem com diferentes tipos de dados"""
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("city", StringType(), True)
        ])
        data = [("John", 25, "NYC"), (None, None, None), ("Jane", 30, None)]
        df = spark.createDataFrame(data, schema)
        
        result = count_nulls_per_column(df, "gold", "test_table")
        
        assert result["name"] == 1
        assert result["age"] == 1
        assert result["city"] == 2


class TestCountDuplicates:
    """Testes para a função count_duplicates"""
    
    def test_count_duplicates_no_duplicates(self, spark, capsys):
        """Testa contagem quando não há duplicados"""
        data = [("John", 25), ("Jane", 30), ("Bob", 35)]
        df = spark.createDataFrame(data, ["name", "age"])
        
        count_duplicates(df, "bronze", "test_table")
        
        captured = capsys.readouterr()
        assert "duplicados" not in captured.out
    
    def test_count_duplicates_with_duplicates(self, spark, capsys):
        """Testa contagem quando há duplicados"""
        data = [("John", 25), ("Jane", 30), ("John", 25), ("John", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        
        count_duplicates(df, "bronze", "test_table")
        
        captured = capsys.readouterr()
        assert "Atenção: Existem 2 registros duplicados na tabela test_table da camada bronze" in captured.out
    
    def test_count_duplicates_all_same(self, spark, capsys):
        """Testa contagem quando todos os registros são iguais"""
        data = [("John", 25), ("John", 25), ("John", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        
        count_duplicates(df, "silver", "test_table")
        
        captured = capsys.readouterr()
        assert "Atenção: Existem 2 registros duplicados na tabela test_table da camada silver" in captured.out
    
    def test_count_duplicates_partial_duplicates(self, spark, capsys):
        """Testa contagem com duplicados parciais"""
        data = [
            ("John", 25),
            ("Jane", 30),
            ("John", 25),
            ("Bob", 35),
            ("Jane", 30)
        ]
        df = spark.createDataFrame(data, ["name", "age"])
        
        count_duplicates(df, "gold", "test_table")
        
        captured = capsys.readouterr()
        assert "Atenção: Existem 2 registros duplicados na tabela test_table da camada gold" in captured.out