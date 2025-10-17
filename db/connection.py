"""
Manejo de conexiones a SQL Server
"""
import pyodbc
import pandas as pd
from typing import Optional, Dict, Any
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

class DatabaseConnection:
    """Administrador de conexiones a SQL Server"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self._connection: Optional[pyodbc.Connection] = None
    
    def connect(self) -> pyodbc.Connection:
        """Establece conexión si no existe"""
        if self._connection is None:
            logger.info("Estableciendo conexión a SQL Server")
            try:
                self._connection = pyodbc.connect(
                    self.connection_string,
                    timeout=30,
                    autocommit=False
                )
                logger.info("Conexión establecida exitosamente")
            except pyodbc.Error as e:
                logger.error(f"Error conectando a base de datos: {e}")
                raise
        return self._connection
    
    def close(self):
        """Cierra conexión activa"""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Conexión cerrada")
    
    @contextmanager
    def cursor(self):
        """Context manager para operaciones con cursor"""
        conn = self.connect()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Error en operación: {e}")
            raise
        finally:
            cursor.close()
    
    def execute_query(self, 
                     query: str, 
                     params: Optional[Dict[str, Any]] = None,
                     chunksize: Optional[int] = None) -> pd.DataFrame:
        """
        Ejecuta query y retorna DataFrame
        
        Args:
            query: SQL query a ejecutar
            params: Parámetros para query parametrizada
            chunksize: Tamaño de chunk para queries grandes (None = todo en memoria)
        
        Returns:
            DataFrame con resultados
        """
        logger.info(f"Ejecutando query (primeros 150 chars):\n{query[:150]}...")
        
        try:
            conn = self.connect()
            
            if chunksize:
                # Para queries muy grandes, procesar por chunks
                chunks = []
                for chunk in pd.read_sql(query, conn, params=params, chunksize=chunksize):
                    chunks.append(chunk)
                df = pd.concat(chunks, ignore_index=True)
            else:
                df = pd.read_sql(query, conn, params=params)
            
            logger.info(f"Query retornó {len(df):,} filas × {len(df.columns)} columnas")
            return df
            
        except Exception as e:
            logger.error(f"Error ejecutando query: {e}")
            raise
    
    def __enter__(self):
        """Permite usar como context manager"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cierra conexión al salir del contexto"""
        self.close()
