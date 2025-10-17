"""
Funciones de normalización y limpieza de datos
CRÍTICO: Elimina padding/espacios en blanco que vienen de SQL Server
"""
import pandas as pd
import re
from typing import List

def strip_all_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina espacios en inicio/fin de TODAS las columnas de texto.
    Esto es crítico porque SQL Server retorna campos CHAR con padding.
    
    Ejemplos:
        "023  " → "023"
        "1484612                    " → "1484612"
        "VESTIDO BODY               " → "VESTIDO BODY"
    """
    df = df.copy()
    
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype('string').str.strip()
    
    return df

def strip_specific_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Elimina espacios solo en columnas específicas.
    Útil cuando no quieres tocar todas las columnas.
    """
    df = df.copy()
    
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype('string').str.strip()
    
    return df

def clean_control_chars(series: pd.Series) -> pd.Series:
    """
    Elimina caracteres de control ASCII (0x00-0x1F, 0x7F)
    que pueden aparecer en bases de datos legacy.
    """
    return series.str.replace(r"[\x00-\x1F\x7F]", "", regex=True)

def clean_referencia(series: pd.Series) -> pd.Series:
    """
    Limpieza completa de Referencia:
    1. Strip espacios
    2. Eliminar caracteres de control
    """
    return (series
            .astype('string')
            .str.strip()
            .pipe(clean_control_chars))

def normalize_talla(series: pd.Series) -> pd.Series:
    """
    Normalización de Talla:
    1. Strip espacios
    2. Upper case
    3. Rellenar vacíos con string vacío
    
    Ejemplos:
        "18m                 " → "18M"
        None → ""
        "  12M  " → "12M"
    """
    return (series
            .astype('string')
            .str.strip()
            .str.upper()
            .fillna(''))

def build_sku(df: pd.DataFrame, 
              ref_col: str = 'Referencia',
              talla_col: str = 'Talla',
              sku_col: str = 'SKU') -> pd.DataFrame:
    """
    Construye SKU = Referencia + Talla
    
    Prerequisitos:
    - Referencia debe estar limpia (sin padding)
    - Talla debe estar normalizada (upper, sin padding)
    
    Ejemplos:
        Ref="1484612", Talla="18M" → SKU="148461218M"
        Ref="BOLSA", Talla="" → SKU="BOLSA"
    """
    df = df.copy()
    
    df[sku_col] = (df[ref_col].fillna('').astype(str) + 
                   df[talla_col].fillna('').astype(str))
    
    return df

def normalize_store_name(x):
    """
    Normalización de nombres de tienda
    Funciona tanto con Series como con strings individuales
    """
    if isinstance(x, pd.Series):
        # Si es una Series completa
        return (x
                .astype('string')
                .str.strip()
                .str.upper())
    else:
        # Si es un string individual
        return str(x).strip().upper() if pd.notna(x) else x

"""
def normalize_store_name(series: pd.Series) -> pd.Series:
    return (series
            .astype('string')
            .str.strip()
            .str.upper())
"""