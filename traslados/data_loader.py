"""
Carga de datos de configuración para el motor de traslados

Solo carga archivos auxiliares de configuración (tiendas, tiempos).
Los datos de ventas/stock se reciben como DataFrames en memoria.
"""
import pandas as pd
from pathlib import Path
from typing import Optional, Tuple, Dict
import logging

from core.normalization import normalize_store_name

logger = logging.getLogger(__name__)


def load_tiendas(path: Optional[Path]) -> Tuple[Dict, Optional[pd.DataFrame]]:
    """
    Carga clasificación de tiendas desde CSV
    
    Archivo esperado (CSV delimitado por ;):
        TIENDA;TIPO;REGION;REGION ID
        CALI CHIPICHAPE;B;VALLE;4
        BARRANQUILLA UNICO;A;ATLANTICO;1
        ...
    
    Args:
        path: Ruta al CSV de clasificación de tiendas (opcional)
    
    Returns:
        tuple: (tiendas_map dict, DataFrame) o ({}, None) si no hay archivo
        
        tiendas_map estructura:
        {
            'CALI CHIPICHAPE': {
                'Tipo': 'B',
                'Region': 'VALLE',
                'RegionID': 4
            },
            ...
        }
    """
    if not path:
        logger.debug("No se proporcionó archivo de clasificación de tiendas")
        return {}, None
    
    if not Path(path).exists():
        logger.warning(f"Archivo de tiendas no encontrado: {path}")
        return {}, None
    
    try:
        # Leer CSV con delimitador ;
        df = pd.read_csv(path, sep=';', encoding='utf-8')
        
        # Validar columnas esperadas
        required_cols = ['TIENDA']
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            logger.error(f"Columnas faltantes en clasificación tiendas: {missing}")
            return {}, None
        
        # Normalizar nombres de columnas
        df.columns = df.columns.str.strip().str.upper()
        
        # Renombrar para consistencia
        col_map = {
            'TIENDA': 'Tienda',
            'TIPO': 'Tipo',
            'REGION': 'Region',
            'REGION ID': 'RegionID'
        }
        df = df.rename(columns=col_map)
        
        # Normalizar valores
        df['Tienda'] = df['Tienda'].apply(normalize_store_name)
        
        if 'Tipo' in df.columns:
            df['Tipo'] = df['Tipo'].astype(str).str.strip().str.upper()
        
        if 'Region' in df.columns:
            df['Region'] = df['Region'].astype(str).str.strip().str.upper()
        
        if 'RegionID' in df.columns:
            df['RegionID'] = pd.to_numeric(df['RegionID'], errors='coerce').astype('Int64')
        
        # Crear diccionario de mapeo
        tiendas_map = df.set_index('Tienda').to_dict(orient='index')
        
        logger.info(f"Cargadas {len(tiendas_map)} tiendas desde {path}")
        
        return tiendas_map, df
        
    except Exception as e:
        logger.error(f"Error cargando clasificación de tiendas: {e}", exc_info=True)
        return {}, None


def load_tiempos(path: Optional[Path]) -> pd.DataFrame:
    """
    Carga tiempos de entrega desde CSV
    
    Archivo esperado (CSV delimitado por ;):
        ORIGEN-DESTINO;DESTINO-ORIGEN;ETA;PRIORIDAD
        CALI CHIPICHAPE;CALI UNICENTRO;1 dia;1
        BODEGA PRINCIPAL;CALI CHIPICHAPE;2 dias;2
        ...
    
    Args:
        path: Ruta al CSV de tiempos de entrega (opcional)
    
    Returns:
        DataFrame con columnas:
            - _O: Tienda origen (normalizada)
            - _D: Tienda destino (normalizada)
            - _ETA_NUM: Días de entrega (float)
            - _PRI_NUM: Prioridad (float)
    """
    if not path:
        logger.debug("No se proporcionó archivo de tiempos de entrega")
        return pd.DataFrame()
    
    if not Path(path).exists():
        logger.warning(f"Archivo de tiempos no encontrado: {path}")
        return pd.DataFrame()
    
    try:
        # Leer CSV con delimitador ;
        df = pd.read_csv(path, sep=';', encoding='utf-8')
        
        if df.empty:
            logger.warning("Archivo de tiempos está vacío")
            return pd.DataFrame()
        
        # Normalizar nombres de columnas
        df.columns = df.columns.str.strip().str.upper()
        
        # Validar columnas mínimas
        if 'ORIGEN-DESTINO' not in df.columns or 'DESTINO-ORIGEN' not in df.columns:
            logger.error("Columnas de origen/destino faltantes")
            return pd.DataFrame()
        
        # Renombrar
        col_map = {
            'ORIGEN-DESTINO': '_O',
            'DESTINO-ORIGEN': '_D',
            'ETA': '_ETA_RAW',
            'PRIORIDAD': '_PRI_NUM'
        }
        df = df.rename(columns=col_map)
        
        # Normalizar tiendas
        df['_O'] = df['_O'].astype(str).str.strip().str.upper()
        df['_D'] = df['_D'].astype(str).str.strip().str.upper()
        
        # Parsear ETA (extraer números de strings como "2 dias", "1 día")
        if '_ETA_RAW' in df.columns:
            df['_ETA_NUM'] = df['_ETA_RAW'].apply(_parse_lead_time_value)
        else:
            df['_ETA_NUM'] = None
        
        # Parsear prioridad
        if '_PRI_NUM' in df.columns:
            df['_PRI_NUM'] = pd.to_numeric(df['_PRI_NUM'], errors='coerce')
        else:
            df['_PRI_NUM'] = None
        
        # Mantener solo columnas relevantes
        df = df[['_O', '_D', '_ETA_NUM', '_PRI_NUM']].copy()
        
        logger.info(f"Cargados {len(df)} registros de tiempos de entrega desde {path}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error cargando tiempos de entrega: {e}", exc_info=True)
        return pd.DataFrame()


def _parse_lead_time_value(x) -> float:
    """
    Extrae número de días de strings como "2 dias", "1 día", "3"
    
    Examples:
        >>> _parse_lead_time_value("2 dias")
        2.0
        >>> _parse_lead_time_value("1 día")
        1.0
        >>> _parse_lead_time_value("3")
        3.0
    """
    if pd.isna(x):
        return float('nan')
    
    import re
    s = str(x).strip().upper()
    
    # Extraer todos los números
    nums = [int(n) for n in re.findall(r'\d+', s)]
    
    if not nums:
        return float('nan')
    
    # Retornar el máximo (por si hay algo como "1-2 días")
    return float(max(nums))


def prepare_auxiliary_data(tiendas_path: Optional[Path] = None,
                          tiempos_path: Optional[Path] = None) -> Tuple[Dict, Optional[pd.DataFrame], pd.DataFrame]:
    """
    Carga datos auxiliares de configuración
    
    Args:
        tiendas_path: Ruta a Clasificacion_Tiendas.csv
        tiempos_path: Ruta a Tiempos de entrega.csv
    
    Returns:
        tuple: (tiendas_map, tiendas_df, tiempos_df)
    """
    logger.info("Cargando datos auxiliares de configuración...")
    
    tiendas_map, tiendas_df = load_tiendas(tiendas_path)
    tiempos_df = load_tiempos(tiempos_path)
    
    return tiendas_map, tiendas_df, tiempos_df