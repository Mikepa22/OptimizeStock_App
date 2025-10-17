"""
Cálculo de ADU (Average Daily Units) - Velocidad de venta

Este módulo calcula la velocidad de venta promedio por SKU/Tienda
a partir de datos de ventas ya procesados.
"""
import pandas as pd
import numpy as np
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def calculate_adu_from_ventas(df_ventas: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula ADU (Average Daily Units) por Tienda y SKU
    
    ADU = Promedio de unidades vendidas por día
    
    Args:
        df_ventas: DataFrame de ventas procesadas con columnas:
            - Tienda (o 'Desc. C.O.')
            - SKU
            - Cantidad inv. (unidades vendidas)
            - Fecha (opcional - para cálculo preciso)
    
    Returns:
        DataFrame con columnas:
            - Tienda: Nombre normalizado de tienda
            - SKU: Código SKU
            - ADU: Velocidad promedio diaria
    
    Examples:
        Si SKU "148461218M" vendió 60 unidades en CALI CHIPICHAPE 
        durante 30 días → ADU = 2.0 unidades/día
    """
    logger.info("Calculando velocidad de venta (ADU)...")
    
    # Validar columnas requeridas
    col_sku = _detect_column(df_ventas, ['SKU', 'sku'])
    col_qty = _detect_column(df_ventas, ['Cantidad inv.', 'cantidad inv.', 'Cantidad', 'Unidades'])
    col_tienda = _detect_column(df_ventas, ['Desc. C.O.', 'desc. c.o.', 'Tienda', 'Bodega'])
    
    if not all([col_sku, col_qty, col_tienda]):
        raise ValueError(
            f"Columnas requeridas faltantes. "
            f"Encontradas: SKU={col_sku}, Cantidad={col_qty}, Tienda={col_tienda}"
        )
    
    # Detectar columna de fecha (opcional)
    col_fecha = _detect_column(df_ventas, ['Fecha', 'fecha', 'F. Documento'])
    
    # Seleccionar columnas necesarias
    cols = [col_sku, col_qty, col_tienda]
    if col_fecha:
        cols.append(col_fecha)
    
    df = df_ventas[cols].copy()
    
    # Renombrar para consistencia
    rename_map = {
        col_sku: 'SKU',
        col_qty: 'Unidades',
        col_tienda: 'Tienda'
    }
    if col_fecha:
        rename_map[col_fecha] = 'Fecha'
    
    df = df.rename(columns=rename_map)
    
    # Limpiar datos
    df['Unidades'] = pd.to_numeric(df['Unidades'], errors='coerce').fillna(0.0)
    
    # Calcular período de días
    if 'Fecha' in df.columns:
        df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
        df = df[df['Fecha'].notna()].copy()
        
        if df.empty:
            logger.warning("No hay fechas válidas en ventas")
            dias_periodo = 30  # Default fallback
        else:
            df['Dia'] = df['Fecha'].dt.date
            dias_periodo = df['Dia'].nunique()
            
            if dias_periodo == 0:
                dias_periodo = 1
            
            logger.info(f"Período de ventas: {dias_periodo} días "
                       f"({df['Fecha'].min().date()} a {df['Fecha'].max().date()})")
    else:
        # Sin fechas, asumir período estándar
        dias_periodo = 30
        logger.warning(f"Sin columna Fecha - asumiendo {dias_periodo} días de período")
    
    # Agrupar y sumar ventas por Tienda/SKU
    adu_df = (df.groupby(['Tienda', 'SKU'], as_index=False)
              .agg(total_units=('Unidades', 'sum')))
    
    # Calcular ADU
    adu_df['ADU'] = adu_df['total_units'] / dias_periodo
    
    # Limpiar
    adu_df = adu_df[['Tienda', 'SKU', 'ADU']].copy()
    adu_df['ADU'] = adu_df['ADU'].round(4)
    
    # Estadísticas
    total_skus = adu_df['SKU'].nunique()
    skus_con_venta = (adu_df['ADU'] > 0).sum()
    
    logger.info(f"ADU calculado: {len(adu_df):,} registros")
    logger.info(f"  SKUs únicos: {total_skus:,}")
    logger.info(f"  Con ventas (ADU>0): {skus_con_venta:,}")
    logger.info(f"  ADU promedio: {adu_df['ADU'].mean():.2f}")
    
    return adu_df


def enrich_stock_with_adu(df_stock: pd.DataFrame, 
                         adu_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquece DataFrame de stock con velocidad de venta (ADU)
    
    Args:
        df_stock: DataFrame de stock procesado
        adu_df: DataFrame con ADU calculado
    
    Returns:
        DataFrame de stock con columna ADU agregada
    """
    logger.info("Enriqueciendo stock con ADU...")
    
    df = df_stock.merge(adu_df, on=['Tienda', 'SKU'], how='left')
    
    # Llenar ADU faltantes con 0 (productos sin ventas)
    df['ADU'] = df['ADU'].fillna(0.0)
    
    # Calcular cobertura en días
    df['Cobertura_dias'] = np.where(
        df['ADU'] > 0,
        df['Existencia'] / df['ADU'],
        np.inf  # Infinito para productos sin ventas
    )
    
    skus_sin_venta = (df['ADU'] == 0).sum()
    if skus_sin_venta > 0:
        logger.warning(f"{skus_sin_venta:,} registros sin ADU (productos sin ventas)")
    
    return df


def _detect_column(df: pd.DataFrame, candidates: list) -> Optional[str]:
    """
    Detecta columna por lista de nombres candidatos
    
    Args:
        df: DataFrame
        candidates: Lista de nombres posibles (case-insensitive)
    
    Returns:
        Nombre de columna encontrado o None
    """
    df_cols_lower = {c.lower(): c for c in df.columns}
    
    for candidate in candidates:
        if candidate.lower() in df_cols_lower:
            return df_cols_lower[candidate.lower()]
    
    return None


def filter_by_talla_curves(df_stock: pd.DataFrame, 
                           curvas_tallas: dict,
                           disable: bool = False) -> pd.DataFrame:
    """
    Filtra stock por curvas de tallas válidas
    
    Args:
        df_stock: DataFrame de stock
        curvas_tallas: Dict con tallas válidas por rango
            {'BEBES': ['0M','3M',...], 'NIÑOS': ['2T','3T',...]}
        disable: Si True, no aplica filtro
    
    Returns:
        DataFrame filtrado
    """
    if disable:
        logger.info("Filtro de curvas desactivado")
        return df_stock
    
    if 'RANGO_CAT' not in df_stock.columns or 'Talla' not in df_stock.columns:
        logger.warning("Columnas RANGO_CAT/Talla faltantes - no se filtra")
        return df_stock
    
    # Construir máscara de tallas válidas
    valid_tallas = set()
    for rango, tallas in curvas_tallas.items():
        valid_tallas.update(tallas)
    
    # Filtrar
    mask_bebes = ((df_stock['RANGO_CAT'] == 'BEBES') & 
                  (df_stock['Talla'].isin(curvas_tallas.get('BEBES', []))))
    
    mask_ninos = ((df_stock['RANGO_CAT'] == 'NIÑOS') & 
                  (df_stock['Talla'].isin(curvas_tallas.get('NIÑOS', []))))
    
    filtered = df_stock[mask_bebes | mask_ninos].copy()
    
    if filtered.empty:
        logger.warning("Filtro de curvas dejó 0 filas - revirtiendo a sin filtrar")
        return df_stock
    
    records_removed = len(df_stock) - len(filtered)
    logger.info(f"Filtro de curvas: {len(filtered):,} registros "
               f"({records_removed:,} removidos por tallas inválidas)")
    
    return filtered