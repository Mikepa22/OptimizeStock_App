"""
Procesador de datos de ventas
Transforma datos crudos de SQL a formato compatible con Basecompleta.py
"""
import pandas as pd
from typing import Optional
import logging

from core.normalization import (
    strip_all_string_columns,
    clean_referencia,
    normalize_talla,
    build_sku
)

logger = logging.getLogger(__name__)

class VentasProcessor:
    """
    Pipeline de transformación para datos de ventas.
    
    Entrada esperada (de vista MP_VENTAS_CODE):
        - C.O., Fecha, Estado, Bodega, Descripcion C.O., Referencia, 
          Desc. item, Talla, Cantidad inv., Valor neto, RANGO, 
          CLASIFICACION, Fuente
    
    Salida (compatible con Basecompleta.py):
        - Igual pero con: padding eliminado, tipos correctos, SKU construido,
          columnas renombradas, filtros aplicados
    """
    
    def __init__(self, debug: bool = False):
        self.debug = debug
        self._log_step = logger.info if debug else logger.debug
    
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Pipeline completo de transformaciones
        
        Args:
            df: DataFrame crudo desde SQL
        
        Returns:
            DataFrame procesado y limpio
        """
        if df.empty:
            logger.warning("DataFrame de entrada está vacío")
            return df
        
        self._log_step(f"[1/10] Inicio: {len(df):,} filas")
        
        # 1. CRÍTICO: Eliminar padding de TODAS las columnas de texto
        df = self._strip_all_text(df)
        
        # 2. Filtrar solo PRENDAS
        df = self._filter_clasificacion(df)
        
        # 3. Limpiar Referencia (caracteres de control)
        df = self._clean_referencia(df)
        
        # 4. Normalizar Talla (upper + strip)
        df = self._normalize_talla(df)
        
        # 5. Construir SKU
        df = self._build_sku(df)
        
        # 6. Convertir tipos de datos
        df = self._convert_types(df)
        
        # 7. Renombrar columnas para compatibilidad
        df = self._rename_columns(df)
        
        # 8. Aplicar filtros de negocio
        df = self._apply_business_filters(df)
        
        # 9. Aplicar reemplazos especiales
        df = self._apply_replacements(df)
        
        # 10. Reordenar columnas
        df = self._reorder_columns(df)
        
        self._log_step(f"[10/10] Final: {len(df):,} filas procesadas")
        
        return df
    
    def _strip_all_text(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 1: Eliminar padding de SQL Server
        
        CRÍTICO: SQL Server retorna campos CHAR con espacios al final:
            "023  " → "023"
            "1484612                          " → "1484612"
            "18M                 " → "18M"
        """
        self._log_step("[1/10] Eliminando padding de columnas de texto")
        
        df = strip_all_string_columns(df)
        
        if self.debug:
            sample_cols = ['Bodega', 'Referencia', 'Talla']
            for col in sample_cols:
                if col in df.columns:
                    sample = df[col].iloc[0] if len(df) > 0 else None
                    logger.debug(f"  {col} sample: '{sample}'")
        
        return df
    
    def _filter_clasificacion(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 2: Filtrar solo CLASIFICACION = 'PRENDAS'
        """
        self._log_step("[2/10] Filtrando CLASIFICACION='PRENDAS'")
        
        before = len(df)
        df = df[df['CLASIFICACION'] == 'PRENDAS'].copy()
        after = len(df)
        
        self._log_step(f"  Filtrado: {after:,}/{before:,} filas ({after/before*100:.1f}%)")
        
        return df
    
    def _clean_referencia(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 3: Limpiar Referencia (caracteres de control ASCII)
        """
        self._log_step("[3/10] Limpiando Referencia")
        
        if 'Referencia' in df.columns:
            df['Referencia'] = clean_referencia(df['Referencia'])
        
        return df
    
    def _normalize_talla(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 4: Normalizar Talla (strip + upper)
        """
        self._log_step("[4/10] Normalizando Talla")
        
        if 'Talla' in df.columns:
            df['Talla'] = normalize_talla(df['Talla'])
        
        return df
    
    def _build_sku(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 5: Construir SKU = Referencia + Talla
        """
        self._log_step("[5/10] Construyendo SKU")
        
        df = build_sku(df, ref_col='Referencia', talla_col='Talla', sku_col='SKU')
        
        if self.debug and len(df) > 0:
            sample = df[['Referencia', 'Talla', 'SKU']].iloc[0]
            logger.debug(f"  Ejemplo SKU: {sample['Referencia']} + {sample['Talla']} = {sample['SKU']}")
        
        return df
    
    def _convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 6: Conversión de tipos de datos
        """
        self._log_step("[6/10] Convirtiendo tipos de datos")
        
        # Fecha: datetime → date (sin hora)
        if 'Fecha' in df.columns:
            df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce').dt.date
        
        # Valor neto: decimal → Int64 (entero nullable)
        if 'Valor neto' in df.columns:
            df['Valor neto'] = (pd.to_numeric(df['Valor neto'], errors='coerce')
                               .round(0)
                               .astype('Int64'))
        
        # Cantidad inv.: asegurar float
        if 'Cantidad inv.' in df.columns:
            df['Cantidad inv.'] = pd.to_numeric(df['Cantidad inv.'], errors='coerce')

        if 'Descripcion C.O.' in df.columns:
            df['IsEcom'] = df['Descripcion C.O.'].str.contains(
                'ECOM|ECO|ONLINE|VIRTUAL|WEB|PRINCIPAL', 
                case=False, 
                na=False
            )
        elif 'Desc. C.O.' in df.columns:
            df['IsEcom'] = df['Desc. C.O.'].str.contains(
                'ECOM|ECO|ONLINE|VIRTUAL|WEB|PRINCIPAL',
                case=False,
                na=False
            )
        else:
            # Si no hay columna de tienda, asumir False
            df['IsEcom'] = False
            
        return df
        
    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 7: Renombrar columnas para compatibilidad con Basecompleta.py
        """
        self._log_step("[7/10] Renombrando columnas")
        
        # Descripcion C.O. → Desc. C.O.
        if 'Descripcion C.O.' in df.columns:
            df = df.rename(columns={'Descripcion C.O.': 'Desc. C.O.'})
        
        return df
    
    def _apply_business_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 8: Filtros de negocio
        """
        self._log_step("[8/10] Aplicando filtros de negocio")
        
        before = len(df)
        
        # Filtro 1: Referencias que NO empiezan con 'N'
        df = df[~df['Referencia'].str.startswith('N', na=False)].copy()
        after_n = len(df)
        
        # Filtro 2: Referencias que NO contienen 'PROMO'
        df = df[~df['Referencia'].str.contains('PROMO', na=False, case=False)].copy()
        after_promo = len(df)
        
        self._log_step(f"  Filtro 'N': {after_n:,}/{before:,} filas")
        self._log_step(f"  Filtro 'PROMO': {after_promo:,}/{after_n:,} filas")
        
        return df
    
    def _apply_replacements(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 9: Reemplazos especiales
        """
        self._log_step("[9/10] Aplicando reemplazos")
        
        # PRINCIPAL → ECOMMERCE en Desc. C.O.
        if 'Desc. C.O.' in df.columns:
            count = (df['Desc. C.O.'] == 'PRINCIPAL').sum()
            df['Desc. C.O.'] = df['Desc. C.O.'].str.replace(
                'PRINCIPAL', 'ECOMMERCE', regex=False
            )
            if count > 0:
                self._log_step(f"  Reemplazados {count} 'PRINCIPAL' → 'ECOMMERCE'")
        
        return df
    
    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 10: Reordenar columnas (compatibilidad con Basecompleta.py)
        """
        self._log_step("[10/10] Reordenando columnas")
        
        # Orden deseado (columnas principales al frente)
        desired_order = [
            "C.O.", "Bodega", "Desc. C.O.", "Fecha", "Referencia", 
            "Desc. item", "Talla", "Cantidad inv.", "Valor neto", 
            "RANGO", "SKU", "IsEcom"
        ]
        
        # Columnas que existen en el orden deseado
        front = [c for c in desired_order if c in df.columns]
        
        # Resto de columnas no especificadas
        rest = [c for c in df.columns if c not in front]
        
        return df[front + rest]
    
    def filter_by_selection(self, 
                           df: pd.DataFrame,
                           selection_df: pd.DataFrame) -> pd.DataFrame:
        """
        Filtro opcional por selección de referencias
        
        Args:
            df: DataFrame procesado
            selection_df: DataFrame con columna 'Referencias' o 'Referencia'
        
        Returns:
            DataFrame filtrado
        """
        logger.info("Aplicando filtro de selección")
        
        # Detectar columna de referencias en selección
        col_sel = None
        if 'Referencias' in selection_df.columns:
            col_sel = 'Referencias'
        elif 'Referencia' in selection_df.columns:
            col_sel = 'Referencia'
        else:
            logger.warning("DataFrame de selección no tiene columna 'Referencias' o 'Referencia'")
            return df
        
        # Extraer referencias válidas
        refs = (selection_df[col_sel]
                .astype('string')
                .str.strip()
                .pipe(clean_referencia))
        refs = refs[refs.notna() & (refs != '')]
        
        if len(refs) == 0:
            logger.warning("Selección está vacía; no se filtra")
            return df
        
        # Filtrar
        before = len(df)
        df_filtered = df[df['Referencia'].isin(refs.unique())].copy()
        after = len(df_filtered)
        
        logger.info(f"Filtro de selección: {after:,}/{before:,} filas ({after/before*100:.1f}%)")
        
        return df_filtered
