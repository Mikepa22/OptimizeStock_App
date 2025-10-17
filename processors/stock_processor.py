"""
Procesador de datos de stock/inventario
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
from config.settings import (
    BODEGAS_ACTIVAS,
    REFERENCIAS_PREFIJOS_EXCLUIR,
    REFERENCIAS_PALABRAS_EXCLUIR
)

logger = logging.getLogger(__name__)

class StockProcessor:
    """
    Pipeline de transformación para datos de stock/inventario.
    
    Entrada esperada (de vista MP_T400):
        - Referencia, detalle ext. 2, Bodega, C.O. bodega, RANGO, 
          CLASIFICACION, Desc. bodega, Cant Disponible, 
          Cant Transito ent, Existencia
    
    Salida (compatible con Basecompleta.py):
        - Referencia, SKU, Talla, Existencia, Desc. bodega, RANGO, 
          CLASIFICACION (todo limpio y normalizado)
    
    IMPORTANTE: Requiere DataFrame de ventas procesadas para 
    filtrar por referencias vendidas (JOIN interno)
    """
    
    def __init__(self, debug: bool = False):
        self.debug = debug
        self._log_step = logger.info if debug else logger.debug
    
    def process(self, 
                stock_df: pd.DataFrame,
                ventas_df: pd.DataFrame) -> pd.DataFrame:
        """
        Pipeline completo de transformaciones
        
        Args:
            stock_df: DataFrame crudo desde SQL (MP_T400)
            ventas_df: DataFrame de ventas YA PROCESADO (para filtrar referencias)
        
        Returns:
            DataFrame procesado y limpio
        """
        if stock_df.empty:
            logger.warning("DataFrame de stock está vacío")
            return stock_df
        
        if ventas_df.empty:
            logger.warning("DataFrame de ventas está vacío; no se puede filtrar referencias")
            # Continuar sin filtro de referencias
        
        self._log_step(f"[1/13] Inicio: {len(stock_df):,} filas de stock")
        
        # 1. CRÍTICO: Eliminar padding de TODAS las columnas de texto
        stock_df = self._strip_all_text(stock_df)
        
        # 2. Construir SKU provisional (antes de renombrar)
        stock_df = self._build_sku_provisional(stock_df)
        
        # 3. Renombrar columnas para consistencia
        stock_df = self._rename_columns(stock_df)
        
        # 4. Filtrar referencias con prefijo excluido (N, S)
        stock_df = self._filter_excluded_prefixes(stock_df)
        
        # 5. Filtrar por lista blanca de bodegas
        stock_df = self._filter_bodegas(stock_df)
        
        # 6. Filtrar referencias con palabras excluidas (PROMO)
        stock_df = self._filter_excluded_words(stock_df)
        
        # 7. Limpiar Referencia (caracteres de control)
        stock_df = self._clean_referencia(stock_df)
        
        # 8. JOIN INTERNO con Ventas (solo referencias vendidas)
        if not ventas_df.empty:
            stock_df = self._filter_by_ventas(stock_df, ventas_df)
        
        # 9. Normalizar Talla (upper + strip)
        stock_df = self._normalize_talla(stock_df)
        
        # 10. Reconstruir SKU con componentes limpios
        stock_df = self._rebuild_sku(stock_df)
        
        # 11. Convertir tipos de datos
        stock_df = self._convert_types(stock_df)
        
        # 12. Eliminar columnas intermedias
        stock_df = self._drop_intermediate_columns(stock_df)
        
        # 13. Reordenar columnas
        stock_df = self._reorder_columns(stock_df)
        
        self._log_step(f"[13/13] Final: {len(stock_df):,} filas procesadas")
        
        return stock_df
    
    def _strip_all_text(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 1: Eliminar padding de SQL Server
        
        CRÍTICO: SQL Server retorna campos CHAR con espacios:
            "CALI UNICO          " → "CALI UNICO"
            "1484612             " → "1484612"
        """
        self._log_step("[1/13] Eliminando padding de columnas de texto")
        
        df = strip_all_string_columns(df)
        
        if self.debug:
            sample_cols = ['Referencia', 'detalle ext. 2', 'Desc. bodega']
            for col in sample_cols:
                if col in df.columns:
                    sample = df[col].iloc[0] if len(df) > 0 else None
                    logger.debug(f"  {col} sample: '{sample}'")
        
        return df
    
    def _build_sku_provisional(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 2: Construir SKU provisional
        (Antes de renombrar "detalle ext. 2" a "Talla")
        """
        self._log_step("[2/13] Construyendo SKU provisional")
        
        if 'Referencia' in df.columns and 'detalle ext. 2' in df.columns:
            df['SKU'] = (df['Referencia'].fillna('').astype(str) + 
                        df['detalle ext. 2'].fillna('').astype(str))
        
        return df
    
    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 3: Renombrar columnas
        """
        self._log_step("[3/13] Renombrando columnas")
        
        rename_map = {
            'detalle ext. 2': 'Talla',
            'Desc. bodega': 'Tienda'  # Para consistencia con Basecompleta
        }
        
        df = df.rename(columns=rename_map)
        
        return df
    
    def _filter_excluded_prefixes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 4: Filtrar referencias con prefijos excluidos (N, S)
        """
        self._log_step("[4/13] Filtrando referencias con prefijos excluidos")
        
        before = len(df)
        
        # Crear máscara para referencias válidas
        mask = pd.Series(True, index=df.index)
        
        for prefix in REFERENCIAS_PREFIJOS_EXCLUIR:
            mask &= ~df['Referencia'].str.startswith(prefix, na=False)
        
        df = df[mask].copy()
        after = len(df)
        
        self._log_step(f"  Filtrado prefijos {REFERENCIAS_PREFIJOS_EXCLUIR}: "
                      f"{after:,}/{before:,} filas")
        
        return df
    
    def _filter_bodegas(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 5: Filtrar por lista blanca de bodegas activas
        """
        self._log_step("[5/13] Filtrando por lista blanca de bodegas")
        
        before = len(df)
        
        # Normalizar nombres de bodegas para comparación
        df_tiendas_normalized = df['Tienda'].str.strip().str.upper()
        bodegas_normalized = {b.strip().upper() for b in BODEGAS_ACTIVAS}
        
        df = df[df_tiendas_normalized.isin(bodegas_normalized)].copy()
        after = len(df)
        
        self._log_step(f"  Filtrado bodegas: {after:,}/{before:,} filas "
                      f"({len(BODEGAS_ACTIVAS)} bodegas permitidas)")
        
        if self.debug:
            bodegas_found = df['Tienda'].unique()
            logger.debug(f"  Bodegas encontradas: {sorted(bodegas_found)[:5]}...")
        
        return df
    
    def _filter_excluded_words(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 6: Filtrar referencias con palabras excluidas (PROMO)
        """
        self._log_step("[6/13] Filtrando referencias con palabras excluidas")
        
        before = len(df)
        
        # Crear máscara para referencias válidas
        mask = pd.Series(True, index=df.index)
        
        for word in REFERENCIAS_PALABRAS_EXCLUIR:
            mask &= ~df['Referencia'].str.contains(word, na=False, case=False)
        
        df = df[mask].copy()
        after = len(df)
        
        self._log_step(f"  Filtrado palabras {REFERENCIAS_PALABRAS_EXCLUIR}: "
                      f"{after:,}/{before:,} filas")
        
        return df
    
    def _clean_referencia(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 7: Limpiar Referencia (caracteres de control ASCII)
        """
        self._log_step("[7/13] Limpiando Referencia")
        
        if 'Referencia' in df.columns:
            df['Referencia'] = clean_referencia(df['Referencia'])
        
        return df
    
    def _filter_by_ventas(self, 
                         df: pd.DataFrame, 
                         ventas_df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 8: JOIN INTERNO con Ventas
        
        Solo mantiene stock de referencias que tienen ventas.
        Esto evita procesar productos obsoletos o sin movimiento.
        """
        self._log_step("[8/13] Filtrando por referencias vendidas (JOIN con Ventas)")
        
        if 'Referencia' not in ventas_df.columns:
            logger.warning("DataFrame de ventas no tiene columna 'Referencia'; "
                          "saltando filtro")
            return df
        
        before = len(df)
        
        # Extraer referencias únicas de ventas (ya limpias)
        refs_vendidas = ventas_df['Referencia'].dropna().unique()
        
        # Filtrar stock
        df = df[df['Referencia'].isin(refs_vendidas)].copy()
        after = len(df)
        
        self._log_step(f"  JOIN con Ventas: {after:,}/{before:,} filas "
                      f"({len(refs_vendidas):,} referencias vendidas)")
        
        return df
    
    def _normalize_talla(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 9: Normalizar Talla (strip + upper)
        """
        self._log_step("[9/13] Normalizando Talla")
        
        if 'Talla' in df.columns:
            df['Talla'] = normalize_talla(df['Talla'])
        
        return df
    
    def _rebuild_sku(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 10: Reconstruir SKU con componentes limpios
        
        Ahora Referencia está limpia y Talla normalizada,
        por lo que el SKU será consistente.
        """
        self._log_step("[10/13] Reconstruyendo SKU")
        
        df = build_sku(df, ref_col='Referencia', talla_col='Talla', sku_col='SKU')
        
        if self.debug and len(df) > 0:
            sample = df[['Referencia', 'Talla', 'SKU']].iloc[0]
            logger.debug(f"  Ejemplo SKU: {sample['Referencia']} + "
                        f"{sample['Talla']} = {sample['SKU']}")
        
        return df
    
    def _convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 11: Conversión de tipos de datos
        """
        self._log_step("[11/13] Convirtiendo tipos de datos")
        
        # Existencia: decimal → Int64 (entero nullable)
        if 'Existencia' in df.columns:
            df['Existencia'] = (pd.to_numeric(df['Existencia'], errors='coerce')
                               .round(0)
                               .astype('Int64'))
        
        # Bodega: asegurar int
        if 'Bodega' in df.columns:
            df['Bodega'] = pd.to_numeric(df['Bodega'], errors='coerce').astype('Int64')
        
        # C.O. bodega: asegurar int
        if 'C.O. bodega' in df.columns:
            df['C.O. bodega'] = pd.to_numeric(df['C.O. bodega'], errors='coerce').astype('Int64')

        if 'Tienda' in df.columns:
            df['IsEcom'] = df['Tienda'].str.contains(
            'ECOM|ECO|ONLINE|VIRTUAL|WEB', 
            case=False, 
            na=False
        )
        else:
        # Si no hay columna Tienda, asumir False
            df['IsEcom'] = False
        
        return df
    
    def _drop_intermediate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 12: Eliminar columnas intermedias que no se necesitan
        """
        self._log_step("[12/13] Eliminando columnas intermedias")
        
        # Columnas que ya no se necesitan
        cols_to_drop = ['Cant Disponible', 'Cant Transito ent']
        
        df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], 
                    errors='ignore')
        
        return df
    
    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PASO 13: Reordenar columnas (compatibilidad con Basecompleta.py)
        """
        self._log_step("[13/13] Reordenando columnas")
        
        # Orden deseado (columnas principales al frente)
        desired_order = [
            "Referencia", "SKU", "Talla", "Existencia", "Tienda",
            "Bodega", "C.O. bodega", "RANGO", "CLASIFICACION", "IsEcom"
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
        logger.info("Aplicando filtro de selección al stock")
        
        # Detectar columna de referencias en selección
        col_sel = None
        if 'Referencias' in selection_df.columns:
            col_sel = 'Referencias'
        elif 'Referencia' in selection_df.columns:
            col_sel = 'Referencia'
        else:
            logger.warning("DataFrame de selección no tiene columna "
                          "'Referencias' o 'Referencia'")
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
        
        logger.info(f"Filtro de selección: {after:,}/{before:,} filas "
                   f"({after/before*100:.1f}%)")
        
        return df_filtered
