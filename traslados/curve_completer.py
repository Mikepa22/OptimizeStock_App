"""
Completador de curvas de tallas desde bodega principal

Completa curvas de tallas faltantes en tiendas, priorizando:
1. Tiendas A > B > C (por categoría)
2. Mayor ADU de tienda (dentro de misma categoría)
3. Referencias que YA maneja la tienda
4. Tallas que SÍ tienen ventas históricas (ADU > umbral)
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging

from config.settings import (
    MIN_POR_SKU_TIENDA,
    MAX_STOCK_PER_SKU,
    CURVAS_TALLAS,
    STORE_CATEGORY,
    get_store_category
)

logger = logging.getLogger(__name__)


class CurveCompleter:
    """
    Completa curvas de tallas desde bodega principal
    
    Ejecuta DESPUÉS de las necesidades base, cuando ya hay stock mínimo
    garantizado en todas las tiendas.
    """
    
    # Umbral mínimo de ADU para considerar una talla
    # 0.05 = al menos 1 venta cada 20 días
    ADU_MIN_THRESHOLD = 0.05
    
    def __init__(self,
                 stock_df: pd.DataFrame,
                 adu_df: pd.DataFrame,
                 bodega_principal: str,
                 debug: bool = False):
        """
        Args:
            stock_df: DataFrame de stock con índices ya construidos
            adu_df: DataFrame con ADU por Tienda/SKU
            bodega_principal: Nombre de bodega principal
            debug: Modo debug
        """
        self.stock_df = stock_df.copy()
        self.adu_df = adu_df
        self.bodega_principal = bodega_principal
        self.debug = debug
        
        self.transfers = []
        
        # Construir índices para búsquedas rápidas
        self._build_indexes()
    
    def _build_indexes(self):
        """Construir índices para búsquedas O(1)"""
        # Índice: (Tienda, SKU) -> índices en DataFrame
        self.idx_tienda_sku = {}
        for idx, row in self.stock_df.iterrows():
            key = (row['Tienda'], row['SKU'])
            if key not in self.idx_tienda_sku:
                self.idx_tienda_sku[key] = []
            self.idx_tienda_sku[key].append(idx)
        
        # Índice de ADU: (Tienda, SKU) -> ADU
        if not self.adu_df.empty:
            self.adu_map = {}
            for _, row in self.adu_df.iterrows():
                key = (row['Tienda'], row['SKU'])
                self.adu_map[key] = float(row['ADU'])
        else:
            self.adu_map = {}
    
    def get_stock(self, tienda: str, sku: str) -> int:
        """Obtener stock actual"""
        key = (tienda, sku)
        if key not in self.idx_tienda_sku:
            return 0
        indices = self.idx_tienda_sku[key]
        return int(self.stock_df.loc[indices, 'Existencia'].sum())
    
    def get_adu(self, tienda: str, sku: str) -> float:
        """Obtener ADU de un SKU en tienda"""
        return self.adu_map.get((tienda, sku), 0.0)
    
    def get_bodega_total(self) -> int:
        """Total de stock en bodega principal"""
        return int(
            self.stock_df[
                self.stock_df['Tienda'] == self.bodega_principal
            ]['Existencia'].sum()
        )
    
    def prioritize_stores(self) -> List[str]:
        """
        Ordena tiendas por prioridad para recibir curvas
        
        Criterio:
        1. Categoría A > B > C
        2. Dentro de categoría: mayor ADU total
        3. Desempate: nombre alfabético
        
        Returns:
            Lista de tiendas ordenadas (excluye bodega)
        """
        # Calcular ADU total por tienda
        adu_per_store = {}
        for (tienda, sku), adu in self.adu_map.items():
            if tienda not in adu_per_store:
                adu_per_store[tienda] = 0.0
            adu_per_store[tienda] += adu
        
        # Listar tiendas (excluir bodega)
        stores = [
            t for t in self.stock_df['Tienda'].unique()
            if t != self.bodega_principal
        ]
        
        # Ordenar por: categoría, ADU total desc, nombre
        def sort_key(tienda):
            cat = get_store_category(tienda)
            cat_rank = {'A': 0, 'B': 1, 'C': 2}.get(cat, 3)
            adu_total = adu_per_store.get(tienda, 0.0)
            return (cat_rank, -adu_total, tienda)
        
        stores.sort(key=sort_key)
        
        if self.debug:
            logger.debug("Priorización de tiendas para curvas:")
            for i, t in enumerate(stores[:5]):
                cat = get_store_category(t)
                adu = adu_per_store.get(t, 0.0)
                logger.debug(f"  {i+1}. {t} (Cat:{cat}, ADU:{adu:.2f})")
        
        return stores
    
    def get_refs_in_store(self, tienda: str) -> List[str]:
        """
        Obtiene referencias que la tienda YA maneja
        
        Incluye solo referencias con stock > 0
        """
        refs = self.stock_df[
            (self.stock_df['Tienda'] == tienda) &
            (self.stock_df['Existencia'] > 0)
        ]['Referencia'].unique().tolist()
        
        return refs
    
    def get_rango_for_ref(self, tienda: str, ref: str) -> Optional[str]:
        """
        Detecta el RANGO (BEBES/NIÑOS) de una referencia en tienda
        
        Basado en las tallas que ya tiene
        """
        rows = self.stock_df[
            (self.stock_df['Tienda'] == tienda) &
            (self.stock_df['Referencia'] == ref)
        ]
        
        if rows.empty:
            return None
        
        tallas_presentes = set(rows['Talla'].dropna().astype(str).str.upper())
        
        bebes_tallas = set(CURVAS_TALLAS.get('BEBES', []))
        ninos_tallas = set(CURVAS_TALLAS.get('NIÑOS', []))
        
        if tallas_presentes & bebes_tallas:
            return 'BEBES'
        if tallas_presentes & ninos_tallas:
            return 'NIÑOS'
        
        return None
    
    def get_candidate_tallas(self, tienda: str, ref: str, rango: str) -> List[str]:
        """
        Obtiene tallas candidatas para completar curva
        
        Filtra por:
        1. Tallas de la curva del rango
        2. ADU > umbral mínimo
        3. Stock actual < MIN_POR_SKU (2)
        
        Returns:
            Lista de tallas ordenadas por ADU desc
        """
        if rango not in CURVAS_TALLAS:
            return []
        
        curva = CURVAS_TALLAS[rango]
        
        candidates = []
        for talla in curva:
            sku = f"{ref}{talla}"
            adu = self.get_adu(tienda, sku)
            stock_actual = self.get_stock(tienda, sku)
            
            # Filtros
            if adu < self.ADU_MIN_THRESHOLD:
                continue  # Sin ventas suficientes
            
            if stock_actual >= MIN_POR_SKU_TIENDA:
                continue  # Ya tiene el mínimo
            
            candidates.append((talla, adu))
        
        # Ordenar por ADU descendente
        candidates.sort(key=lambda x: -x[1])
        
        return [t for t, _ in candidates]
    
    def execute_transfer(self,
                        origen: str,
                        destino: str,
                        sku: str,
                        cantidad: int,
                        referencia: str,
                        talla: str) -> bool:
        """
        Ejecuta traslado y actualiza estado
        
        Returns:
            True si se ejecutó, False si no pudo
        """
        key_origen = (origen, sku)
        key_destino = (destino, sku)
        
        # Validar origen tiene stock
        if key_origen not in self.idx_tienda_sku:
            return False
        
        stock_origen_antes = self.get_stock(origen, sku)
        if stock_origen_antes < cantidad:
            return False
        
        stock_destino_antes = self.get_stock(destino, sku)
        
        # Actualizar origen
        indices_origen = self.idx_tienda_sku[key_origen]
        qty_per_row = cantidad / len(indices_origen)
        self.stock_df.loc[indices_origen, 'Existencia'] -= qty_per_row
        
        # Actualizar destino (o crear fila si no existe)
        if key_destino in self.idx_tienda_sku:
            indices_destino = self.idx_tienda_sku[key_destino]
            qty_per_row = cantidad / len(indices_destino)
            self.stock_df.loc[indices_destino, 'Existencia'] += qty_per_row
        else:
            # Crear fila nueva
            new_row = self._create_stock_row(destino, sku, referencia, talla, cantidad)
            new_index = len(self.stock_df)
            self.stock_df = pd.concat([
                self.stock_df,
                pd.DataFrame([new_row], index=[new_index])
            ], ignore_index=False)
            
            # Actualizar índice
            self.idx_tienda_sku[key_destino] = [new_index]
        
        # Registrar traslado
        stock_origen_despues = self.get_stock(origen, sku)
        stock_destino_despues = self.get_stock(destino, sku)
        
        self.transfers.append({
            'Tienda origen': origen,
            'Tienda destino': destino,
            'Stock tienda origen antes traslado': stock_origen_antes,
            'Stock tienda origen despues traslado': stock_origen_despues,
            'Stock tienda destino antes traslado': stock_destino_antes,
            'Stock tienda destino despues del traslado': stock_destino_despues,
            'Unidades a trasladar': cantidad,
            'Referencia': referencia,
            'Talla': talla
        })
        
        return True
    
    def _create_stock_row(self, tienda: str, sku: str, ref: str, talla: str, qty: int) -> dict:
        """Crea fila nueva para stock (siembra de talla)"""
        adu = self.get_adu(tienda, sku)
        
        return {
            'Tienda': tienda,
            'SKU': sku,
            'Referencia': ref,
            'Talla': talla,
            'RANGO_CAT': None,
            'Region': None,
            'RegionID': None,
            'Tipo': None,
            'IsEcom': 'ECOM' in tienda.upper(),
            'MinObjetivo': MIN_POR_SKU_TIENDA,
            'ADU': adu,
            'Cobertura_dias': qty / adu if adu > 0 else np.inf,
            'Existencia': qty
        }
    
    def complete_curves(self) -> Tuple[pd.DataFrame, List[dict]]:
        """
        Ejecuta proceso de completar curvas
        
        Returns:
            Tuple (stock_df actualizado, lista de traslados)
        """
        logger.info("=" * 60)
        logger.info("COMPLETAR CURVAS DESDE BODEGA")
        logger.info("=" * 60)
        
        # Verificar bodega tiene stock
        bodega_total = self.get_bodega_total()
        if bodega_total <= 0:
            logger.info("Bodega sin stock - omitiendo completar curvas")
            return self.stock_df, []
        
        logger.info(f"Stock inicial en bodega: {bodega_total:,} unidades")
        
        # Priorizar tiendas
        stores = self.prioritize_stores()
        
        transfers_count = 0
        
        for tienda in stores:
            # Verificar bodega
            if self.get_bodega_total() <= 0:
                if self.debug:
                    logger.debug("Bodega agotada - fin")
                break
            
            # Referencias que YA tiene la tienda
            refs = self.get_refs_in_store(tienda)
            
            if not refs:
                continue
            
            # Ordenar refs por ADU total en tienda (desc)
            refs_with_adu = []
            for ref in refs:
                # Sumar ADU de todas las tallas de esta ref en esta tienda
                ref_adu = sum(
                    self.get_adu(tienda, f"{ref}{t}")
                    for t in CURVAS_TALLAS.get('BEBES', []) + CURVAS_TALLAS.get('NIÑOS', [])
                )
                refs_with_adu.append((ref, ref_adu))
            
            refs_with_adu.sort(key=lambda x: -x[1])
            refs = [r for r, _ in refs_with_adu]
            
            for ref in refs:
                if self.get_bodega_total() <= 0:
                    break
                
                # Detectar rango
                rango = self.get_rango_for_ref(tienda, ref)
                if not rango:
                    continue
                
                # Tallas candidatas
                tallas = self.get_candidate_tallas(tienda, ref, rango)
                
                for talla in tallas:
                    if self.get_bodega_total() <= 0:
                        break
                    
                    sku = f"{ref}{talla}"
                    
                    # Calcular necesidad
                    stock_actual = self.get_stock(tienda, sku)
                    necesita = MIN_POR_SKU_TIENDA - stock_actual
                    
                    # Cap con máximo
                    cap_disponible = MAX_STOCK_PER_SKU - stock_actual
                    necesita = min(necesita, cap_disponible)
                    
                    if necesita <= 0:
                        continue
                    
                    # Verificar bodega tiene
                    bodega_stock = self.get_stock(self.bodega_principal, sku)
                    if bodega_stock <= 0:
                        continue
                    
                    # Cantidad a trasladar
                    qty = min(necesita, bodega_stock)
                    
                    if qty > 0:
                        success = self.execute_transfer(
                            self.bodega_principal,
                            tienda,
                            sku,
                            qty,
                            ref,
                            talla
                        )
                        
                        if success:
                            transfers_count += 1
        
        bodega_final = self.get_bodega_total()
        
        logger.info(f"Completar curvas finalizado:")
        logger.info(f"  Traslados ejecutados: {transfers_count}")
        logger.info(f"  Stock final bodega: {bodega_final:,} unidades")
        logger.info(f"  Unidades movidas: {bodega_total - bodega_final:,}")
        
        return self.stock_df, self.transfers