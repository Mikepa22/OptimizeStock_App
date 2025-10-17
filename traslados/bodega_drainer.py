"""
Drenador inteligente de bodega principal

Vacía residual de bodega enviando a tiendas que tienen historial de ventas,
priorizando SKUs de alto movimiento y tiendas de categoría superior.
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging

from config.settings import (
    MAX_STOCK_PER_SKU,
    STORE_CATEGORY,
    get_store_category
)

logger = logging.getLogger(__name__)


class BodegaDrainer:
    """
    Drena stock residual de bodega principal
    
    Ejecuta al FINAL, después de satisfacer necesidades base y completar curvas.
    Objetivo: minimizar inventario muerto en bodega (no vende).
    """
    
    def __init__(self,
                 stock_df: pd.DataFrame,
                 adu_df: pd.DataFrame,
                 bodega_principal: str,
                 no_seed: bool = True,
                 allow_seed_if_adu: bool = False,
                 debug: bool = False):
        """
        Args:
            stock_df: DataFrame de stock
            adu_df: DataFrame con ADU por Tienda/SKU
            bodega_principal: Nombre de bodega principal
            no_seed: Si True, no sembrar referencias nuevas
            allow_seed_if_adu: Permitir siembra si ADU > 0
            debug: Modo debug
        """
        self.stock_df = stock_df.copy()
        self.adu_df = adu_df
        self.bodega_principal = bodega_principal
        self.no_seed = no_seed
        self.allow_seed_if_adu = allow_seed_if_adu
        self.debug = debug
        
        self.transfers = []
        
        # Construir índices
        self._build_indexes()
    
    def _build_indexes(self):
        """Construir índices para búsquedas rápidas"""
        # Índice: (Tienda, SKU) -> índices
        self.idx_tienda_sku = {}
        for idx, row in self.stock_df.iterrows():
            key = (row['Tienda'], row['SKU'])
            if key not in self.idx_tienda_sku:
                self.idx_tienda_sku[key] = []
            self.idx_tienda_sku[key].append(idx)
        
        # Índice de ADU
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
        """Obtener ADU"""
        return self.adu_map.get((tienda, sku), 0.0)
    
    def get_bodega_total(self) -> int:
        """Total en bodega"""
        return int(
            self.stock_df[
                self.stock_df['Tienda'] == self.bodega_principal
            ]['Existencia'].sum()
        )
    
    def can_seed_to_store(self, tienda: str, referencia: str, sku: str) -> bool:
        """
        Valida si se puede sembrar un SKU en tienda
        
        Misma lógica que el motor principal
        """
        # Verificar si tiene la referencia
        has_ref_now = (
            self.stock_df[
                (self.stock_df['Tienda'] == tienda) &
                (self.stock_df['Referencia'] == referencia) &
                (self.stock_df['Existencia'] > 0)
            ].shape[0] > 0
        )
        
        if has_ref_now:
            return True
        
        # Política de siembra
        if self.no_seed:
            if self.allow_seed_if_adu:
                adu = self.get_adu(tienda, sku)
                if adu > 0:
                    return True
            
            if self.debug:
                logger.debug(f"[seed-block] {tienda} / {referencia} / {sku}")
            return False
        
        return True
    
    def calculate_drain_limit(self, safety_ratio: float) -> int:
        """
        Calcula límite de drenaje respetando safety_ratio
        
        Args:
            safety_ratio: % de stock a conservar en bodega (0-1)
                         0.0 = drenar todo
                         0.2 = conservar 20%, drenar 80%
        
        Returns:
            Unidades máximas a drenar
        """
        total_bodega = self.get_bodega_total()
        
        if safety_ratio <= 0:
            return total_bodega
        
        # Validar rango
        safety_ratio = max(0.0, min(0.99, safety_ratio))
        
        # Calcular cuánto drenar
        max_drain = int(np.floor(total_bodega * (1.0 - safety_ratio)))
        
        if self.debug:
            conservar = total_bodega - max_drain
            logger.debug(f"Límite drenaje: drenar={max_drain:,}, conservar={conservar:,} ({safety_ratio*100:.0f}%)")
        
        return max_drain
    
    def get_skus_to_drain(self) -> List[Tuple[str, int, float]]:
        """
        Obtiene SKUs en bodega ordenados por prioridad
        
        Prioridad: ADU total descendente (SKUs de alto movimiento primero)
        
        Returns:
            Lista de (sku, cantidad_disponible, adu_total)
        """
        # Agrupar por SKU
        bodega_stock = self.stock_df[
            self.stock_df['Tienda'] == self.bodega_principal
        ].groupby('SKU', as_index=False).agg({
            'Existencia': 'sum'
        })
        
        # Calcular ADU total de cada SKU (suma de todas las tiendas)
        sku_adu_total = {}
        for (tienda, sku), adu in self.adu_map.items():
            if sku not in sku_adu_total:
                sku_adu_total[sku] = 0.0
            sku_adu_total[sku] += adu
        
        # Agregar ADU total
        bodega_stock['ADU_Total'] = bodega_stock['SKU'].map(sku_adu_total).fillna(0.0)
        
        # Ordenar por ADU descendente
        bodega_stock = bodega_stock.sort_values('ADU_Total', ascending=False)
        
        # Convertir a lista de tuplas
        skus = [
            (row['SKU'], int(row['Existencia']), row['ADU_Total'])
            for _, row in bodega_stock.iterrows()
        ]
        
        return skus
    
    def get_destinations_for_sku(self, sku: str) -> List[Tuple[str, float]]:
        """
        Obtiene destinos ordenados para un SKU
        
        Criterios:
        1. Tienen ADU > 0 para este SKU (lo venden)
        2. Ordenar por: categoría A/B/C, luego ADU del SKU desc
        
        Returns:
            Lista de (tienda, adu_sku)
        """
        # Buscar tiendas con ADU > 0 para este SKU
        candidates = []
        for (tienda, sku_check), adu in self.adu_map.items():
            if sku_check != sku:
                continue
            if tienda == self.bodega_principal:
                continue
            if adu <= 0:
                continue
            
            candidates.append((tienda, adu))
        
        if not candidates:
            return []
        
        # Ordenar por categoría, luego ADU
        def sort_key(item):
            tienda, adu = item
            cat = get_store_category(tienda)
            cat_rank = {'A': 0, 'B': 1, 'C': 2}.get(cat, 3)
            return (cat_rank, -adu, tienda)
        
        candidates.sort(key=sort_key)
        
        return candidates
    
    def execute_transfer(self,
                        origen: str,
                        destino: str,
                        sku: str,
                        cantidad: int,
                        referencia: str,
                        talla: str) -> bool:
        """Ejecuta traslado"""
        key_origen = (origen, sku)
        key_destino = (destino, sku)
        
        if key_origen not in self.idx_tienda_sku:
            return False
        
        stock_origen_antes = self.get_stock(origen, sku)
        stock_destino_antes = self.get_stock(destino, sku)
        
        # Actualizar origen
        indices_origen = self.idx_tienda_sku[key_origen]
        qty_per_row = cantidad / len(indices_origen)
        self.stock_df.loc[indices_origen, 'Existencia'] -= qty_per_row
        
        # Actualizar destino
        if key_destino in self.idx_tienda_sku:
            indices_destino = self.idx_tienda_sku[key_destino]
            qty_per_row = cantidad / len(indices_destino)
            self.stock_df.loc[indices_destino, 'Existencia'] += qty_per_row
        else:
            # Crear fila (siembra validada previamente)
            new_row = {
                'Tienda': destino,
                'SKU': sku,
                'Referencia': referencia,
                'Talla': talla,
                'RANGO_CAT': None,
                'Region': None,
                'RegionID': None,
                'Tipo': None,
                'IsEcom': 'ECOM' in destino.upper(),
                'MinObjetivo': 2,
                'ADU': self.get_adu(destino, sku),
                'Cobertura_dias': np.inf,
                'Existencia': cantidad
            }
            new_index = len(self.stock_df)
            self.stock_df = pd.concat([
                self.stock_df,
                pd.DataFrame([new_row], index=[new_index])
            ], ignore_index=False)
            
            self.idx_tienda_sku[key_destino] = [new_index]
        
        # Registrar
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
    
    def drain(self, safety_ratio: float = 0.0) -> Tuple[pd.DataFrame, List[dict]]:
        """
        Ejecuta drenaje de bodega
        
        Args:
            safety_ratio: % stock a conservar (0.0 = drenar todo, 0.2 = conservar 20%)
        
        Returns:
            Tuple (stock_df actualizado, lista de traslados)
        """
        logger.info("=" * 60)
        logger.info("DRENAJE RESIDUAL DE BODEGA")
        logger.info("=" * 60)
        
        bodega_inicial = self.get_bodega_total()
        if bodega_inicial <= 0:
            logger.info("Bodega sin stock - omitiendo drenaje")
            return self.stock_df, []
        
        logger.info(f"Stock inicial bodega: {bodega_inicial:,} unidades")
        logger.info(f"Safety ratio: {safety_ratio*100:.0f}%")
        
        # Calcular límite
        max_drain = self.calculate_drain_limit(safety_ratio)
        drained = 0
        
        # Obtener SKUs ordenados por ADU
        skus = self.get_skus_to_drain()
        
        transfers_count = 0
        
        for sku, disponible, adu_total in skus:
            if drained >= max_drain:
                if self.debug:
                    logger.debug("Límite de drenaje alcanzado")
                break
            
            # Extraer ref y talla
            if len(sku) < 7:
                continue
            ref = sku[:7]
            talla = sku[7:] if len(sku) > 7 else ""
            
            # Cuánto podemos drenar de este SKU
            qty_disponible = min(disponible, max_drain - drained)
            if qty_disponible <= 0:
                continue
            
            # Obtener destinos
            destinos = self.get_destinations_for_sku(sku)
            if not destinos:
                continue
            
            # Distribuir a destinos
            for tienda, adu_sku in destinos:
                if qty_disponible <= 0:
                    break
                if drained >= max_drain:
                    break
                
                # Validar siembra
                if not self.can_seed_to_store(tienda, ref, sku):
                    continue
                
                # Calcular capacidad
                stock_actual = self.get_stock(tienda, sku)
                cap_disponible = max(0, MAX_STOCK_PER_SKU - stock_actual)
                
                if cap_disponible <= 0:
                    continue
                
                # Cantidad a trasladar
                qty = min(cap_disponible, qty_disponible, max_drain - drained)
                
                if qty > 0:
                    success = self.execute_transfer(
                        self.bodega_principal,
                        tienda,
                        sku,
                        int(qty),
                        ref,
                        talla
                    )
                    
                    if success:
                        drained += int(qty)
                        qty_disponible -= int(qty)
                        transfers_count += 1
        
        bodega_final = self.get_bodega_total()
        
        logger.info(f"Drenaje finalizado:")
        logger.info(f"  Traslados ejecutados: {transfers_count}")
        logger.info(f"  Unidades drenadas: {drained:,}")
        logger.info(f"  Stock final bodega: {bodega_final:,}")
        
        return self.stock_df, self.transfers