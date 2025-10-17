"""
Tests unitarios para el motor de traslados
"""
import pandas as pd
import numpy as np
import pytest
from pathlib import Path
import sys

# Agregar path del proyecto
sys.path.insert(0, str(Path(__file__).parent.parent))

from traslados.engine_core import TrasladosEngineCore


class TestSeedingLogic:
    """Tests para lógica de siembra"""
    
    @pytest.fixture
    def sample_stock(self):
        """Stock de prueba"""
        return pd.DataFrame([
            # CALI CHIPICHAPE tiene ref 1234567 en varias tallas
            {'Tienda': 'CALI CHIPICHAPE', 'SKU': '123456712M', 'Referencia': '1234567', 
             'Talla': '12M', 'Existencia': 5, 'ADU': 1.0, 'IsEcom': False, 'Cobertura_dias': 5.0},
            {'Tienda': 'CALI CHIPICHAPE', 'SKU': '123456718M', 'Referencia': '1234567', 
             'Talla': '18M', 'Existencia': 3, 'ADU': 0.8, 'IsEcom': False, 'Cobertura_dias': 3.75},
            
            # CALI UNICENTRO NO tiene ref 1234567 (nunca la ha vendido)
            {'Tienda': 'CALI UNICENTRO', 'SKU': '987654318M', 'Referencia': '9876543', 
             'Talla': '18M', 'Existencia': 10, 'ADU': 2.0, 'IsEcom': False, 'Cobertura_dias': 5.0},
            
            # BODEGA PRINCIPAL tiene de todo
            {'Tienda': 'BODEGA PRINCIPAL', 'SKU': '123456712M', 'Referencia': '1234567', 
             'Talla': '12M', 'Existencia': 50, 'ADU': 0.0, 'IsEcom': False, 'Cobertura_dias': np.inf},
            {'Tienda': 'BODEGA PRINCIPAL', 'SKU': '123456718M', 'Referencia': '1234567', 
             'Talla': '18M', 'Existencia': 40, 'ADU': 0.0, 'IsEcom': False, 'Cobertura_dias': np.inf},
        ])
    
    @pytest.fixture
    def sample_adu(self):
        """ADU de prueba"""
        return pd.DataFrame([
            {'Tienda': 'CALI CHIPICHAPE', 'SKU': '123456712M', 'ADU': 1.0},
            {'Tienda': 'CALI CHIPICHAPE', 'SKU': '123456718M', 'ADU': 0.8},
            {'Tienda': 'CALI UNICENTRO', 'SKU': '123456718M', 'ADU': 0.5},  # SÍ vendió esta talla
            {'Tienda': 'CALI UNICENTRO', 'SKU': '987654318M', 'ADU': 2.0},
        ])
    
    def test_siembra_permitida_ref_existente(self, sample_stock, sample_adu):
        """Test: Permite siembra si la tienda YA tiene la referencia"""
        engine = TrasladosEngineCore(
            stock_df=sample_stock,
            adu_df=sample_adu,
            bodega_principal='BODEGA PRINCIPAL',
            no_seed=True  # Política estricta
        )
        
        # CALI CHIPICHAPE ya tiene ref 1234567 (tallas 12M y 18M)
        # Por lo tanto, puede recibir talla 6M de la misma ref
        result = engine.can_seed_to_store(
            tienda='CALI CHIPICHAPE',
            referencia='1234567',
            sku='12345676M'  # Nueva talla
        )
        
        assert result is True, "Debe permitir siembra de nueva talla si ref existe"
    
    def test_siembra_bloqueada_ref_nueva_sin_adu(self, sample_stock, sample_adu):
        """Test: Bloquea siembra de referencia nueva sin ventas"""
        engine = TrasladosEngineCore(
            stock_df=sample_stock,
            adu_df=sample_adu,
            bodega_principal='BODEGA PRINCIPAL',
            no_seed=True  # Política estricta
        )
        
        # CALI UNICENTRO NO tiene ref 1234567
        # Y el SKU 123456712M NO tiene ADU en esa tienda
        result = engine.can_seed_to_store(
            tienda='CALI UNICENTRO',
            referencia='1234567',
            sku='123456712M'
        )
        
        assert result is False, "Debe bloquear siembra de ref nueva sin ventas"
    
    def test_siembra_permitida_con_adu(self, sample_stock, sample_adu):
        """Test: Permite siembra si el SKU tiene ADU > 0 (ventas históricas)"""
        engine = TrasladosEngineCore(
            stock_df=sample_stock,
            adu_df=sample_adu,
            bodega_principal='BODEGA PRINCIPAL',
            no_seed=True,
            allow_seed_if_adu=True  # Excepción: permitir si hay ventas
        )
        
        # CALI UNICENTRO NO tiene ref 1234567
        # PERO el SKU 123456718M SÍ tiene ADU=0.5 en esa tienda (sí lo vendió)
        result = engine.can_seed_to_store(
            tienda='CALI UNICENTRO',
            referencia='1234567',
            sku='123456718M'
        )
        
        assert result is True, "Debe permitir siembra si SKU tiene ADU > 0"


class TestOriginRanking:
    """Tests para ranking de orígenes"""
    
    @pytest.fixture
    def sample_stock_with_regions(self):
        """Stock con info de regiones"""
        return pd.DataFrame([
            # Destino: CALI CHIPICHAPE necesita SKU
            {'Tienda': 'CALI CHIPICHAPE', 'SKU': '123456718M', 'Referencia': '1234567', 
             'Talla': '18M', 'Existencia': 1, 'ADU': 2.0, 'IsEcom': False, 
             'Cobertura_dias': 0.5, 'Region': 'VALLE', 'RegionID': 4},
            
            # Origen 1: CALI UNICENTRO (misma región, mucha cobertura)
            {'Tienda': 'CALI UNICENTRO', 'SKU': '123456718M', 'Referencia': '1234567', 
             'Talla': '18M', 'Existencia': 20, 'ADU': 1.0, 'IsEcom': False, 
             'Cobertura_dias': 20.0, 'Region': 'VALLE', 'RegionID': 4},
            
            # Origen 2: BARRANQUILLA (región diferente, cobertura media)
            {'Tienda': 'BARRANQUILLA UNICO', 'SKU': '123456718M', 'Referencia': '1234567', 
             'Talla': '18M', 'Existencia': 15, 'ADU': 1.0, 'IsEcom': False, 
             'Cobertura_dias': 15.0, 'Region': 'ATLANTICO', 'RegionID': 1},
            
            # Origen 3: BODEGA PRINCIPAL (valle, cobertura infinita)
            {'Tienda': 'BODEGA PRINCIPAL', 'SKU': '123456718M', 'Referencia': '1234567', 
             'Talla': '18M', 'Existencia': 100, 'ADU': 0.0, 'IsEcom': False, 
             'Cobertura_dias': np.inf, 'Region': 'VALLE', 'RegionID': 4},
        ])
    
    @pytest.fixture
    def sample_tiempos(self):
        """Tiempos de entrega"""
        return pd.DataFrame([
            {'_O': 'BODEGA PRINCIPAL', '_D': 'CALI CHIPICHAPE', '_ETA_NUM': 2.0, '_PRI_NUM': 1},
            {'_O': 'CALI UNICENTRO', '_D': 'CALI CHIPICHAPE', '_ETA_NUM': 1.0, '_PRI_NUM': 2},
            {'_O': 'BARRANQUILLA UNICO', '_D': 'CALI CHIPICHAPE', '_ETA_NUM': 3.0, '_PRI_NUM': 3},
        ])
    
    def test_ranking_prioriza_bodega_principal(self, sample_stock_with_regions, sample_tiempos):
        """Test: Bodega principal siempre es primera opción"""
        engine = TrasladosEngineCore(
            stock_df=sample_stock_with_regions,
            adu_df=pd.DataFrame(),  # No importa para este test
            tiempos_df=sample_tiempos,
            bodega_principal='BODEGA PRINCIPAL'
        )
        
        origins = engine.rank_origins_for_sku(
            sku='123456718M',
            dest_tienda='CALI CHIPICHAPE',
            dest_referencia='1234567'
        )
        
        assert origins[0] == 'BODEGA PRINCIPAL', "Bodega principal debe ser primera"
    
    def test_ranking_misma_region_prioritario(self, sample_stock_with_regions, sample_tiempos):
        """Test: Misma región es prioritaria sobre región diferente"""
        # Temporalmente sin bodega para ver el ranking puro
        engine = TrasladosEngineCore(
            stock_df=sample_stock_with_regions[
                sample_stock_with_regions['Tienda'] != 'BODEGA PRINCIPAL'
            ],
            adu_df=pd.DataFrame(),
            tiempos_df=sample_tiempos,
            bodega_principal=None
        )
        
        origins = engine.rank_origins_for_sku(
            sku='123456718M',
            dest_tienda='CALI CHIPICHAPE',
            dest_referencia='1234567'
        )
        
        # CALI UNICENTRO (misma región) debe estar antes que BARRANQUILLA (otra región)
        idx_unicentro = origins.index('CALI UNICENTRO')
        idx_barranquilla = origins.index('BARRANQUILLA UNICO')
        
        assert idx_unicentro < idx_barranquilla, "Misma región debe tener prioridad"


class TestStockCalculations:
    """Tests para cálculos de stock y cobertura"""
    
    def test_allowed_to_send_bodega(self):
        """Test: Bodega puede enviar TODO su stock"""
        stock = pd.DataFrame([
            {'Tienda': 'BODEGA PRINCIPAL', 'SKU': '123456718M', 'Referencia': '1234567',
             'Talla': '18M', 'Existencia': 100, 'ADU': 0.0, 'IsEcom': False, 
             'MinObjetivo': 0, 'Cobertura_dias': np.inf}
        ])
        
        engine = TrasladosEngineCore(
            stock_df=stock,
            adu_df=pd.DataFrame(),
            bodega_principal='BODEGA PRINCIPAL'
        )
        
        disponible = engine.allowed_to_send('BODEGA PRINCIPAL', '123456718M')
        
        assert disponible == 100, "Bodega debe poder enviar TODO"
    
    def test_allowed_to_send_tienda_guarda_cobertura(self):
        """Test: Tiendas guardan cobertura mínima (7 días)"""
        stock = pd.DataFrame([
            {'Tienda': 'CALI CHIPICHAPE', 'SKU': '123456718M', 'Referencia': '1234567',
             'Talla': '18M', 'Existencia': 20, 'ADU': 2.0, 'IsEcom': False,
             'MinObjetivo': 2, 'Cobertura_dias': 10.0}
        ])
        
        engine = TrasladosEngineCore(
            stock_df=stock,
            adu_df=pd.DataFrame(),
            bodega_principal='BODEGA PRINCIPAL'
        )
        
        disponible = engine.allowed_to_send('CALI CHIPICHAPE', '123456718M')
        
        # Debe guardar: max(MinObjetivo=2, 7_días * 2.0_ADU = 14)
        # Guardar = 14, disponible = 20 - 14 = 6
        assert disponible == 6, "Debe guardar 7 días de cobertura"


def run_tests():
    """Ejecuta todos los tests"""
    pytest.main([__file__, '-v', '--tb=short'])


if __name__ == '__main__':
    run_tests()