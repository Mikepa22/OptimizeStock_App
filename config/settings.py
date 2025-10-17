"""
Configuración de reglas de negocio y constantes
Externalizadas para fácil mantenimiento
"""
from typing import Set

# ==========================================
# BODEGAS ACTIVAS
# ==========================================

BODEGAS_ACTIVAS: Set[str] = {
    "BARRANQUILLA BUENAVISTA",
    "BARRANQUILLA PORTAL DEL PRADO",
    "BARRANQUILLA UNICO",
    "BARRANQUILLA VIVA",
    "BODEGA ECOMMERCE",
    "BODEGA PRINCIPAL",
    "BOGOTA PLAZA CENTRAL",
    "BUGA PLAZA",
    "CALI CHIPICHAPE",
    "CALI JARDIN PLAZA",
    "CALI UNICENTRO",
    "CALI UNICO",
    "CARTAGENA CARIBE PLAZA",
    "CUCUTA UNICENTRO",
    "ECOMMERCE",
    "MONTERIA ALAMEDAS",
    "NEIVA SAN PEDRO",
    "PALMIRA LLANOGRANDE",
    "POPAYAN CAMPANARIO",
    "TULUA LA HERRADURA"
}

# ==========================================
# FILTROS DE REFERENCIAS
# ==========================================

# Prefijos que deben excluirse
REFERENCIAS_PREFIJOS_EXCLUIR: Set[str] = {
    "N",  # Referencias que empiezan con N
    "S"   # Referencias que empiezan con S
}

# Palabras que NO deben aparecer en referencias
REFERENCIAS_PALABRAS_EXCLUIR: Set[str] = {
    "PROMO"
}

# ==========================================
# CLASIFICACIONES
# ==========================================

# Solo procesar estas clasificaciones
CLASIFICACIONES_PERMITIDAS: Set[str] = {
    "PRENDAS"
}

# ==========================================
# CURVAS DE TALLAS (para Basecompleta)
# ==========================================

CURVAS_TALLAS = {
    'BEBES': ['0M', '3M', '6M', '9M', '12M', '18M'],
    'NIÑOS': ['2T', '3T', '4T', '5T', '6', '8', '10', '12']
}

CURVAS_MIN_TALLAS = {
    'BEBES': 3,  # Mínimo 3 tallas con stock
    'NIÑOS': 5   # Mínimo 5 tallas con stock
}

# ==========================================
# OBJETIVOS DE STOCK (para Basecompleta)
# ==========================================

MIN_POR_SKU_TIENDA = 2      # Mínimo por SKU en tiendas físicas
MIN_POR_SKU_ECOM = 3        # Mínimo por SKU en e-commerce
MAX_STOCK_PER_SKU = 6       # Tope máximo por SKU

# ==========================================
# COBERTURA (para Basecompleta)
# ==========================================

ORIGIN_MIN_COV_DAYS = 7      # Origen debe mantener 7 días de cobertura
DEST_TARGET_COV_DAYS = 7     # Destino apunta a 7 días de cobertura
ORIGIN_MIN_COV_ECOM = 7      # E-commerce necesita más cobertura
DEST_TARGET_COV_ECOM = 7
COV_BUFFER_DAYS = 1          # Margen de seguridad

# ==========================================
# CATEGORIZACIÓN DE TIENDAS (para Basecompleta)
# ==========================================

STORE_CATEGORY = {
    'BOGOTA PLAZA CENTRAL': 'C',
    'NEIVA SAN PEDRO': 'B',
    'BARRANQUILLA BUENAVISTA': 'B',
    'BARRANQUILLA PORTAL DEL PRADO': 'C',
    'BARRANQUILLA UNICO': 'A',
    'BARRANQUILLA VIVA': 'C',
    'CARTAGENA CARIBE PLAZA': 'B',
    'CUCUTA UNICENTRO': 'C',
    'MONTERIA ALAMEDAS': 'C',
    'BUGA PLAZA': 'C',
    'CALI CHIPICHAPE': 'B',
    'CALI JARDIN PLAZA': 'A',
    'CALI UNICENTRO': 'B',
    'CALI UNICO': 'A',
    'ECOMMERCE': 'A',
    'PALMIRA LLANOGRANDE': 'B',
    'POPAYAN CAMPANARIO': 'C',
    'TULUA LA HERRADURA': 'C',
}

def get_store_category(store_name: str) -> str:
    """Retorna categoría de tienda (A/B/C), default C"""
    return STORE_CATEGORY.get(store_name.strip().upper(), 'C')
