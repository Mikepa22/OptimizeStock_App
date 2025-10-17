"""
Script de verificación previo a la ejecución del pipeline
Verifica que todos los componentes estén listos
"""
import sys
from pathlib import Path

def verify_setup():
    """Verifica la configuración antes de ejecutar el pipeline"""
    
    print("=" * 70)
    print("VERIFICACIÓN DE CONFIGURACIÓN - Sistema de Traslados")
    print("=" * 70)
    print()
    
    errors = []
    warnings = []
    
    # 1. Verificar archivos CSV auxiliares
    print("1. Verificando archivos CSV auxiliares...")
    
    # Buscar en raíz y en data/
    tiendas_files = (list(Path('.').glob('TIENDAS.csv')) + 
                     list(Path('.').glob('Clasificacion_Tiendas.csv')) +
                     list(Path('data').glob('TIENDAS.csv')) +
                     list(Path('data').glob('Clasificacion_Tiendas.csv')))
    if tiendas_files:
        print(f"   ✓ Archivo de tiendas encontrado: {tiendas_files[0]}")
    else:
        errors.append("   ✗ No se encontró TIENDAS.csv o Clasificacion_Tiendas.csv en raíz o data/")
    
    tiempo_files = (list(Path('.').glob('TIEMPO*.csv')) + 
                    list(Path('.').glob('Tiempos*.csv')) +
                    list(Path('data').glob('TIEMPO*.csv')) +
                    list(Path('data').glob('Tiempos*.csv')))
    if tiempo_files:
        print(f"   ✓ Archivo de tiempos encontrado: {tiempo_files[0]}")
    else:
        warnings.append("   ⚠ No se encontró TIEMPO.csv o TIEMPOS.csv en raíz o data/ (opcional)")
    
    print()
    
    # 2. Verificar .env
    print("2. Verificando archivo de configuración...")
    env_file = Path('.env')
    if env_file.exists():
        print(f"   ✓ Archivo .env encontrado")
        
        # Leer y verificar variables clave
        with open(env_file, 'r', encoding='utf-8') as f:
            env_content = f.read()
            required_vars = ['SQL_SERVER', 'SQL_DATABASE', 'SQL_USER', 'SQL_PASSWORD']
            for var in required_vars:
                if var in env_content:
                    print(f"   ✓ {var} configurado")
                else:
                    errors.append(f"   ✗ {var} no encontrado en .env")
    else:
        errors.append("   ✗ Archivo .env no encontrado")
    
    print()
    
    # 3. Verificar estructura de directorios
    print("3. Verificando estructura de directorios...")
    required_dirs = ['config', 'db', 'processors', 'core', 'traslados']
    for dir_name in required_dirs:
        if Path(dir_name).exists():
            print(f"   ✓ Directorio {dir_name}/ encontrado")
        else:
            errors.append(f"   ✗ Directorio {dir_name}/ no encontrado")
    
    print()
    
    # 4. Verificar archivos principales
    print("4. Verificando archivos principales...")
    required_files = ['main.py', 'config/database.py', 'db/connection.py', 'traslados/orchestrator.py']
    for file_name in required_files:
        if Path(file_name).exists():
            print(f"   ✓ {file_name} encontrado")
        else:
            errors.append(f"   ✗ {file_name} no encontrado")
    
    print()
    
    # 5. Verificar módulos Python
    print("5. Verificando módulos Python instalados...")
    required_modules = ['pandas', 'pyodbc', 'openpyxl', 'python-dotenv']
    for module in required_modules:
        try:
            __import__(module.replace('-', '_'))
            print(f"   ✓ {module} instalado")
        except ImportError:
            errors.append(f"   ✗ {module} no instalado")
    
    print()
    
    # 6. Test de importación de módulos del proyecto
    print("6. Verificando módulos del proyecto...")
    try:
        from config.database import DatabaseConfig
        print("   ✓ config.database importado correctamente")
    except Exception as e:
        errors.append(f"   ✗ Error importando config.database: {e}")
    
    try:
        from traslados.orchestrator import TrasladosOrchestrator
        print("   ✓ traslados.orchestrator importado correctamente")
    except Exception as e:
        errors.append(f"   ✗ Error importando traslados.orchestrator: {e}")
    
    print()
    print("=" * 70)
    
    # Resumen
    if errors:
        print("❌ ERRORES ENCONTRADOS:")
        for error in errors:
            print(error)
        print()
    
    if warnings:
        print("⚠️  ADVERTENCIAS:")
        for warning in warnings:
            print(warning)
        print()
    
    if not errors and not warnings:
        print("✅ VERIFICACIÓN COMPLETA - Todo listo para ejecutar el pipeline!")
        print()
        print("Comando sugerido:")
        print("  python main.py --meses 2 --debug")
        return True
    elif not errors:
        print("✅ VERIFICACIÓN COMPLETA CON ADVERTENCIAS")
        print("   El pipeline puede ejecutarse, pero revisa las advertencias.")
        return True
    else:
        print("❌ VERIFICACIÓN FALLIDA")
        print("   Corrige los errores antes de ejecutar el pipeline.")
        return False
    
if __name__ == "__main__":
    success = verify_setup()
    sys.exit(0 if success else 1)