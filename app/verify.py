"""
Script para verificar que main.py está correcto
"""
import sys
from pathlib import Path

def verify_main_py():
    """Verifica el contenido de main.py"""
    
    main_file = Path("main.py")
    
    if not main_file.exists():
        print("❌ ERROR: main.py no existe en el directorio actual")
        print(f"   Directorio actual: {Path.cwd()}")
        return False
    
    print("✅ main.py existe")
    print(f"   Tamaño: {main_file.stat().st_size} bytes")
    
    # Leer contenido
    content = main_file.read_text(encoding='utf-8')
    
    # Verificaciones críticas
    checks = {
        "import pandas as pd": "Import de pandas",
        "app = FastAPI(": "Creación de app FastAPI",
        "def run_traslados_pipeline": "Función principal del pipeline",
        "@app.get(\"/\")": "Endpoint raíz",
        "@app.post(\"/api/generate\")": "Endpoint de generación",
        "@app.get(\"/api/status\")": "Endpoint de estado",
        "TrasladosOrchestrator": "Import del orchestrator"
    }
    
    print("\n📋 Verificando contenido:")
    all_ok = True
    
    for pattern, description in checks.items():
        if pattern in content:
            print(f"   ✅ {description}")
        else:
            print(f"   ❌ FALTA: {description}")
            all_ok = False
    
    # Intentar compilar
    print("\n🔧 Verificando sintaxis...")
    try:
        compile(content, 'main.py', 'exec')
        print("   ✅ Sintaxis correcta")
    except SyntaxError as e:
        print(f"   ❌ ERROR DE SINTAXIS en línea {e.lineno}: {e.msg}")
        all_ok = False
    
    # Intentar importar
    print("\n📦 Intentando importar...")
    try:
        import main
        print("   ✅ Import exitoso")
        
        if hasattr(main, 'app'):
            print("   ✅ Objeto 'app' encontrado")
        else:
            print("   ❌ Objeto 'app' NO encontrado")
            all_ok = False
            
    except Exception as e:
        print(f"   ❌ Error al importar: {e}")
        all_ok = False
    
    print("\n" + "="*60)
    if all_ok:
        print("✅ main.py está CORRECTO - Puedes ejecutar el servidor")
        print("\n💡 Ejecuta: uvicorn main:app --reload")
    else:
        print("❌ main.py tiene PROBLEMAS - Necesita corrección")
        print("\n💡 Copia nuevamente el contenido del artifact")
    
    return all_ok

if __name__ == "__main__":
    verify_main_py()