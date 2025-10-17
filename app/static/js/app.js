// Estado de la aplicación
let pollingInterval = null;

// Elementos del DOM
const configForm = document.getElementById('configForm');
const generateBtn = document.getElementById('generateBtn');
const btnText = document.getElementById('btnText');
const progressSection = document.getElementById('progressSection');
const progressFill = document.getElementById('progressFill');
const progressText = document.getElementById('progressText');
const progressStage = document.getElementById('progressStage');
const resultsSection = document.getElementById('resultsSection');
const resultsContent = document.getElementById('resultsContent');
const errorSection = document.getElementById('errorSection');
const errorContent = document.getElementById('errorContent');

// Event Listeners
configForm.addEventListener('submit', handleFormSubmit);

/**
 * Maneja el envío del formulario
 */
async function handleFormSubmit(event) {
    event.preventDefault();
    
    // Recolectar datos del formulario
    const formData = {
        meses: parseInt(document.getElementById('meses').value),
        debug: document.getElementById('debug').checked,
        save_intermediates: document.getElementById('save_intermediates').checked,
        dias_min: parseInt(document.getElementById('dias_min').value),
        dias_max: parseInt(document.getElementById('dias_max').value),
        safety_ratio: parseFloat(document.getElementById('safety_ratio').value),
        allow_seed: document.getElementById('allow_seed').checked
    };
    
    // Validaciones
    if (formData.dias_min >= formData.dias_max) {
        alert('Los días mínimos deben ser menores que los días máximos');
        return;
    }
    
    // Iniciar proceso
    await startGeneration(formData);
}

/**
 * Inicia la generación de traslados
 */
async function startGeneration(params) {
    try {
        // Ocultar secciones anteriores
        hideAllSections();
        
        // Deshabilitar botón y mostrar loading
        generateBtn.disabled = true;
        btnText.innerHTML = 'Procesando... <span class="loading"></span>';
        
        // Mostrar sección de progreso
        progressSection.classList.remove('hidden');
        updateProgress(0, 'Iniciando proceso...');
        
        // Llamar al API
        const response = await fetch('/api/generate', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(params)
        });
        
        if (!response.ok) {
            throw new Error('Error al iniciar el proceso');
        }
        
        // Iniciar polling del estado
        startPolling();
        
    } catch (error) {
        showError(error.message);
        resetButton();
    }
}

/**
 * Actualiza la barra de progreso
 */
function updateProgress(progress, stage) {
    progressFill.style.width = `${progress}%`;
    progressText.textContent = `${progress}%`;
    progressStage.textContent = stage;
}

/**
 * Inicia el polling del estado
 */
function startPolling() {
    pollingInterval = setInterval(async () => {
        try {
            const response = await fetch('/api/status');
            const status = await response.json();
            
            // Actualizar progreso
            updateProgress(status.progress, status.stage);
            
            // Verificar si terminó
            if (!status.running) {
                stopPolling();
                
                if (status.error) {
                    showError(status.error);
                } else if (status.output_files && status.output_files.length > 0) {
                    // Obtener estadísticas finales
                    fetchFinalResults();
                } else {
                    showError('Proceso completado pero no se generaron archivos');
                }
                
                resetButton();
            }
        } catch (error) {
            console.error('Error en polling:', error);
        }
    }, 1000); // Poll cada segundo
}

/**
 * Detiene el polling
 */
function stopPolling() {
    if (pollingInterval) {
        clearInterval(pollingInterval);
        pollingInterval = null;
    }
}

/**
 * Obtiene los resultados finales
 */
async function fetchFinalResults() {
    try {
        const response = await fetch('/api/status');
        const status = await response.json();
        
        if (status.output_files && status.output_files.length > 0) {
            showResults(status.output_files);
        }
    } catch (error) {
        showError('Error al obtener resultados: ' + error.message);
    }
}

/**
 * Muestra los resultados exitosos
 */
function showResults(files) {
    hideAllSections();
    resultsSection.classList.remove('hidden');
    
    // Crear contenido de resultados
    let html = `
        <div class="success-message">
            <p style="font-size: 1.2rem; margin-bottom: 30px; color: var(--success-color); font-weight: 600;">
                ✓ Traslados generados exitosamente
            </p>
        </div>
        
        <div class="download-section">
            <h3>Archivos Generados</h3>
            <p style="margin-bottom: 20px; color: var(--text-secondary);">
                Haz clic en los botones para descargar los archivos:
            </p>
    `;
    
    files.forEach((file, index) => {
        const fileType = file.includes('resumen') ? 'Resumen' : 'Traslados Completos';
        html += `
            <a href="/api/download/${file}" class="download-btn" download>
                📥 Descargar ${fileType}
            </a>
        `;
    });
    
    html += `
        </div>
        <div style="margin-top: 30px; text-align: center;">
            <button onclick="resetApp()" class="btn-secondary">Generar Nuevo Reporte</button>
        </div>
    `;
    
    resultsContent.innerHTML = html;
}

/**
 * Muestra un error
 */
function showError(message) {
    hideAllSections();
    errorSection.classList.remove('hidden');
    errorContent.textContent = message;
}

/**
 * Oculta todas las secciones de resultado
 */
function hideAllSections() {
    progressSection.classList.add('hidden');
    resultsSection.classList.add('hidden');
    errorSection.classList.add('hidden');
}

/**
 * Resetea el botón de generar
 */
function resetButton() {
    generateBtn.disabled = false;
    btnText.textContent = 'Generar Traslados';
}

/**
 * Resetea toda la aplicación
 */
async function resetApp() {
    // Resetear estado en el servidor
    await fetch('/api/reset', { method: 'DELETE' });
    
    // Resetear UI
    hideAllSections();
    resetButton();
    updateProgress(0, '');
    
    // Scroll al inicio
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

// Cleanup al cerrar la página
window.addEventListener('beforeunload', () => {
    stopPolling();
});