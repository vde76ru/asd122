<?php
// src/views/admin/monitoring.php
use App\Services\AuthService;

// Проверка прав доступа
if (!AuthService::checkRole('admin')) {
    header('Location: /');
    exit;
}
?>

<!-- Копируем содержимое из monitoring-interface артефакта выше -->
<!-- Это тот же HTML/CSS/JS код, но внутри PHP view -->

<style>
/* Добавляем стили специфичные для интеграции с сайтом */
.monitoring-container {
    margin-left: 0; /* Отступ для сайдбара если нужно */
}

@media (max-width: 768px) {
    .grid {
        grid-template-columns: 1fr;
    }
    
    .metric-grid {
        grid-template-columns: 1fr;
    }
}
</style>

<div class="monitoring-container">
    <!-- Весь HTML код из monitoring-interface -->
</div>

<script>
// Добавляем CSRF токен для запросов
const csrfToken = '<?= \App\Core\CSRF::token() ?>';

// Модифицируем fetch запросы чтобы включить CSRF
const originalFetch = window.fetch;
window.fetch = function(url, options = {}) {
    if (url.startsWith('/api/')) {
        options.headers = {
            ...options.headers,
            'X-CSRF-Token': csrfToken,
            'X-Requested-With': 'XMLHttpRequest'
        };
    }
    return originalFetch(url, options);
};

// Дополнительные функции для интеграции
function exportReport() {
    if (!lastReport) {
        showNotification('Сначала запустите проверку', 'warning');
        return;
    }
    
    const dataStr = JSON.stringify(lastReport, null, 2);
    const dataUri = 'data:application/json;charset=utf-8,'+ encodeURIComponent(dataStr);
    
    const exportFileDefaultName = `monitoring-report-${new Date().toISOString()}.json`;
    
    const linkElement = document.createElement('a');
    linkElement.setAttribute('href', dataUri);
    linkElement.setAttribute('download', exportFileDefaultName);
    linkElement.click();
}

// Добавляем кнопку экспорта
document.addEventListener('DOMContentLoaded', function() {
    const headerDiv = document.querySelector('.header > div');
    const exportBtn = document.createElement('button');
    exportBtn.className = 'btn';
    exportBtn.style.marginLeft = '10px';
    exportBtn.innerHTML = '📥 Экспорт';
    exportBtn.onclick = exportReport;
    headerDiv.appendChild(exportBtn);
});
</script>