</div> <!-- .page-container -->
</main> <!-- .main-content -->
</div> <!-- .main-wrapper -->
</div> <!-- .app-layout -->

<!-- Toast контейнер для уведомлений -->
<div class="toast-container" id="toastContainer"></div>

<!-- Подключаем скомпилированные JS файлы от Vite -->
<?php
$distPath = $_SERVER['DOCUMENT_ROOT'] . '/assets/dist/assets/';
if (is_dir($distPath)) {
    $jsFiles = glob($distPath . 'main-*.js');
    foreach ($jsFiles as $jsFile) {
        $jsUrl = str_replace($_SERVER['DOCUMENT_ROOT'], '', $jsFile);
        echo '<script type="module" src="' . htmlspecialchars($jsUrl) . '"></script>' . PHP_EOL;
    }
} else {
    echo '<!-- Vite assets not found. Run "npm run build" to generate them. -->' . PHP_EOL;
}
?>

<!-- Минимальная инициализация для layout -->
<script>
(function() {
    // Восстанавливаем состояние сайдбара
    const sidebar = document.getElementById('sidebar');
    const sidebarCollapsed = localStorage.getItem('sidebar-collapsed') === 'true';
    if (sidebar && sidebarCollapsed) {
        sidebar.classList.add('collapsed');
    }
    
    // Обработчик для сайдбара
    const sidebarToggle = document.getElementById('sidebarToggle');
    if (sidebarToggle) {
        sidebarToggle.addEventListener('click', function() {
            sidebar.classList.toggle('collapsed');
            localStorage.setItem('sidebar-collapsed', sidebar.classList.contains('collapsed'));
        });
    }
    
    // Мобильное меню
    const mobileMenuBtn = document.getElementById('mobileMenuBtn');
    if (window.innerWidth <= 768 && mobileMenuBtn) {
        mobileMenuBtn.classList.remove('d-none');
        mobileMenuBtn.addEventListener('click', function() {
            sidebar.classList.toggle('mobile-open');
        });
    }
})();
</script>

</body>
</html>