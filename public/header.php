<?php
declare(strict_types=1);
use App\Core\Database;
use App\Core\CSRF;
use App\Services\AuthService;

// Дополнительные заголовки только если они не установлены nginx
if (!headers_sent()) {
    header("Cache-Control: no-store, no-cache, must-revalidate, max-age=0");
    header("Pragma: no-cache");
    header("Expires: Sat, 26 Jul 1997 05:00:00 GMT");
    header("Vary: Accept-Encoding");
    header("X-Content-Type-Options: nosniff");
    header("X-Frame-Options: SAMEORIGIN");
    header("X-XSS-Protection: 1; mode=block");
    header("Referrer-Policy: strict-origin-when-cross-origin");
}

try {
    $pdo = Database::getConnection();
    $stmt = $pdo->query("SELECT city_id, name FROM cities ORDER BY name");
    $cities = $stmt->fetchAll();
} catch (PDOException $e) {
    error_log('Database error in header.php: ' . $e->getMessage());
    $cities = [];
}

$current_path = $_SERVER['REQUEST_URI'] ?? '/';
?>
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>VDestor B2B - Электротехническое оборудование</title>
    
    <!-- Preconnect для ускорения загрузки -->
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link rel="preconnect" href="https://cdnjs.cloudflare.com">
    
    <!-- Шрифты -->
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    
    <!-- Иконки Font Awesome -->
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" integrity="sha512-iecdLmaskl7CVkqkXNQ/ZH/XLlvWZOJyj7Yy7tcenmpD1ypASozpmT/E0iPtmFIB46ZmdtAc9eNBvH0H/ZpiBw==" crossorigin="anonymous" referrerpolicy="no-referrer" />
    
    <!-- Фавикон -->
    <link rel="icon" href="/favicon.ico" type="image/x-icon">
    <link rel="icon" type="image/png" sizes="32x32" href="/assets/icons/favicon-32x32.png">
    <link rel="icon" type="image/png" sizes="16x16" href="/assets/icons/favicon-16x16.png">
    <link rel="apple-touch-icon" sizes="180x180" href="/assets/icons/apple-touch-icon.png">
    <link rel="manifest" href="/site.webmanifest">
    
    <!-- jQuery для совместимости -->
    <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/3.7.1/jquery.min.js" integrity="sha512-v2CJ7UaYy4JwqLDIrZUI/4hqeoQieOmAZNXBeQyjo21dadnwR+8ZaIJVT8EE2iyI61OV8e6M8PP2/4hpQINQ/g==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>
    
    <!-- Динамическое подключение CSS от Vite -->
    <?php
    $distPath = $_SERVER['DOCUMENT_ROOT'] . '/assets/dist/assets/';
    if (is_dir($distPath)) {
        $cssFiles = glob($distPath . 'main-*.css');
        foreach ($cssFiles as $cssFile) {
            $cssUrl = str_replace($_SERVER['DOCUMENT_ROOT'], '', $cssFile);
            echo '<link rel="stylesheet" href="' . htmlspecialchars($cssUrl) . '">' . PHP_EOL;
        }
    } else {
        echo '<style>body { font-family: Inter, sans-serif; margin: 0; padding: 20px; }</style>' . PHP_EOL;
    }
    ?>
    
    <!-- Глобальные переменные для JS (только критичные) -->
    <script>
        window.APP_CONFIG = {
            csrfToken: <?= json_encode(CSRF::token(), JSON_HEX_TAG) ?>,
            isAuthenticated: <?= AuthService::check() ? 'true' : 'false' ?>,
            currentPath: <?= json_encode($current_path, JSON_HEX_TAG) ?>
        };
    </script>
</head>
<body>
    <div class="app-layout">
        <!-- Боковая панель навигации -->
        <aside class="sidebar" id="sidebar">
            <div class="sidebar-header">
                <a href="/" class="sidebar-logo">
                    <div class="sidebar-logo-icon">V</div>
                    <span class="sidebar-logo-text">VDestor B2B</span>
                </a>
            </div>
            
            <nav class="sidebar-nav">
                <!-- Основное меню -->
                <div class="nav-section">
                    <div class="nav-section-title">Основное</div>
                    <ul class="nav-menu">
                        <li class="nav-item">
                            <a href="/" class="nav-link <?= $current_path === '/' ? 'active' : '' ?>">
                                <svg class="nav-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6"></path>
                                </svg>
                                <span class="nav-text">Дашборд</span>
                            </a>
                        </li>
                        <li class="nav-item">
                            <a href="/shop" class="nav-link <?= strpos($current_path, '/shop') === 0 ? 'active' : '' ?>">
                                <svg class="nav-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 11V7a4 4 0 00-8 0v4M5 9h14l1 12H4L5 9z"></path>
                                </svg>
                                <span class="nav-text">Каталог товаров</span>
                            </a>
                        </li>
                        <li class="nav-item">
                            <a href="/cart" class="nav-link <?= $current_path === '/cart' ? 'active' : '' ?>">
                                <svg class="nav-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 3h2l.4 2M7 13h10l4-8H5.4M7 13L5.4 5M7 13l-2.293 2.293c-.63.63-.184 1.707.707 1.707H17m0 0a2 2 0 100 4 2 2 0 000-4zm-8 2a2 2 0 11-4 0 2 2 0 014 0z"></path>
                                </svg>
                                <span class="nav-text">Корзина</span>
                                <span class="nav-badge" id="cartBadge" style="display: none;">0</span>
                            </a>
                        </li>
                        <li class="nav-item">
                            <a href="/specifications" class="nav-link <?= strpos($current_path, '/specification') === 0 ? 'active' : '' ?>">
                                <svg class="nav-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path>
                                </svg>
                                <span class="nav-text">Спецификации</span>
                            </a>
                        </li>
                    </ul>
                </div>
                
                <!-- Инструменты -->
                <div class="nav-section">
                    <div class="nav-section-title">Инструменты</div>
                    <ul class="nav-menu">
                        <li class="nav-item">
                            <a href="/calculator" class="nav-link">
                                <svg class="nav-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 7h6m0 10v-3m-3 3h.01M9 17h.01M9 14h.01M12 14h.01M15 11h.01M12 11h.01M9 11h.01M7 21h10a2 2 0 002-2V5a2 2 0 00-2-2H7a2 2 0 00-2 2v14a2 2 0 002 2z"></path>
                                </svg>
                                <span class="nav-text">Калькуляторы</span>
                            </a>
                        </li>
                        <li class="nav-item">
                            <a href="/history" class="nav-link">
                                <svg class="nav-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"></path>
                                </svg>
                                <span class="nav-text">История заказов</span>
                            </a>
                        </li>
                    </ul>
                </div>
                
                <?php if (AuthService::check() && AuthService::checkRole('admin')): ?>
                <!-- Управление (для администраторов) -->
                <div class="nav-section">
                    <div class="nav-section-title">Управление</div>
                    <ul class="nav-menu">
                        <li class="nav-item">
                            <a href="/admin" class="nav-link">
                                <svg class="nav-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"></path>
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"></path>
                                </svg>
                                <span class="nav-text">Админ панель</span>
                            </a>
                        </li>
                    </ul>
                </div>
                <?php endif; ?>
                
                <!-- Пользователь -->
                <div class="nav-section mt-auto mb-4">
                    <ul class="nav-menu">
                        <?php if (AuthService::check()): ?>
                            <li class="nav-item">
                                <a href="/profile" class="nav-link">
                                    <svg class="nav-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"></path>
                                    </svg>
                                    <span class="nav-text">Профиль</span>
                                </a>
                            </li>
                            <li class="nav-item">
                                <a href="/logout" class="nav-link">
                                    <svg class="nav-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1"></path>
                                    </svg>
                                    <span class="nav-text">Выйти</span>
                                </a>
                            </li>
                        <?php else: ?>
                            <li class="nav-item">
                                <a href="/login" class="nav-link">
                                    <svg class="nav-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 16l-4-4m0 0l4-4m-4 4h14m-5 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1"></path>
                                    </svg>
                                    <span class="nav-text">Войти</span>
                                </a>
                            </li>
                        <?php endif; ?>
                    </ul>
                </div>
            </nav>
            
            <!-- Кнопка сворачивания -->
            <button class="sidebar-toggle" id="sidebarToggle">
                <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 19l-7-7 7-7m8 14l-7-7 7-7"></path>
                </svg>
            </button>
        </aside>
        
        <!-- Основная область -->
        <div class="main-wrapper">
            <!-- Верхняя панель -->
            <header class="top-header">
                <div class="header-content">
                    <!-- Кнопка мобильного меню -->
                    <button class="btn btn-icon d-none" id="mobileMenuBtn">
                        <svg width="20" height="20" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16"></path>
                        </svg>
                    </button>
                    
                    <!-- Поиск -->
                    <div class="search-box">
                        <svg class="search-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"></path>
                        </svg>
                        <input type="text" class="search-input" placeholder="Поиск товаров..." id="globalSearch">
                    </div>
                    
                    <!-- Правая часть -->
                    <div class="d-flex align-center gap-3 ml-auto">
                        <!-- Выбор города -->
                        <select id="citySelect" class="form-select form-control" style="width: 200px;">
                            <?php foreach ($cities as $city): ?>
                                <option value="<?= htmlspecialchars((string)$city['city_id'], ENT_QUOTES) ?>">
                                    <?= htmlspecialchars($city['name'], ENT_QUOTES) ?>
                                </option>
                            <?php endforeach; ?>
                            <?php if (empty($cities)): ?>
                                <option value="3">Ярославль</option>
                            <?php endif; ?>
                        </select>
                        
                        <!-- Уведомления -->
                        <button class="btn btn-icon" id="notificationsBtn">
                            <svg width="20" height="20" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9"></path>
                            </svg>
                        </button>
                        
                        <!-- Быстрая корзина -->
                        <button class="btn btn-primary btn-sm" onclick="window.location.href='/cart'">
                            <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 3h2l.4 2M7 13h10l4-8H5.4M7 13L5.4 5M7 13l-2.293 2.293c-.63.63-.184 1.707.707 1.707H17m0 0a2 2 0 100 4 2 2 0 000-4zm-8 2a2 2 0 11-4 0 2 2 0 014 0z"></path>
                            </svg>
                            <span>Корзина</span>
                        </button>
                    </div>
                </div>
            </header>
            
            <!-- Основной контент -->
            <main class="main-content">
                <div class="page-container">