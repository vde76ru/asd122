<?php
use App\Core\Database;

// Получаем статистику из БД
$pdo = Database::getConnection();
$stats = [
    'products' => $pdo->query("SELECT COUNT(*) FROM products")->fetchColumn(),
    'brands' => $pdo->query("SELECT COUNT(*) FROM brands")->fetchColumn(),
    'categories' => $pdo->query("SELECT COUNT(*) FROM categories")->fetchColumn(),
    'users' => $pdo->query("SELECT COUNT(*) FROM users WHERE is_active = 1")->fetchColumn()
];

// Популярные категории
$categories = $pdo->query("
    SELECT c.*, COUNT(pc.product_id) as product_count 
    FROM categories c 
    LEFT JOIN product_categories pc ON c.category_id = pc.category_id 
    WHERE c.parent_id IS NULL 
    GROUP BY c.category_id 
    ORDER BY product_count DESC 
    LIMIT 6
")->fetchAll();

// Новости/акции (можно заменить на реальные данные)
$news = [
    ['date' => date('d.m.Y', strtotime('-2 days')), 'title' => 'Новое поступление продукции ABB', 'text' => 'Расширен ассортимент автоматических выключателей серии S200'],
    ['date' => date('d.m.Y', strtotime('-5 days')), 'title' => 'Открыт новый склад в Санкт-Петербурге', 'text' => 'Теперь доставка по СПб за 1 день'],
    ['date' => date('d.m.Y', strtotime('-7 days')), 'title' => 'Специальные цены на кабельную продукцию', 'text' => 'Скидки до 15% на весь ассортимент кабелей']
];
?>

<!-- Hero Section с параллакс эффектом -->
<section class="hero-section">
    <div class="hero-background">
        <div class="hero-particles" id="particles-js"></div>
        <div class="hero-gradient"></div>
    </div>
    
    <div class="hero-content animate-fadeIn">
        <h1 class="hero-title">
            <span class="hero-title-line">Электротехническое</span>
            <span class="hero-title-line gradient-text">оборудование</span>
            <span class="hero-title-line">для вашего бизнеса</span>
        </h1>
        
        <p class="hero-subtitle">
            Более <?= number_format($stats['products']) ?> наименований от ведущих производителей.<br>
            Прямые поставки. Гарантия качества. Доставка по всей России.
        </p>
        
        <div class="hero-actions">
            <a href="/shop" class="btn btn-primary btn-lg hero-btn">
                <svg width="20" height="20" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 11V7a4 4 0 00-8 0v4M5 9h14l1 12H4L5 9z"></path>
                </svg>
                Перейти в каталог
            </a>
            <a href="#features" class="btn btn-outline btn-lg hero-btn">
                <svg width="20" height="20" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path>
                </svg>
                Узнать больше
            </a>
        </div>
        
        <div class="hero-stats">
            <div class="hero-stat">
                <div class="hero-stat-value" data-count="<?= $stats['products'] ?>">0</div>
                <div class="hero-stat-label">Товаров</div>
            </div>
            <div class="hero-stat">
                <div class="hero-stat-value" data-count="<?= $stats['brands'] ?>">0</div>
                <div class="hero-stat-label">Брендов</div>
            </div>
            <div class="hero-stat">
                <div class="hero-stat-value" data-count="<?= $stats['users'] ?>">0</div>
                <div class="hero-stat-label">Клиентов</div>
            </div>
        </div>
    </div>
    
    <div class="hero-scroll-indicator">
        <svg width="24" height="24" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 14l-7 7m0 0l-7-7m7 7V3"></path>
        </svg>
    </div>
</section>

<!-- Преимущества с иконками -->
<section id="features" class="features-section">
    <div class="container">
        <div class="section-header">
            <h2 class="section-title">Почему выбирают нас</h2>
            <p class="section-subtitle">Мы предлагаем комплексные решения для вашего бизнеса</p>
        </div>
        
        <div class="features-grid">
            <div class="feature-card animate-slideInUp">
                <div class="feature-icon">
                    <div class="feature-icon-bg"></div>
                    <svg width="48" height="48" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"></path>
                    </svg>
                </div>
                <h3 class="feature-title">Гарантия качества</h3>
                <p class="feature-text">Вся продукция сертифицирована и соответствует российским стандартам. Работаем только с проверенными поставщиками.</p>
                <a href="/certificates" class="feature-link">
                    Сертификаты
                    <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path>
                    </svg>
                </a>
            </div>
            
            <div class="feature-card animate-slideInUp" style="animation-delay: 0.1s">
                <div class="feature-icon">
                    <div class="feature-icon-bg"></div>
                    <svg width="48" height="48" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path>
                    </svg>
                </div>
                <h3 class="feature-title">Быстрая доставка</h3>
                <p class="feature-text">Доставка по Москве на следующий день. Отправка в регионы в день заказа. Собственный автопарк.</p>
                <a href="/delivery" class="feature-link">
                    Условия доставки
                    <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path>
                    </svg>
                </a>
            </div>
            
            <div class="feature-card animate-slideInUp" style="animation-delay: 0.2s">
                <div class="feature-icon">
                    <div class="feature-icon-bg"></div>
                    <svg width="48" height="48" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path>
                    </svg>
                </div>
                <h3 class="feature-title">Выгодные цены</h3>
                <p class="feature-text">Прямые контракты с производителями. Индивидуальные условия для постоянных клиентов. Гибкая система скидок.</p>
                <a href="/prices" class="feature-link">
                    Прайс-лист
                    <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path>
                    </svg>
                </a>
            </div>
            
            <div class="feature-card animate-slideInUp" style="animation-delay: 0.3s">
                <div class="feature-icon">
                    <div class="feature-icon-bg"></div>
                    <svg width="48" height="48" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18.364 5.636l-3.536 3.536m0 5.656l3.536 3.536M9.172 9.172L5.636 5.636m3.536 9.192l-3.536 3.536M21 12a9 9 0 11-18 0 9 9 0 0118 0zm-5 0a4 4 0 11-8 0 4 4 0 018 0z"></path>
                    </svg>
                </div>
                <h3 class="feature-title">Техподдержка</h3>
                <p class="feature-text">Квалифицированные специалисты помогут подобрать оборудование и ответят на технические вопросы.</p>
                <a href="/support" class="feature-link">
                    Связаться
                    <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path>
                    </svg>
                </a>
            </div>
            
            <div class="feature-card animate-slideInUp" style="animation-delay: 0.4s">
                <div class="feature-icon">
                    <div class="feature-icon-bg"></div>
                    <svg width="48" height="48" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-3 7h3m-3 4h3m-6-4h.01M9 16h.01"></path>
                    </svg>
                </div>
                <h3 class="feature-title">Удобный сервис</h3>
                <p class="feature-text">Личный кабинет с историей заказов. Формирование спецификаций онлайн. Интеграция с 1С.</p>
                <a href="/features" class="feature-link">
                    Возможности
                    <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path>
                    </svg>
                </a>
            </div>
            
            <div class="feature-card animate-slideInUp" style="animation-delay: 0.5s">
                <div class="feature-icon">
                    <div class="feature-icon-bg"></div>
                    <svg width="48" height="48" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4"></path>
                    </svg>
                </div>
                <h3 class="feature-title">Широкий ассортимент</h3>
                <p class="feature-text">Более <?= number_format($stats['products']) ?> позиций на складе. Автоматы, кабель, освещение, щитовое оборудование.</p>
                <a href="/catalog" class="feature-link">
                    Каталог товаров
                    <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path>
                    </svg>
                </a>
            </div>
        </div>
    </div>
</section>

<!-- Популярные категории -->
<section class="categories-section">
    <div class="container">
        <div class="section-header">
            <h2 class="section-title">Популярные категории</h2>
            <a href="/shop" class="section-link">
                Все категории
                <svg width="20" height="20" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 8l4 4m0 0l-4 4m4-4H3"></path>
                </svg>
            </a>
        </div>
        
        <div class="categories-grid">
            <?php foreach ($categories as $index => $category): ?>
            <a href="/shop?category=<?= $category['category_id'] ?>" class="category-card animate-slideInUp" style="animation-delay: <?= $index * 0.1 ?>s">
                <div class="category-card-bg"></div>
                <div class="category-icon">
                    <?php
                    // Иконки для категорий (можно заменить на реальные из БД)
                    $icons = [
                        'default' => '<svg width="40" height="40" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20 7l-8-4-8 4m16 0l-8 4m8-4v10l-8 4m0-10L4 7m8 4v10M4 7v10l8 4"></path></svg>',
                        'Автоматические выключатели' => '<svg width="40" height="40" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path></svg>',
                        'Розетки и выключатели' => '<svg width="40" height="40" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 6V4m0 2a2 2 0 100 4m0-4a2 2 0 110 4m-6 8a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4m6 6v10m6-2a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4"></path></svg>',
                        'Кабели и провода' => '<svg width="40" height="40" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"></path></svg>',
                        'Щитовое оборудование' => '<svg width="40" height="40" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 5a1 1 0 011-1h14a1 1 0 011 1v2a1 1 0 01-1 1H5a1 1 0 01-1-1V5zM4 13a1 1 0 011-1h6a1 1 0 011 1v6a1 1 0 01-1 1H5a1 1 0 01-1-1v-6zM16 13a1 1 0 011-1h2a1 1 0 011 1v6a1 1 0 01-1 1h-2a1 1 0 01-1-1v-6z"></path></svg>',
                        'Освещение' => '<svg width="40" height="40" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z"></path></svg>',
                        'Инструменты' => '<svg width="40" height="40" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"></path></svg>'
                    ];
                    echo $icons[$category['name']] ?? $icons['default'];
                    ?>
                </div>
                <h3 class="category-name"><?= htmlspecialchars($category['name']) ?></h3>
                <p class="category-count"><?= number_format($category['product_count']) ?> товаров</p>
            </a>
            <?php endforeach; ?>
        </div>
    </div>
</section>

<!-- Блок CTA с градиентом -->
<section class="cta-section">
    <div class="cta-bg">
        <div class="cta-pattern"></div>
    </div>
    <div class="container">
        <div class="cta-content">
            <h2 class="cta-title">Готовы сделать заказ?</h2>
            <p class="cta-subtitle">Зарегистрируйтесь и получите доступ к специальным ценам для бизнеса</p>
            <div class="cta-actions">
                <a href="/register" class="btn btn-primary btn-lg">
                    Регистрация
                    <svg width="20" height="20" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"></path>
                    </svg>
                </a>
                <a href="/contact" class="btn btn-outline btn-lg">
                    Связаться с менеджером
                </a>
            </div>
        </div>
        <div class="cta-features">
            <div class="cta-feature">
                <svg width="24" height="24" fill="currentColor" viewBox="0 0 20 20">
                    <path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clip-rule="evenodd"></path>
                </svg>
                <span>Индивидуальные цены</span>
            </div>
            <div class="cta-feature">
                <svg width="24" height="24" fill="currentColor" viewBox="0 0 20 20">
                    <path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clip-rule="evenodd"></path>
                </svg>
                <span>Отсрочка платежа</span>
            </div>
            <div class="cta-feature">
                <svg width="24" height="24" fill="currentColor" viewBox="0 0 20 20">
                    <path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clip-rule="evenodd"></path>
                </svg>
                <span>Персональный менеджер</span>
            </div>
        </div>
    </div>
</section>

<!-- Новости и инструменты -->
<section class="info-section">
    <div class="container">
        <div class="info-grid">
            <!-- Новости -->
            <div class="info-card animate-slideInLeft">
                <div class="info-card-header">
                    <h3 class="info-card-title">
                        <svg width="24" height="24" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 20H5a2 2 0 01-2-2V6a2 2 0 012-2h10a2 2 0 012 2v1m2 13a2 2 0 01-2-2V7m2 13a2 2 0 002-2V9a2 2 0 00-2-2h-2m-4-3H9M7 16h6M7 8h6v4H7V8z"></path>
                        </svg>
                        Новости и акции
                    </h3>
                    <a href="/news" class="info-card-link">Все новости</a>
                </div>
                <div class="news-list">
                    <?php foreach ($news as $item): ?>
                    <article class="news-item">
                        <time class="news-date"><?= $item['date'] ?></time>
                        <h4 class="news-title"><?= htmlspecialchars($item['title']) ?></h4>
                        <p class="news-text"><?= htmlspecialchars($item['text']) ?></p>
                    </article>
                    <?php endforeach; ?>
                </div>
            </div>
            
            <!-- Инструменты -->
            <div class="info-card animate-slideInRight">
                <div class="info-card-header">
                    <h3 class="info-card-title">
                        <svg width="24" height="24" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 7h6m0 10v-3m-3 3h.01M9 17h.01M9 14h.01M12 14h.01M15 11h.01M12 11h.01M9 11h.01M7 21h10a2 2 0 002-2V5a2 2 0 00-2-2H7a2 2 0 00-2 2v14a2 2 0 002 2z"></path>
                        </svg>
                        Полезные инструменты
                    </h3>
                    <a href="/tools" class="info-card-link">Все инструменты</a>
                </div>
                <div class="tools-list">
                    <a href="/calculator/cable" class="tool-card">
                        <div class="tool-icon" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);">
                            <svg width="32" height="32" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 7h6m0 10v-3m-3 3h.01M9 17h.01M9 14h.01M12 14h.01M15 11h.01M12 11h.01M9 11h.01M7 21h10a2 2 0 002-2V5a2 2 0 00-2-2H7a2 2 0 00-2 2v14a2 2 0 002 2z"></path>
                            </svg>
                        </div>
                        <div class="tool-content">
                            <h4 class="tool-title">Калькулятор сечения кабеля</h4>
                            <p class="tool-description">Подберите правильное сечение для вашей нагрузки</p>
                        </div>
                        <svg class="tool-arrow" width="20" height="20" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path>
                        </svg>
                    </a>
                    
                    <a href="/calculator/power" class="tool-card">
                        <div class="tool-icon" style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);">
                            <svg width="32" height="32" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path>
                            </svg>
                        </div>
                        <div class="tool-content">
                            <h4 class="tool-title">Калькулятор мощности</h4>
                            <p class="tool-description">Рассчитайте необходимую мощность оборудования</p>
                        </div>
                        <svg class="tool-arrow" width="20" height="20" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path>
                        </svg>
                    </a>
                    
                    <a href="/specification/create" class="tool-card">
                        <div class="tool-icon" style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);">
                            <svg width="32" height="32" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path>
                            </svg>
                        </div>
                        <div class="tool-content">
                            <h4 class="tool-title">Создать спецификацию</h4>
                            <p class="tool-description">Быстрое формирование технического задания</p>
                        </div>
                        <svg class="tool-arrow" width="20" height="20" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path>
                        </svg>
                    </a>
                </div>
            </div>
        </div>
    </div>
</section>

<!-- Стили для главной страницы -->
<style>
/* Hero Section */
.hero-section {
    position: relative;
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    overflow: hidden;
    background: var(--bg-dark);
}

.hero-background {
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    z-index: 0;
}

.hero-particles {
    position: absolute;
    width: 100%;
    height: 100%;
}

.hero-gradient {
    position: absolute;
    width: 100%;
    height: 100%;
    background: radial-gradient(ellipse at center, transparent 0%, rgba(15, 23, 42, 0.8) 100%);
}

.hero-content {
    position: relative;
    z-index: 1;
    text-align: center;
    padding: 2rem;
    max-width: 1200px;
}

.hero-title {
    font-size: clamp(2.5rem, 8vw, 5rem);
    font-weight: 800;
    margin-bottom: 2rem;
    line-height: 1.1;
}

.hero-title-line {
    display: block;
    animation: slideInUp 0.8s ease-out forwards;
    opacity: 0;
}

.hero-title-line:nth-child(1) {
    animation-delay: 0.2s;
    color: white;
}

.hero-title-line:nth-child(2) {
    animation-delay: 0.4s;
}

.hero-title-line:nth-child(3) {
    animation-delay: 0.6s;
    color: white;
}

.gradient-text {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 20%, #f093fb 40%, #f5576c 60%, #4facfe 80%, #00f2fe 100%);
    background-size: 200% 200%;
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    animation: gradientShift 3s ease infinite;
}

@keyframes gradientShift {
    0% { background-position: 0% 50%; }
    50% { background-position: 100% 50%; }
    100% { background-position: 0% 50%; }
}

.hero-subtitle {
    font-size: clamp(1.125rem, 3vw, 1.5rem);
    color: rgba(255, 255, 255, 0.8);
    margin-bottom: 3rem;
    max-width: 800px;
    margin-left: auto;
    margin-right: auto;
    animation: fadeIn 1s ease-out 0.8s forwards;
    opacity: 0;
}

.hero-actions {
    display: flex;
    gap: 1.5rem;
    justify-content: center;
    margin-bottom: 4rem;
    flex-wrap: wrap;
    animation: fadeIn 1s ease-out 1s forwards;
    opacity: 0;
}

.hero-btn {
    position: relative;
    overflow: hidden;
    z-index: 1;
}

.hero-btn::before {
    content: '';
    position: absolute;
    top: 0;
    left: -100%;
    width: 100%;
    height: 100%;
    background: rgba(255, 255, 255, 0.1);
    transition: left 0.5s;
    z-index: -1;
}

.hero-btn:hover::before {
    left: 100%;
}

.hero-stats {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 3rem;
    max-width: 600px;
    margin: 0 auto;
    animation: fadeIn 1s ease-out 1.2s forwards;
    opacity: 0;
}

.hero-stat {
    text-align: center;
}

.hero-stat-value {
    font-size: 3rem;
    font-weight: 800;
    color: var(--primary);
    margin-bottom: 0.5rem;
    text-shadow: 0 0 30px rgba(1, 98, 65, 0.5);
}

.hero-stat-label {
    font-size: 1rem;
    color: rgba(255, 255, 255, 0.6);
    text-transform: uppercase;
    letter-spacing: 0.1em;
}

.hero-scroll-indicator {
    position: absolute;
    bottom: 2rem;
    left: 50%;
    transform: translateX(-50%);
    animation: bounce 2s infinite;
}

.hero-scroll-indicator svg {
    width: 32px;
    height: 32px;
    color: rgba(255, 255, 255, 0.5);
}

@keyframes bounce {
    0%, 20%, 50%, 80%, 100% {
        transform: translateX(-50%) translateY(0);
    }
    40% {
        transform: translateX(-50%) translateY(-10px);
    }
    60% {
        transform: translateX(-50%) translateY(-5px);
    }
}

/* Features Section */
.features-section {
    padding: 6rem 0;
    background: var(--bg-primary);
    position: relative;
}

.features-section::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 1px;
    background: linear-gradient(90deg, transparent, var(--primary), transparent);
}

.container {
    max-width: 1200px;
    margin: 0 auto;
    padding: 0 2rem;
}

.section-header {
    text-align: center;
    margin-bottom: 4rem;
}

.section-title {
    font-size: clamp(2rem, 5vw, 3rem);
    font-weight: 800;
    margin-bottom: 1rem;
    position: relative;
    display: inline-block;
}

.section-title::after {
    content: '';
    position: absolute;
    bottom: -10px;
    left: 50%;
    transform: translateX(-50%);
    width: 60px;
    height: 4px;
    background: var(--gradient-primary);
    border-radius: 2px;
}

.section-subtitle {
    font-size: 1.25rem;
    color: var(--text-secondary);
}

.features-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
    gap: 2rem;
}

.feature-card {
    background: var(--bg-primary);
    border-radius: var(--radius-xl);
    padding: 2.5rem;
    text-align: center;
    border: 1px solid var(--gray-200);
    transition: all var(--transition-base);
    position: relative;
    overflow: hidden;
}

.feature-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 4px;
    background: var(--gradient-primary);
    transform: scaleX(0);
    transition: transform var(--transition-base);
}

.feature-card:hover {
    transform: translateY(-10px);
    box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1);
}

.feature-card:hover::before {
    transform: scaleX(1);
}

.feature-card:hover .feature-icon {
    transform: scale(1.1) rotate(5deg);
}

.feature-card:hover .feature-icon-bg {
    transform: scale(1.5);
    opacity: 0.1;
}

.feature-icon {
    position: relative;
    width: 80px;
    height: 80px;
    margin: 0 auto 2rem;
    display: flex;
    align-items: center;
    justify-content: center;
    color: var(--primary);
    transition: all var(--transition-base);
}

.feature-icon-bg {
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    background: var(--gradient-primary);
    border-radius: 50%;
    opacity: 0.2;
    transition: all var(--transition-base);
}

.feature-title {
    font-size: 1.5rem;
    font-weight: 700;
    margin-bottom: 1rem;
    color: var(--text-primary);
}

.feature-text {
    color: var(--text-secondary);
    line-height: 1.8;
    margin-bottom: 1.5rem;
}

.feature-link {
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
    color: var(--primary);
    text-decoration: none;
    font-weight: 600;
    transition: all var(--transition-fast);
}

.feature-link:hover {
    gap: 1rem;
    color: var(--primary-dark);
}

/* Categories Section */
.categories-section {
    padding: 6rem 0;
    background: var(--bg-secondary);
}

.section-link {
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
    color: var(--primary);
    text-decoration: none;
    font-weight: 600;
    transition: all var(--transition-fast);
}

.section-link:hover {
    gap: 1rem;
}

.categories-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
    gap: 1.5rem;
    margin-top: 3rem;
}

.category-card {
    background: var(--bg-primary);
    border-radius: var(--radius-xl);
    padding: 2rem;
    text-align: center;
    text-decoration: none;
    color: var(--text-primary);
    transition: all var(--transition-base);
    position: relative;
    overflow: hidden;
    border: 2px solid transparent;
}

.category-card-bg {
    position: absolute;
    top: -50%;
    left: -50%;
    width: 200%;
    height: 200%;
    background: radial-gradient(circle, var(--primary-alpha-10) 0%, transparent 70%);
    transform: scale(0);
    transition: transform 0.6s ease;
}

.category-card:hover .category-card-bg {
    transform: scale(1);
}

.category-card:hover {
    transform: translateY(-5px);
    border-color: var(--primary);
    box-shadow: 0 10px 30px rgba(1, 98, 65, 0.15);
}

.category-card:hover .category-icon {
    transform: scale(1.2) rotate(10deg);
    color: var(--primary);
}

.category-icon {
    margin-bottom: 1rem;
    color: var(--primary);
    transition: all var(--transition-base);
}

.category-name {
    font-size: 1.125rem;
    font-weight: 600;
    margin-bottom: 0.5rem;
}

.category-count {
    font-size: 0.875rem;
    color: var(--text-secondary);
}

/* CTA Section */
.cta-section {
    position: relative;
    padding: 6rem 0;
    overflow: hidden;
}

.cta-bg {
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: var(--gradient-primary);
    z-index: 0;
}

.cta-pattern {
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background-image: url("data:image/svg+xml,%3Csvg width='60' height='60' viewBox='0 0 60 60' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='none' fill-rule='evenodd'%3E%3Cg fill='%23ffffff' fill-opacity='0.05'%3E%3Cpath d='M36 34v-4h-2v4h-4v2h4v4h2v-4h4v-2h-4zm0-30V0h-2v4h-4v2h4v4h2V6h4V4h-4zM6 34v-4H4v4H0v2h4v4h2v-4h4v-2H6zM6 4V0H4v4H0v2h4v4h2V6h4V4H6z'/%3E%3C/g%3E%3C/g%3E%3C/svg%3E");
}

.cta-content {
    position: relative;
    z-index: 1;
    text-align: center;
    color: white;
}

.cta-title {
    font-size: clamp(2rem, 5vw, 3rem);
    font-weight: 800;
    margin-bottom: 1rem;
}

.cta-subtitle {
    font-size: 1.25rem;
    margin-bottom: 2.5rem;
    opacity: 0.9;
}

.cta-actions {
    display: flex;
    gap: 1.5rem;
    justify-content: center;
    flex-wrap: wrap;
    margin-bottom: 3rem;
}

.cta-features {
    display: flex;
    justify-content: center;
    gap: 3rem;
    flex-wrap: wrap;
}

.cta-feature {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    color: white;
    font-weight: 500;
}

/* Info Section */
.info-section {
    padding: 6rem 0;
    background: var(--bg-primary);
}

.info-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
    gap: 3rem;
}

.info-card {
    background: var(--bg-primary);
    border-radius: var(--radius-xl);
    border: 1px solid var(--gray-200);
    overflow: hidden;
    box-shadow: var(--shadow-md);
}

.info-card-header {
    padding: 2rem;
    background: var(--bg-tertiary);
    display: flex;
    justify-content: space-between;
    align-items: center;
}

.info-card-title {
    font-size: 1.5rem;
    font-weight: 700;
    display: flex;
    align-items: center;
    gap: 0.75rem;
}

.info-card-link {
    color: var(--primary);
    text-decoration: none;
    font-weight: 600;
    transition: all var(--transition-fast);
}

.info-card-link:hover {
    text-decoration: underline;
}

/* News List */
.news-list {
    padding: 2rem;
}

.news-item {
    padding-bottom: 1.5rem;
    margin-bottom: 1.5rem;
    border-bottom: 1px solid var(--gray-200);
}

.news-item:last-child {
    border-bottom: none;
    margin-bottom: 0;
    padding-bottom: 0;
}

.news-date {
    font-size: 0.875rem;
    color: var(--text-tertiary);
    font-weight: 500;
}

.news-title {
    font-size: 1.125rem;
    font-weight: 600;
    margin: 0.5rem 0;
    color: var(--text-primary);
}

.news-text {
    color: var(--text-secondary);
    line-height: 1.6;
}

/* Tools List */
.tools-list {
    padding: 1rem;
}

.tool-card {
    display: flex;
    align-items: center;
    gap: 1.5rem;
    padding: 1.5rem;
    text-decoration: none;
    color: var(--text-primary);
    border-radius: var(--radius-lg);
    transition: all var(--transition-base);
    position: relative;
    overflow: hidden;
}

.tool-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: var(--bg-tertiary);
    transform: scaleX(0);
    transform-origin: left;
    transition: transform var(--transition-base);
    z-index: -1;
}

.tool-card:hover::before {
    transform: scaleX(1);
}

.tool-card:hover {
    transform: translateX(10px);
}

.tool-card:hover .tool-icon {
    transform: scale(1.1) rotate(-5deg);
}

.tool-card:hover .tool-arrow {
    transform: translateX(5px);
}

.tool-icon {
    width: 64px;
    height: 64px;
    border-radius: var(--radius-lg);
    display: flex;
    align-items: center;
    justify-content: center;
    color: white;
    flex-shrink: 0;
    transition: all var(--transition-base);
    box-shadow: 0 8px 20px rgba(0, 0, 0, 0.15);
}

.tool-content {
    flex: 1;
}

.tool-title {
    font-size: 1.125rem;
    font-weight: 600;
    margin-bottom: 0.25rem;
}

.tool-description {
    font-size: 0.875rem;
    color: var(--text-secondary);
}

.tool-arrow {
    color: var(--text-tertiary);
    transition: all var(--transition-fast);
}

/* Responsive */
@media (max-width: 768px) {
    .hero-actions {
        flex-direction: column;
        width: 100%;
        padding: 0 1rem;
    }
    
    .hero-btn {
        width: 100%;
    }
    
    .hero-stats {
        grid-template-columns: 1fr;
        gap: 2rem;
    }
    
    .features-grid {
        grid-template-columns: 1fr;
    }
    
    .categories-grid {
        grid-template-columns: repeat(2, 1fr);
        gap: 1rem;
    }
    
    .category-card {
        padding: 1.5rem 1rem;
    }
    
    .cta-features {
        flex-direction: column;
        gap: 1rem;
    }
    
    .info-grid {
        grid-template-columns: 1fr;
    }
}

/* Counter Animation */
@keyframes countUp {
    from { opacity: 0; }
    to { opacity: 1; }
}
</style>

<!-- Скрипты для главной страницы -->
<script>
// Particles.js конфигурация
document.addEventListener('DOMContentLoaded', function() {
    // Анимация счетчиков
    const counters = document.querySelectorAll('.hero-stat-value');
    const speed = 200;
    
    const animateCounter = (counter) => {
        const target = +counter.getAttribute('data-count');
        const count = +counter.innerText;
        const inc = target / speed;
        
        if (count < target) {
            counter.innerText = Math.ceil(count + inc);
            setTimeout(() => animateCounter(counter), 1);
        } else {
            counter.innerText = target.toLocaleString('ru-RU');
        }
    };
    
    // Intersection Observer для запуска анимации
    const observerOptions = {
        threshold: 0.5,
        rootMargin: '0px'
    };
    
    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const counter = entry.target;
                animateCounter(counter);
                observer.unobserve(counter);
            }
        });
    }, observerOptions);
    
    counters.forEach(counter => {
        observer.observe(counter);
    });
    
    // Плавный скролл к секциям
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function (e) {
            e.preventDefault();
            const target = document.querySelector(this.getAttribute('href'));
            if (target) {
                target.scrollIntoView({
                    behavior: 'smooth',
                    block: 'start'
                });
            }
        });
    });
    
    // Параллакс эффект для hero
    window.addEventListener('scroll', () => {
        const scrolled = window.pageYOffset;
        const parallax = document.querySelector('.hero-background');
        if (parallax) {
            parallax.style.transform = `translateY(${scrolled * 0.5}px)`;
        }
    });
});

// Простая анимация частиц (если не подключен particles.js)
function createParticles() {
    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d');
    const particlesContainer = document.getElementById('particles-js');
    
    if (!particlesContainer) return;
    
    particlesContainer.appendChild(canvas);
    
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;
    
    const particles = [];
    const particleCount = 50;
    
    class Particle {
        constructor() {
            this.x = Math.random() * canvas.width;
            this.y = Math.random() * canvas.height;
            this.size = Math.random() * 3 + 1;
            this.speedX = Math.random() * 3 - 1.5;
            this.speedY = Math.random() * 3 - 1.5;
            this.opacity = Math.random() * 0.5 + 0.2;
        }
        
        update() {
            this.x += this.speedX;
            this.y += this.speedY;
            
            if (this.x > canvas.width) this.x = 0;
            if (this.x < 0) this.x = canvas.width;
            if (this.y > canvas.height) this.y = 0;
            if (this.y < 0) this.y = canvas.height;
        }
        
        draw() {
            ctx.fillStyle = `rgba(255, 255, 255, ${this.opacity})`;
            ctx.beginPath();
            ctx.arc(this.x, this.y, this.size, 0, Math.PI * 2);
            ctx.fill();
        }
    }
    
    for (let i = 0; i < particleCount; i++) {
        particles.push(new Particle());
    }
    
    function animate() {
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        
        particles.forEach(particle => {
            particle.update();
            particle.draw();
        });
        
        // Рисуем линии между близкими частицами
        particles.forEach((particle, i) => {
            particles.slice(i + 1).forEach(otherParticle => {
                const distance = Math.sqrt(
                    Math.pow(particle.x - otherParticle.x, 2) +
                    Math.pow(particle.y - otherParticle.y, 2)
                );
                
                if (distance < 120) {
                    ctx.strokeStyle = `rgba(255, 255, 255, ${0.2 * (1 - distance / 120)})`;
                    ctx.lineWidth = 1;
                    ctx.beginPath();
                    ctx.moveTo(particle.x, particle.y);
                    ctx.lineTo(otherParticle.x, otherParticle.y);
                    ctx.stroke();
                }
            });
        });
        
        requestAnimationFrame(animate);
    }
    
    animate();
    
    // Resize handler
    window.addEventListener('resize', () => {
        canvas.width = window.innerWidth;
        canvas.height = window.innerHeight;
    });
}

// Запускаем частицы
createParticles();
</script>