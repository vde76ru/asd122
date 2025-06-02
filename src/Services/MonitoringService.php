<?php
// src/Services/MonitoringService.php
namespace App\Services;

use App\Core\Database;
use App\Core\Cache;
use App\Core\Logger;
use App\Core\Config;
use OpenSearch\ClientBuilder;

/**
 * Комплексная система мониторинга и тестирования
 * Проверяет все компоненты системы и собирает метрики
 */
class MonitoringService
{
    private array $results = [];
    private float $startTime;
    private array $errors = [];
    private array $warnings = [];
    
    public function __construct()
    {
        $this->startTime = microtime(true);
    }
    
    /**
     * Запустить полную проверку системы
     */
    public function runFullCheck(): array
    {
        Logger::info('Starting system monitoring check');
        
        // 1. Базовые проверки
        $this->checkPHPConfiguration();
        $this->checkFileSystem();
        $this->checkMemoryUsage();
        
        // 2. Проверка подключений
        $this->checkDatabase();
        $this->checkOpenSearch();
        $this->checkCache();
        
        // 3. Проверка API
        $this->checkSearchAPI();
        $this->checkAvailabilityAPI();
        $this->checkCartAPI();
        
        // 4. Проверка производительности
        $this->checkDatabasePerformance();
        $this->checkSearchPerformance();
        
        // 5. Проверка данных
        $this->checkDataIntegrity();
        $this->checkProductsData();
        
        // 6. Проверка безопасности
        $this->checkSecurity();
        
        // 7. Проверка очередей и задач
        $this->checkQueues();
        
        // Финальный отчет
        return $this->generateReport();
    }
    
    /**
     * 1. Проверка конфигурации PHP
     */
    private function checkPHPConfiguration(): void
    {
        $checks = [
            'version' => [
                'value' => PHP_VERSION,
                'required' => '7.4.0',
                'check' => version_compare(PHP_VERSION, '7.4.0', '>=')
            ],
            'memory_limit' => [
                'value' => ini_get('memory_limit'),
                'required' => '256M',
                'check' => $this->parseMemoryLimit(ini_get('memory_limit')) >= 268435456
            ],
            'max_execution_time' => [
                'value' => ini_get('max_execution_time'),
                'required' => '300',
                'check' => (int)ini_get('max_execution_time') >= 300 || ini_get('max_execution_time') == 0
            ],
            'post_max_size' => [
                'value' => ini_get('post_max_size'),
                'required' => '32M',
                'check' => $this->parseMemoryLimit(ini_get('post_max_size')) >= 33554432
            ],
            'upload_max_filesize' => [
                'value' => ini_get('upload_max_filesize'),
                'required' => '32M',
                'check' => $this->parseMemoryLimit(ini_get('upload_max_filesize')) >= 33554432
            ]
        ];
        
        $this->results['php_configuration'] = [
            'status' => 'checked',
            'checks' => $checks,
            'passed' => array_reduce($checks, fn($carry, $item) => $carry && $item['check'], true)
        ];
        
        foreach ($checks as $name => $check) {
            if (!$check['check']) {
                $this->warnings[] = "PHP {$name} is {$check['value']}, recommended: {$check['required']}";
            }
        }
    }
    
    /**
     * 2. Проверка файловой системы
     */
    private function checkFileSystem(): void
    {
        $paths = [
            'logs' => '/var/log/vdestor',
            'cache' => '/tmp/vdestor_cache',
            'uploads' => $_SERVER['DOCUMENT_ROOT'] . '/uploads',
            'assets' => $_SERVER['DOCUMENT_ROOT'] . '/assets/dist'
        ];
        
        $results = [];
        foreach ($paths as $name => $path) {
            $exists = file_exists($path);
            $writable = $exists && is_writable($path);
            
            $results[$name] = [
                'path' => $path,
                'exists' => $exists,
                'writable' => $writable
            ];
            
            if (!$exists) {
                $this->warnings[] = "Directory {$name} does not exist: {$path}";
            } elseif (!$writable) {
                $this->errors[] = "Directory {$name} is not writable: {$path}";
            }
        }
        
        // Проверка свободного места
        $freeSpace = disk_free_space('/');
        $totalSpace = disk_total_space('/');
        $usedPercent = round((($totalSpace - $freeSpace) / $totalSpace) * 100, 2);
        
        $results['disk_space'] = [
            'free' => $this->formatBytes($freeSpace),
            'total' => $this->formatBytes($totalSpace),
            'used_percent' => $usedPercent
        ];
        
        if ($usedPercent > 90) {
            $this->errors[] = "Disk space critically low: {$usedPercent}% used";
        } elseif ($usedPercent > 80) {
            $this->warnings[] = "Disk space warning: {$usedPercent}% used";
        }
        
        $this->results['filesystem'] = $results;
    }
    
    /**
     * 3. Проверка использования памяти
     */
    private function checkMemoryUsage(): void
    {
        $currentUsage = memory_get_usage(true);
        $peakUsage = memory_get_peak_usage(true);
        $limit = $this->parseMemoryLimit(ini_get('memory_limit'));
        
        $usagePercent = $limit > 0 ? round(($currentUsage / $limit) * 100, 2) : 0;
        $peakPercent = $limit > 0 ? round(($peakUsage / $limit) * 100, 2) : 0;
        
        $this->results['memory'] = [
            'current' => $this->formatBytes($currentUsage),
            'peak' => $this->formatBytes($peakUsage),
            'limit' => $this->formatBytes($limit),
            'usage_percent' => $usagePercent,
            'peak_percent' => $peakPercent
        ];
        
        if ($usagePercent > 80) {
            $this->warnings[] = "High memory usage: {$usagePercent}%";
        }
    }
    
    /**
     * 4. Проверка базы данных
     */
    private function checkDatabase(): void
    {
        $startTime = microtime(true);
        
        try {
            $pdo = Database::getConnection();
            
            // Проверяем подключение
            $stmt = $pdo->query("SELECT VERSION() as version, CONNECTION_ID() as conn_id");
            $info = $stmt->fetch();
            
            // Проверяем статус
            $stmt = $pdo->query("SHOW STATUS LIKE 'Threads_connected'");
            $connections = $stmt->fetch();
            
            // Проверяем размер БД
            $stmt = $pdo->query("
                SELECT 
                    SUM(data_length + index_length) as size,
                    COUNT(*) as tables_count
                FROM information_schema.tables 
                WHERE table_schema = DATABASE()
            ");
            $dbSize = $stmt->fetch();
            
            // Проверяем важные таблицы
            $requiredTables = [
                'products', 'users', 'carts', 'prices', 'stock_balances',
                'categories', 'brands', 'series', 'cities', 'warehouses'
            ];
            
            $existingTables = [];
            $stmt = $pdo->query("SHOW TABLES");
            while ($row = $stmt->fetch(\PDO::FETCH_NUM)) {
                $existingTables[] = $row[0];
            }
            
            $missingTables = array_diff($requiredTables, $existingTables);
            
            $this->results['database'] = [
                'status' => 'connected',
                'version' => $info['version'],
                'connection_id' => $info['conn_id'],
                'active_connections' => $connections['Value'] ?? 0,
                'database_size' => $this->formatBytes($dbSize['size'] ?? 0),
                'tables_count' => $dbSize['tables_count'] ?? 0,
                'missing_tables' => $missingTables,
                'response_time' => round((microtime(true) - $startTime) * 1000, 2) . 'ms'
            ];
            
            if (!empty($missingTables)) {
                $this->errors[] = "Missing database tables: " . implode(', ', $missingTables);
            }
            
        } catch (\Exception $e) {
            $this->results['database'] = [
                'status' => 'error',
                'error' => $e->getMessage()
            ];
            $this->errors[] = "Database connection failed: " . $e->getMessage();
        }
    }
    
    /**
     * 5. Проверка OpenSearch
     */
    private function checkOpenSearch(): void
    {
        $startTime = microtime(true);
        
        try {
            $client = ClientBuilder::create()
                ->setHosts(['localhost:9200'])
                ->setConnectionParams([
                    'timeout' => 5,
                    'connect_timeout' => 3
                ])
                ->build();
            
            // Проверяем здоровье кластера
            $health = $client->cluster()->health();
            
            // Проверяем индексы
            $indices = $client->indices()->stats(['index' => 'products*']);
            
            // Проверяем алиас
            $aliases = [];
            try {
                $aliasInfo = $client->indices()->getAlias(['name' => 'products_current']);
                $aliases = array_keys($aliasInfo);
            } catch (\Exception $e) {
                // Алиас может не существовать
            }
            
            $this->results['opensearch'] = [
                'status' => 'connected',
                'cluster_status' => $health['status'],
                'cluster_name' => $health['cluster_name'],
                'nodes_count' => $health['number_of_nodes'],
                'active_shards' => $health['active_shards'],
                'indices_count' => count($indices['indices'] ?? []),
                'current_alias' => $aliases,
                'response_time' => round((microtime(true) - $startTime) * 1000, 2) . 'ms'
            ];
            
            if ($health['status'] === 'red') {
                $this->errors[] = "OpenSearch cluster status is RED";
            } elseif ($health['status'] === 'yellow') {
                $this->warnings[] = "OpenSearch cluster status is YELLOW";
            }
            
            if (empty($aliases)) {
                $this->warnings[] = "OpenSearch alias 'products_current' not found";
            }
            
        } catch (\Exception $e) {
            $this->results['opensearch'] = [
                'status' => 'error',
                'error' => $e->getMessage()
            ];
            $this->warnings[] = "OpenSearch not available: " . $e->getMessage();
        }
    }
    
    /**
     * 6. Проверка кеша
     */
    private function checkCache(): void
    {
        try {
            $testKey = 'monitor_test_' . time();
            $testValue = 'test_value_' . uniqid();
            
            // Тест записи
            $writeResult = Cache::set($testKey, $testValue, 60);
            
            // Тест чтения
            $readValue = Cache::get($testKey);
            
            // Тест удаления
            $deleteResult = Cache::delete($testKey);
            
            // Получаем статистику
            $stats = Cache::getStats();
            
            $this->results['cache'] = [
                'status' => 'working',
                'write_test' => $writeResult,
                'read_test' => $readValue === $testValue,
                'delete_test' => $deleteResult,
                'stats' => $stats
            ];
            
            if (!$writeResult || $readValue !== $testValue) {
                $this->warnings[] = "Cache system not working properly";
            }
            
        } catch (\Exception $e) {
            $this->results['cache'] = [
                'status' => 'error',
                'error' => $e->getMessage()
            ];
            $this->warnings[] = "Cache error: " . $e->getMessage();
        }
    }
    
    /**
     * 7. Проверка Search API
     */
    private function checkSearchAPI(): void
    {
        $startTime = microtime(true);
        
        try {
            // Тест пустого поиска
            $emptySearch = SearchService::search([
                'q' => '',
                'page' => 1,
                'limit' => 1,
                'city_id' => 1
            ]);
            
            // Тест поиска по артикулу
            $articleSearch = SearchService::search([
                'q' => 'TEST123',
                'page' => 1,
                'limit' => 1,
                'city_id' => 1
            ]);
            
            // Тест автодополнения
            $autocomplete = SearchService::autocomplete('авт', 5);
            
            $this->results['search_api'] = [
                'status' => 'working',
                'empty_search' => [
                    'success' => $emptySearch['success'] ?? false,
                    'source' => $emptySearch['data']['source'] ?? 'unknown',
                    'total' => $emptySearch['data']['total'] ?? 0
                ],
                'article_search' => [
                    'success' => $articleSearch['success'] ?? false,
                    'source' => $articleSearch['data']['source'] ?? 'unknown'
                ],
                'autocomplete' => [
                    'working' => is_array($autocomplete),
                    'results_count' => count($autocomplete)
                ],
                'response_time' => round((microtime(true) - $startTime) * 1000, 2) . 'ms'
            ];
            
        } catch (\Exception $e) {
            $this->results['search_api'] = [
                'status' => 'error',
                'error' => $e->getMessage()
            ];
            $this->errors[] = "Search API error: " . $e->getMessage();
        }
    }
    
    /**
     * 8. Проверка Availability API
     */
    private function checkAvailabilityAPI(): void
    {
        try {
            // Получаем несколько случайных товаров для теста
            $stmt = Database::query("SELECT product_id FROM products ORDER BY RAND() LIMIT 5");
            $productIds = $stmt->fetchAll(\PDO::FETCH_COLUMN);
            
            if (empty($productIds)) {
                $this->results['availability_api'] = [
                    'status' => 'no_products',
                    'message' => 'No products found for testing'
                ];
                return;
            }
            
            $startTime = microtime(true);
            
            $dynamicService = new DynamicProductDataService();
            $data = $dynamicService->getProductsDynamicData($productIds, 1, null);
            
            $validResults = 0;
            foreach ($data as $productData) {
                if (isset($productData['price']) && isset($productData['stock'])) {
                    $validResults++;
                }
            }
            
            $this->results['availability_api'] = [
                'status' => 'working',
                'tested_products' => count($productIds),
                'valid_results' => $validResults,
                'response_time' => round((microtime(true) - $startTime) * 1000, 2) . 'ms'
            ];
            
        } catch (\Exception $e) {
            $this->results['availability_api'] = [
                'status' => 'error',
                'error' => $e->getMessage()
            ];
            $this->errors[] = "Availability API error: " . $e->getMessage();
        }
    }
    
    /**
     * 9. Проверка Cart API
     */
    private function checkCartAPI(): void
    {
        try {
            // Проверяем сессию
            if (session_status() !== PHP_SESSION_ACTIVE) {
                $this->results['cart_api'] = [
                    'status' => 'error',
                    'error' => 'Session not active'
                ];
                return;
            }
            
            // Тест получения корзины
            $cart = CartService::get();
            
            $this->results['cart_api'] = [
                'status' => 'working',
                'cart_items' => count($cart),
                'session_active' => true
            ];
            
        } catch (\Exception $e) {
            $this->results['cart_api'] = [
                'status' => 'error',
                'error' => $e->getMessage()
            ];
            $this->warnings[] = "Cart API error: " . $e->getMessage();
        }
    }
    
    /**
     * 10. Проверка производительности БД
     */
    private function checkDatabasePerformance(): void
    {
        try {
            $queries = [
                'simple_select' => "SELECT 1",
                'products_count' => "SELECT COUNT(*) FROM products",
                'complex_join' => "
                    SELECT COUNT(*) 
                    FROM products p
                    JOIN brands b ON p.brand_id = b.brand_id
                    JOIN product_categories pc ON p.product_id = pc.product_id
                ",
                'indexed_search' => "SELECT * FROM products WHERE external_id = 'TEST' LIMIT 1"
            ];
            
            $results = [];
            foreach ($queries as $name => $sql) {
                $startTime = microtime(true);
                try {
                    $stmt = Database::query($sql);
                    $stmt->fetchAll();
                    $results[$name] = round((microtime(true) - $startTime) * 1000, 2);
                } catch (\Exception $e) {
                    $results[$name] = 'error';
                }
            }
            
            $this->results['database_performance'] = $results;
            
            // Проверяем медленные запросы
            foreach ($results as $name => $time) {
                if (is_numeric($time) && $time > 100) {
                    $this->warnings[] = "Slow database query '{$name}': {$time}ms";
                }
            }
            
        } catch (\Exception $e) {
            $this->results['database_performance'] = [
                'status' => 'error',
                'error' => $e->getMessage()
            ];
        }
    }
    
    /**
     * 11. Проверка производительности поиска
     */
    private function checkSearchPerformance(): void
    {
        $testQueries = [
            'single_word' => 'автомат',
            'multiple_words' => 'автоматический выключатель',
            'article' => 'ABB-S201',
            'with_typo' => 'афтомат',
            'brand' => 'schneider'
        ];
        
        $results = [];
        foreach ($testQueries as $type => $query) {
            $startTime = microtime(true);
            try {
                $result = SearchService::search([
                    'q' => $query,
                    'page' => 1,
                    'limit' => 10,
                    'city_id' => 1
                ]);
                
                $results[$type] = [
                    'time' => round((microtime(true) - $startTime) * 1000, 2),
                    'found' => $result['data']['total'] ?? 0,
                    'source' => $result['data']['source'] ?? 'unknown'
                ];
            } catch (\Exception $e) {
                $results[$type] = ['error' => $e->getMessage()];
            }
        }
        
        $this->results['search_performance'] = $results;
    }
    
    /**
     * 12. Проверка целостности данных
     */
    private function checkDataIntegrity(): void
    {
        try {
            $checks = [];
            
            // Проверка товаров без цен
            $stmt = Database::query("
                SELECT COUNT(*) FROM products p
                LEFT JOIN prices pr ON p.product_id = pr.product_id AND pr.is_base = 1
                WHERE pr.price_id IS NULL
            ");
            $checks['products_without_prices'] = (int)$stmt->fetchColumn();
            
            // Проверка товаров без остатков
            $stmt = Database::query("
                SELECT COUNT(DISTINCT p.product_id) FROM products p
                LEFT JOIN stock_balances sb ON p.product_id = sb.product_id
                WHERE sb.product_id IS NULL
            ");
            $checks['products_without_stock'] = (int)$stmt->fetchColumn();
            
            // Проверка товаров без категорий
            $stmt = Database::query("
                SELECT COUNT(*) FROM products p
                LEFT JOIN product_categories pc ON p.product_id = pc.product_id
                WHERE pc.product_id IS NULL
            ");
            $checks['products_without_categories'] = (int)$stmt->fetchColumn();
            
            // Проверка дубликатов артикулов
            $stmt = Database::query("
                SELECT COUNT(*) FROM (
                    SELECT external_id, COUNT(*) as cnt 
                    FROM products 
                    GROUP BY external_id 
                    HAVING cnt > 1
                ) as duplicates
            ");
            $checks['duplicate_external_ids'] = (int)$stmt->fetchColumn();
            
            $this->results['data_integrity'] = $checks;
            
            // Генерируем предупреждения
            if ($checks['products_without_prices'] > 0) {
                $this->warnings[] = "Found {$checks['products_without_prices']} products without prices";
            }
            if ($checks['duplicate_external_ids'] > 0) {
                $this->errors[] = "Found {$checks['duplicate_external_ids']} duplicate external IDs";
            }
            
        } catch (\Exception $e) {
            $this->results['data_integrity'] = [
                'status' => 'error',
                'error' => $e->getMessage()
            ];
        }
    }
    
    /**
     * 13. Проверка данных товаров
     */
    private function checkProductsData(): void
    {
        try {
            $stmt = Database::query("
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN name IS NULL OR name = '' THEN 1 ELSE 0 END) as without_name,
                    SUM(CASE WHEN external_id IS NULL OR external_id = '' THEN 1 ELSE 0 END) as without_external_id,
                    SUM(CASE WHEN brand_id IS NULL THEN 1 ELSE 0 END) as without_brand,
                    SUM(CASE WHEN LENGTH(description) < 10 THEN 1 ELSE 0 END) as short_description
                FROM products
            ");
            
            $stats = $stmt->fetch();
            
            $this->results['products_quality'] = $stats;
            
            if ($stats['without_name'] > 0) {
                $this->errors[] = "Found {$stats['without_name']} products without names";
            }
            if ($stats['without_external_id'] > 0) {
                $this->errors[] = "Found {$stats['without_external_id']} products without external IDs";
            }
            
        } catch (\Exception $e) {
            $this->results['products_quality'] = [
                'status' => 'error',
                'error' => $e->getMessage()
            ];
        }
    }
    
    /**
     * 14. Проверка безопасности
     */
    private function checkSecurity(): void
    {
        $checks = [];
        
        // Проверка HTTPS
        $checks['https'] = (!empty($_SERVER['HTTPS']) && $_SERVER['HTTPS'] !== 'off');
        
        // Проверка заголовков безопасности
        $headers = array_change_key_case(getallheaders(), CASE_LOWER);
        $checks['security_headers'] = [
            'x-content-type-options' => isset($headers['x-content-type-options']),
            'x-frame-options' => isset($headers['x-frame-options']),
            'x-xss-protection' => isset($headers['x-xss-protection']),
            'strict-transport-security' => isset($headers['strict-transport-security'])
        ];
        
        // Проверка конфигурации
        $configIssues = Config::validateSecurity();
        $checks['config_issues'] = $configIssues;
        
        $this->results['security'] = $checks;
        
        if (!$checks['https']) {
            $this->warnings[] = "HTTPS is not enabled";
        }
        
        foreach ($checks['security_headers'] as $header => $present) {
            if (!$present) {
                $this->warnings[] = "Security header missing: {$header}";
            }
        }
        
        if (!empty($configIssues)) {
            foreach ($configIssues as $issue) {
                $this->warnings[] = "Config security: {$issue}";
            }
        }
    }
    
    /**
     * 15. Проверка очередей
     */
    private function checkQueues(): void
    {
        try {
            $stats = QueueService::getStats();
            
            $pendingJobs = $stats['by_status']['pending']['count'] ?? 0;
            $failedJobs = $stats['by_status']['failed']['count'] ?? 0;
            
            $this->results['queues'] = [
                'stats' => $stats,
                'queue_length' => $stats['queue_length'] ?? 0
            ];
            
            if ($pendingJobs > 1000) {
                $this->warnings[] = "High queue length: {$pendingJobs} pending jobs";
            }
            
            if ($failedJobs > 100) {
                $this->errors[] = "Many failed jobs in queue: {$failedJobs}";
            }
            
        } catch (\Exception $e) {
            $this->results['queues'] = [
                'status' => 'error',
                'error' => $e->getMessage()
            ];
        }
    }
    
    /**
     * Генерация финального отчета
     */
    private function generateReport(): array
    {
        $executionTime = microtime(true) - $this->startTime;
        
        return [
            'timestamp' => date('Y-m-d H:i:s'),
            'execution_time' => round($executionTime, 3) . 's',
            'status' => $this->calculateOverallStatus(),
            'errors_count' => count($this->errors),
            'warnings_count' => count($this->warnings),
            'errors' => $this->errors,
            'warnings' => $this->warnings,
            'results' => $this->results,
            'system_load' => sys_getloadavg()
        ];
    }
    
    /**
     * Рассчитать общий статус системы
     */
    private function calculateOverallStatus(): string
    {
        if (!empty($this->errors)) {
            return 'critical';
        }
        if (!empty($this->warnings)) {
            return 'warning';
        }
        return 'healthy';
    }
    
    /**
     * Утилиты
     */
    private function parseMemoryLimit(string $limit): int
    {
        $limit = trim($limit);
        if ($limit === '-1') {
            return PHP_INT_MAX;
        }
        
        $last = strtolower($limit[strlen($limit) - 1]);
        $value = (int)$limit;
        
        switch ($last) {
            case 'g': 
                $value *= 1024 * 1024 * 1024; 
                break;
            case 'm': 
                $value *= 1024 * 1024; 
                break;
            case 'k': 
                $value *= 1024; 
                break;
        }
        
        return $value;
    }
    
    private function formatBytes(int $bytes): string
    {
        $units = ['B', 'KB', 'MB', 'GB', 'TB'];
        $bytes = max($bytes, 0);
        $pow = floor(($bytes ? log($bytes) : 0) / log(1024));
        $pow = min($pow, count($units) - 1);
        $bytes /= pow(1024, $pow);
        
        return round($bytes, 2) . ' ' . $units[$pow];
    }
}

// Контроллер для веб-интерфейса мониторинга
namespace App\Controllers;

use App\Services\MonitoringService;
use App\Services\AuthService;

class MonitoringController extends BaseController
{
    /**
     * GET /api/monitoring/check - Запустить полную проверку
     */
    public function checkAction(): void
    {
        // Только для администраторов
        $this->requireRole('admin');
        
        $monitor = new MonitoringService();
        $report = $monitor->runFullCheck();
        
        $this->success($report);
    }
    
    /**
     * GET /api/monitoring/health - Быстрая проверка здоровья
     */
    public function healthAction(): void
    {
        $checks = [
            'database' => $this->checkDatabaseHealth(),
            'cache' => $this->checkCacheHealth(),
            'opensearch' => $this->checkOpenSearchHealth(),
            'memory' => $this->checkMemoryHealth()
        ];
        
        $healthy = array_reduce($checks, fn($carry, $item) => $carry && $item, true);
        
        if ($healthy) {
            $this->success([
                'status' => 'healthy',
                'checks' => $checks
            ]);
        } else {
            $this->error('System unhealthy', 503, ['checks' => $checks]);
        }
    }
    
    private function checkDatabaseHealth(): bool
    {
        try {
            \App\Core\Database::query("SELECT 1");
            return true;
        } catch (\Exception $e) {
            return false;
        }
    }
    
    private function checkCacheHealth(): bool
    {
        try {
            $key = 'health_check_' . time();
            \App\Core\Cache::set($key, true, 1);
            $result = \App\Core\Cache::get($key);
            \App\Core\Cache::delete($key);
            return $result === true;
        } catch (\Exception $e) {
            return false;
        }
    }
    
    private function checkOpenSearchHealth(): bool
    {
        try {
            $client = \OpenSearch\ClientBuilder::create()
                ->setHosts(['localhost:9200'])
                ->setConnectionParams(['timeout' => 3])
                ->build();
            
            $health = $client->cluster()->health(['timeout' => '2s']);
            return in_array($health['status'] ?? 'red', ['green', 'yellow']);
        } catch (\Exception $e) {
            return false;
        }
    }
    
    private function checkMemoryHealth(): bool
    {
        $usage = memory_get_usage(true);
        $limit = ini_get('memory_limit');
        
        if ($limit === '-1') {
            return true;
        }
        
        $limitBytes = $this->parseMemoryLimit($limit);
        return ($usage / $limitBytes) < 0.9; // Меньше 90%
    }
    
    private function parseMemoryLimit(string $limit): int
    {
        $limit = trim($limit);
        $last = strtolower($limit[strlen($limit) - 1]);
        $value = (int)$limit;
        
        switch ($last) {
            case 'g': $value *= 1024 * 1024 * 1024; break;
            case 'm': $value *= 1024 * 1024; break;
            case 'k': $value *= 1024; break;
        }
        
        return $value;
    }
}