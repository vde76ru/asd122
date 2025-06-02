<?php
namespace App\Services;

use App\Core\Database;
use App\Core\Cache;
use App\Core\Logger;

/**
 * Сервис для сбора и анализа метрик приложения
 * 
 * ВАЖНО: Этот сервис НЕ использует QueueService напрямую,
 * чтобы избежать циклических зависимостей
 */
class MetricsService
{
    // Типы метрик, которые мы собираем
    const METRIC_PAGE_VIEW = 'page_view';
    const METRIC_API_CALL = 'api_call';
    const METRIC_DB_QUERY = 'db_query';
    const METRIC_CACHE_HIT = 'cache_hit';
    const METRIC_CACHE_MISS = 'cache_miss';
    const METRIC_ERROR = 'error';
    const METRIC_CART_ACTION = 'cart_action';
    const METRIC_SEARCH = 'search';
    const METRIC_LOGIN = 'login';
    
    // Флаг для предотвращения рекурсии
    private static bool $isRecording = false;
    
    /**
     * Записать метрику
     * 
     * @param string $type    Тип метрики
     * @param array  $data    Дополнительные данные
     * @param float  $value   Числовое значение (например, время выполнения)
     */
    public static function record(string $type, array $data = [], float $value = 1.0): void
    {
        // Предотвращаем рекурсию
        if (self::$isRecording) {
            return;
        }
        
        self::$isRecording = true;
        
        try {
            // Добавляем общие данные
            $data = array_merge($data, [
                'timestamp' => microtime(true),
                'memory_usage' => memory_get_usage(true),
                'peak_memory' => memory_get_peak_usage(true),
                'user_id' => $_SESSION['user_id'] ?? null,
                'session_id' => session_id() ?: null,
                'ip' => $_SERVER['REMOTE_ADDR'] ?? null,
                'user_agent' => $_SERVER['HTTP_USER_AGENT'] ?? null,
                'request_uri' => $_SERVER['REQUEST_URI'] ?? null
            ]);
            
            // Сохраняем напрямую в БД без использования QueueService
            self::saveMetricDirectly($type, $data, $value);
            
            // Обновляем счетчики в кеше для быстрого доступа
            self::updateCounters($type, $value);
            
        } catch (\Exception $e) {
            // Не даем ошибкам метрик ломать основное приложение
            // Используем error_log вместо Logger чтобы избежать рекурсии
            error_log('Metrics recording failed: ' . $e->getMessage());
        } finally {
            self::$isRecording = false;
        }
    }
    
    /**
     * Сохранить метрику напрямую в БД
     */
    private static function saveMetricDirectly(string $type, array $data, float $value): void
    {
        try {
            Database::query(
                "INSERT INTO metrics (metric_type, data, value, created_at) 
                 VALUES (?, ?, ?, NOW())",
                [$type, json_encode($data, JSON_UNESCAPED_UNICODE), $value]
            );
        } catch (\Exception $e) {
            // Используем error_log вместо Logger чтобы избежать рекурсии
            error_log('Failed to save metric: ' . $e->getMessage());
        }
    }
    
    /**
     * Замерить время выполнения операции
     */
    public static function startTimer(): float
    {
        return microtime(true);
    }
    
    /**
     * Завершить замер времени и записать метрику
     */
    public static function endTimer(float $startTime, string $metricType, array $data = []): float
    {
        $duration = microtime(true) - $startTime;
        self::record($metricType, $data, $duration);
        return $duration;
    }
    
    /**
     * Получить статистику за период
     */
    public static function getStats(string $period = 'day'): array
    {
        // Пробуем получить из кеша
        $cacheKey = "metrics_stats_{$period}";
        $cached = Cache::get($cacheKey);
        if ($cached !== null) {
            return $cached;
        }
        
        // Вычисляем временные рамки
        $intervals = [
            'hour' => '-1 hour',
            'day' => '-1 day',
            'week' => '-7 days',
            'month' => '-30 days'
        ];
        
        $since = date('Y-m-d H:i:s', strtotime($intervals[$period] ?? '-1 day'));
        
        try {
            $stats = [
                'summary' => self::getSummaryStats($since),
                'performance' => self::getPerformanceStats($since),
                'errors' => self::getErrorStats($since),
                'top_pages' => self::getTopPages($since),
                'user_activity' => self::getUserActivityStats($since)
            ];
            
            // Кешируем на 5 минут
            Cache::set($cacheKey, $stats, 300);
            
            return $stats;
            
        } catch (\Exception $e) {
            error_log('Failed to get metrics stats: ' . $e->getMessage());
            return [];
        }
    }
    
    /**
     * Получить текущее состояние системы (для дашборда)
     */
    public static function getSystemHealth(): array
    {
        return [
            // Состояние базы данных
            'database' => [
                'status' => self::checkDatabaseHealth(),
                'query_count' => Database::getStats()['query_count'],
                'avg_query_time' => Database::getStats()['average_time']
            ],
            
            // Состояние кеша
            'cache' => [
                'status' => self::checkCacheHealth(),
                'hit_rate' => self::getCacheHitRate()
            ],
            
            // Использование памяти
            'memory' => [
                'current' => memory_get_usage(true),
                'peak' => memory_get_peak_usage(true),
                'limit' => ini_get('memory_limit')
            ],
            
            // Средняя нагрузка
            'load_average' => sys_getloadavg(),
            
            // Дисковое пространство
            'disk_space' => [
                'free' => disk_free_space('/'),
                'total' => disk_total_space('/')
            ]
        ];
    }
    
    /**
     * Очистить старые метрики
     * Запускать через cron раз в день
     */
    public static function cleanup(int $daysToKeep = 30): int
    {
        try {
            $cutoffDate = date('Y-m-d', strtotime("-{$daysToKeep} days"));
            
            $stmt = Database::query(
                "DELETE FROM metrics WHERE created_at < ?",
                [$cutoffDate]
            );
            
            $deleted = $stmt->rowCount();
            
            // Используем простое логирование без зависимостей
            error_log("Metrics cleanup completed: deleted {$deleted} records");
            
            return $deleted;
            
        } catch (\Exception $e) {
            error_log('Metrics cleanup failed: ' . $e->getMessage());
            return 0;
        }
    }
    
    // === Приватные методы ===
    
    /**
     * Обновить счетчики в кеше
     */
    private static function updateCounters(string $type, float $value): void
    {
        $key = "metric_counter_{$type}_" . date('Y-m-d-H');
        
        // Инкрементируем счетчик
        $current = Cache::get($key) ?: ['count' => 0, 'sum' => 0];
        $current['count']++;
        $current['sum'] += $value;
        
        Cache::set($key, $current, 3600); // Храним час
    }
    
    /**
     * Получить общую статистику
     */
    private static function getSummaryStats(string $since): array
    {
        $stmt = Database::query("
            SELECT 
                metric_type,
                COUNT(*) as count,
                AVG(value) as avg_value,
                MIN(value) as min_value,
                MAX(value) as max_value
            FROM metrics
            WHERE created_at >= ?
            GROUP BY metric_type
        ", [$since]);
        
        $result = [];
        while ($row = $stmt->fetch()) {
            $result[$row['metric_type']] = [
                'count' => (int)$row['count'],
                'average' => round((float)$row['avg_value'], 3),
                'min' => round((float)$row['min_value'], 3),
                'max' => round((float)$row['max_value'], 3)
            ];
        }
        
        return $result;
    }
    
    /**
     * Получить статистику производительности
     */
    private static function getPerformanceStats(string $since): array
    {
        return [
            'avg_page_load_time' => self::getAverageMetric(self::METRIC_PAGE_VIEW, $since),
            'avg_api_response_time' => self::getAverageMetric(self::METRIC_API_CALL, $since),
            'avg_db_query_time' => self::getAverageMetric(self::METRIC_DB_QUERY, $since),
            'cache_hit_rate' => self::getCacheHitRate($since)
        ];
    }
    
    /**
     * Получить среднее значение метрики
     */
    private static function getAverageMetric(string $type, string $since): float
    {
        $stmt = Database::query("
            SELECT AVG(value) as avg_value
            FROM metrics
            WHERE metric_type = ? AND created_at >= ?
        ", [$type, $since]);
        
        return round((float)$stmt->fetchColumn(), 3);
    }
    
    /**
     * Получить процент попаданий в кеш
     */
    private static function getCacheHitRate(string $since = null): float
    {
        if ($since === null) {
            $since = date('Y-m-d H:i:s', strtotime('-1 hour'));
        }
        
        $stmt = Database::query("
            SELECT 
                SUM(CASE WHEN metric_type = ? THEN 1 ELSE 0 END) as hits,
                SUM(CASE WHEN metric_type = ? THEN 1 ELSE 0 END) as misses
            FROM metrics
            WHERE metric_type IN (?, ?) AND created_at >= ?
        ", [
            self::METRIC_CACHE_HIT,
            self::METRIC_CACHE_MISS,
            self::METRIC_CACHE_HIT,
            self::METRIC_CACHE_MISS,
            $since
        ]);
        
        $data = $stmt->fetch();
        $total = ($data['hits'] ?? 0) + ($data['misses'] ?? 0);
        
        return $total > 0 ? round(($data['hits'] / $total) * 100, 2) : 0;
    }
    
    /**
     * Получить статистику ошибок
     */
    private static function getErrorStats(string $since): array
    {
        $stmt = Database::query("
            SELECT 
                JSON_EXTRACT(data, '$.error_type') as error_type,
                COUNT(*) as count
            FROM metrics
            WHERE metric_type = ? AND created_at >= ?
            GROUP BY error_type
            ORDER BY count DESC
            LIMIT 10
        ", [self::METRIC_ERROR, $since]);
        
        return $stmt->fetchAll();
    }
    
    /**
     * Получить топ посещаемых страниц
     */
    private static function getTopPages(string $since, int $limit = 10): array
    {
        $stmt = Database::query("
            SELECT 
                JSON_EXTRACT(data, '$.request_uri') as page,
                COUNT(*) as views,
                AVG(value) as avg_load_time
            FROM metrics
            WHERE metric_type = ? AND created_at >= ?
            GROUP BY page
            ORDER BY views DESC
            LIMIT ?
        ", [self::METRIC_PAGE_VIEW, $since, $limit]);
        
        return $stmt->fetchAll();
    }
    
    /**
     * Получить статистику активности пользователей
     */
    private static function getUserActivityStats(string $since): array
    {
        $stmt = Database::query("
            SELECT 
                DATE_FORMAT(created_at, '%Y-%m-%d %H:00:00') as hour,
                COUNT(DISTINCT JSON_EXTRACT(data, '$.user_id')) as unique_users,
                COUNT(DISTINCT JSON_EXTRACT(data, '$.session_id')) as sessions,
                COUNT(*) as actions
            FROM metrics
            WHERE created_at >= ?
            GROUP BY hour
            ORDER BY hour
        ", [$since]);
        
        return $stmt->fetchAll();
    }
    
    /**
     * Проверить состояние БД
     */
    private static function checkDatabaseHealth(): string
    {
        try {
            Database::query("SELECT 1");
            return 'healthy';
        } catch (\Exception $e) {
            return 'unhealthy';
        }
    }
    
    /**
     * Проверить состояние кеша
     */
    private static function checkCacheHealth(): string
    {
        try {
            $testKey = 'health_check_' . time();
            Cache::set($testKey, true, 1);
            $result = Cache::get($testKey);
            Cache::delete($testKey);
            
            return $result === true ? 'healthy' : 'unhealthy';
        } catch (\Exception $e) {
            return 'unhealthy';
        }
    }
}