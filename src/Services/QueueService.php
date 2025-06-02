<?php
// src/Services/QueueService.php
namespace App\Services;

use App\Core\Database;
use App\Core\Logger;

/**
 * Сервис для работы с очередями задач
 * 
 * Представьте очередь в магазине - люди стоят по порядку и обслуживаются
 * один за другим. Так же и здесь - задачи выполняются последовательно,
 * что позволяет не нагружать сервер и обрабатывать всё по порядку!
 */
class QueueService
{
    // Приоритеты задач (как VIP-очередь в аэропорту!)
    const PRIORITY_CRITICAL = 10;  // Критически важные задачи
    const PRIORITY_HIGH = 7;       // Высокий приоритет
    const PRIORITY_NORMAL = 5;     // Обычный приоритет
    const PRIORITY_LOW = 3;        // Низкий приоритет
    const PRIORITY_BACKGROUND = 1; // Фоновые задачи
    
    // Статусы задач
    const STATUS_PENDING = 'pending';       // Ожидает выполнения
    const STATUS_PROCESSING = 'processing'; // Выполняется сейчас
    const STATUS_COMPLETED = 'completed';   // Успешно выполнена
    const STATUS_FAILED = 'failed';         // Ошибка при выполнении
    
    // Максимальное количество попыток
    const MAX_ATTEMPTS = 3;
    
    /**
     * Добавить задачу в очередь
     * 
     * @param string $type     Тип задачи (email, metrics, import и т.д.)
     * @param array  $payload  Данные для обработки
     * @param int    $priority Приоритет задачи
     * @param int    $delay    Задержка в секундах перед выполнением
     * @return int ID созданной задачи
     */
    public static function push(string $type, array $payload, int $priority = self::PRIORITY_NORMAL, int $delay = 0): int
    {
        try {
            // Определяем время, когда задачу можно начать выполнять
            $availableAt = $delay > 0 ? date('Y-m-d H:i:s', time() + $delay) : date('Y-m-d H:i:s');
            
            // Сохраняем задачу в базу данных
            $stmt = Database::query(
                "INSERT INTO job_queue (type, payload, priority, status, available_at, created_at) 
                 VALUES (?, ?, ?, ?, ?, NOW())",
                [
                    $type,
                    json_encode($payload, JSON_UNESCAPED_UNICODE),
                    $priority,
                    self::STATUS_PENDING,
                    $availableAt
                ]
            );
            
            $jobId = Database::getConnection()->lastInsertId();
            
            Logger::info('Job added to queue', [
                'job_id' => $jobId,
                'type' => $type,
                'priority' => $priority
            ]);
            
            return $jobId;
            
        } catch (\Exception $e) {
            Logger::error('Failed to add job to queue', [
                'type' => $type,
                'error' => $e->getMessage()
            ]);
            throw new \RuntimeException('Не удалось добавить задачу в очередь');
        }
    }
    
    /**
     * Получить следующую задачу для обработки
     * 
     * Это как кассир зовет следующего в очереди!
     */
    public static function pop(array $types = []): ?array
    {
        try {
            $pdo = Database::getConnection();
            $pdo->beginTransaction();
            
            // Строим запрос с учетом типов задач
            $sql = "SELECT * FROM job_queue 
                    WHERE status = ? 
                    AND available_at <= NOW() 
                    AND attempts < ?";
            
            $params = [self::STATUS_PENDING, self::MAX_ATTEMPTS];
            
            if (!empty($types)) {
                $placeholders = implode(',', array_fill(0, count($types), '?'));
                $sql .= " AND type IN ($placeholders)";
                $params = array_merge($params, $types);
            }
            
            $sql .= " ORDER BY priority DESC, created_at ASC 
                     LIMIT 1 
                     FOR UPDATE SKIP LOCKED"; // Важно! Блокируем строку для других воркеров
            
            $stmt = $pdo->prepare($sql);
            $stmt->execute($params);
            
            $job = $stmt->fetch();
            
            if (!$job) {
                $pdo->commit();
                return null;
            }
            
            // Помечаем задачу как обрабатываемую
            $updateStmt = $pdo->prepare(
                "UPDATE job_queue 
                 SET status = ?, started_at = NOW(), attempts = attempts + 1 
                 WHERE id = ?"
            );
            $updateStmt->execute([self::STATUS_PROCESSING, $job['id']]);
            
            $pdo->commit();
            
            // Декодируем payload
            $job['payload'] = json_decode($job['payload'], true);
            
            return $job;
            
        } catch (\Exception $e) {
            if (isset($pdo) && $pdo->inTransaction()) {
                $pdo->rollBack();
            }
            
            Logger::error('Failed to pop job from queue', [
                'error' => $e->getMessage()
            ]);
            
            return null;
        }
    }
    
    /**
     * Пометить задачу как выполненную
     */
    public static function complete(int $jobId, array $result = []): void
    {
        try {
            Database::query(
                "UPDATE job_queue 
                 SET status = ?, completed_at = NOW(), result = ? 
                 WHERE id = ?",
                [
                    self::STATUS_COMPLETED,
                    json_encode($result, JSON_UNESCAPED_UNICODE),
                    $jobId
                ]
            );
            
            Logger::info('Job completed', ['job_id' => $jobId]);
            
        } catch (\Exception $e) {
            Logger::error('Failed to mark job as completed', [
                'job_id' => $jobId,
                'error' => $e->getMessage()
            ]);
        }
    }
    
    /**
     * Пометить задачу как проваленную
     */
    public static function fail(int $jobId, string $error, bool $retry = true): void
    {
        try {
            $job = self::getJob($jobId);
            if (!$job) return;
            
            // Если можно повторить и попытки не исчерпаны
            if ($retry && $job['attempts'] < self::MAX_ATTEMPTS) {
                // Возвращаем в очередь с задержкой
                $delay = pow(2, $job['attempts']) * 60; // Экспоненциальная задержка
                
                Database::query(
                    "UPDATE job_queue 
                     SET status = ?, last_error = ?, available_at = ? 
                     WHERE id = ?",
                    [
                        self::STATUS_PENDING,
                        $error,
                        date('Y-m-d H:i:s', time() + $delay),
                        $jobId
                    ]
                );
                
                Logger::warning('Job failed, will retry', [
                    'job_id' => $jobId,
                    'attempt' => $job['attempts'],
                    'retry_in' => $delay
                ]);
            } else {
                // Окончательный провал
                Database::query(
                    "UPDATE job_queue 
                     SET status = ?, failed_at = NOW(), last_error = ? 
                     WHERE id = ?",
                    [
                        self::STATUS_FAILED,
                        $error,
                        $jobId
                    ]
                );
                
                Logger::error('Job permanently failed', [
                    'job_id' => $jobId,
                    'error' => $error
                ]);
                
                // Отправляем уведомление администратору
                self::notifyAdminAboutFailure($job, $error);
            }
            
        } catch (\Exception $e) {
            Logger::error('Failed to mark job as failed', [
                'job_id' => $jobId,
                'error' => $e->getMessage()
            ]);
        }
    }
    
    /**
     * Обработать задачу определенного типа
     * 
     * Это основной метод, который вызывается из воркера очереди
     */
    public static function process(array $job): bool
    {
        try {
            // Определяем обработчик по типу задачи
            $handler = self::getHandler($job['type']);
            
            if (!$handler) {
                throw new \RuntimeException("Нет обработчика для типа задачи: {$job['type']}");
            }
            
            // Вызываем обработчик
            $result = call_user_func($handler, $job['payload']);
            
            // Помечаем как выполненную
            self::complete($job['id'], ['result' => $result]);
            
            return true;
            
        } catch (\Exception $e) {
            // Помечаем как проваленную
            self::fail($job['id'], $e->getMessage());
            return false;
        }
    }
    
    /**
     * Получить статистику очереди
     */
    public static function getStats(): array
    {
        try {
            $stats = [];
            
            // Общая статистика
            $stmt = Database::query("
                SELECT 
                    status,
                    COUNT(*) as count,
                    AVG(TIMESTAMPDIFF(SECOND, created_at, completed_at)) as avg_processing_time
                FROM job_queue
                GROUP BY status
            ");
            
            while ($row = $stmt->fetch()) {
                $stats['by_status'][$row['status']] = [
                    'count' => (int)$row['count'],
                    'avg_time' => $row['avg_processing_time'] ? round($row['avg_processing_time'], 2) : null
                ];
            }
            
            // По типам задач
            $stmt = Database::query("
                SELECT 
                    type,
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
                FROM job_queue
                GROUP BY type
            ");
            
            while ($row = $stmt->fetch()) {
                $stats['by_type'][$row['type']] = [
                    'total' => (int)$row['total'],
                    'completed' => (int)$row['completed'],
                    'failed' => (int)$row['failed'],
                    'success_rate' => $row['total'] > 0 
                        ? round(($row['completed'] / $row['total']) * 100, 2) 
                        : 0
                ];
            }
            
            // Текущая нагрузка
            $stmt = Database::query("
                SELECT COUNT(*) FROM job_queue 
                WHERE status IN ('pending', 'processing')
            ");
            $stats['queue_length'] = (int)$stmt->fetchColumn();
            
            return $stats;
            
        } catch (\Exception $e) {
            Logger::error('Failed to get queue stats', ['error' => $e->getMessage()]);
            return [];
        }
    }
    
    /**
     * Очистить старые выполненные задачи
     */
    public static function cleanup(int $daysToKeep = 7): int
    {
        try {
            $cutoffDate = date('Y-m-d', strtotime("-{$daysToKeep} days"));
            
            $stmt = Database::query(
                "DELETE FROM job_queue 
                 WHERE status IN (?, ?) AND completed_at < ?",
                [self::STATUS_COMPLETED, self::STATUS_FAILED, $cutoffDate]
            );
            
            $deleted = $stmt->rowCount();
            
            Logger::info('Queue cleanup completed', [
                'deleted' => $deleted,
                'cutoff_date' => $cutoffDate
            ]);
            
            return $deleted;
            
        } catch (\Exception $e) {
            Logger::error('Queue cleanup failed', ['error' => $e->getMessage()]);
            return 0;
        }
    }
    
    // === Приватные методы ===
    
    /**
     * Получить обработчик для типа задачи
     */
    private static function getHandler(string $type): ?callable
    {
        // Карта обработчиков
        $handlers = [
            'email' => [EmailService::class, 'processQueuedEmail'],
            'metrics' => [self::class, 'processMetrics'],
            'import' => [ImportService::class, 'processImport'],
            'export' => [ExportService::class, 'processExport'],
            'notification' => [NotificationService::class, 'processNotification'],
            'cleanup' => [self::class, 'processCleanup']
        ];
        
        return $handlers[$type] ?? null;
    }
    
    /**
     * Обработчик для метрик
     */
    /**
     * Обработчик для метрик
     * ВАЖНО: Записываем напрямую в БД без использования MetricsService,
     * чтобы избежать циклических вызовов
     */
    public static function processMetrics(array $payload): bool
    {
        try {
            // Записываем метрику напрямую в БД
            Database::query(
                "INSERT INTO metrics (metric_type, data, value, created_at) 
                 VALUES (?, ?, ?, ?)",
                [
                    $payload['type'],
                    json_encode($payload['data'], JSON_UNESCAPED_UNICODE),
                    $payload['value'],
                    $payload['created_at']
                ]
            );
            
            return true;
        } catch (\Exception $e) {
            // Используем error_log вместо Logger для избежания рекурсии
            error_log('Failed to save metric from queue: ' . $e->getMessage());
            return false;
        }
    }
    
    /**
     * Обработчик для очистки
     */
    public static function processCleanup(array $payload): array
    {
        $results = [];
        
        // Очистка метрик
        if ($payload['metrics'] ?? true) {
            $results['metrics'] = MetricsService::cleanup($payload['days'] ?? 30);
        }
        
        // Очистка логов
        if ($payload['logs'] ?? true) {
            $results['logs'] = Logger::cleanup($payload['days'] ?? 30);
        }
        
        // Очистка самой очереди
        if ($payload['queue'] ?? true) {
            $results['queue'] = self::cleanup($payload['days'] ?? 7);
        }
        
        return $results;
    }
    
    /**
     * Получить информацию о задаче
     */
    private static function getJob(int $jobId): ?array
    {
        $stmt = Database::query("SELECT * FROM job_queue WHERE id = ?", [$jobId]);
        return $stmt->fetch() ?: null;
    }
    
    /**
     * Уведомить администратора о критической ошибке
     */
    private static function notifyAdminAboutFailure(array $job, string $error): void
    {
        // Добавляем задачу на отправку email администратору
        self::push('email', [
            'to' => Config::get('app.admin_email'),
            'subject' => 'Критическая ошибка в очереди задач',
            'template' => 'admin/queue_failure',
            'data' => [
                'job' => $job,
                'error' => $error
            ]
        ], self::PRIORITY_HIGH);
    }
}