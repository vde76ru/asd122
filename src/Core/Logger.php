<?php
namespace App\Core;

class Logger
{
    private static bool $useDatabase = true;
    private static array $deferredLogs = [];
    
    private const LEVELS = [
        'emergency' => 0, 'alert' => 1, 'critical' => 2, 'error' => 3,
        'warning' => 4, 'notice' => 5, 'info' => 6, 'debug' => 7
    ];

    public static function initialize(): void
    {
        // Инициализация выполняется после Database
        self::$useDatabase = true;
        self::flushDeferredLogs();
    }

    public static function critical(string $message, array $context = []): void
    {
        self::log('critical', $message, $context);
    }

    public static function emergency(string $message, array $context = []): void
    {
        self::log('emergency', $message, $context);
    }

    public static function error(string $message, array $context = []): void
    {
        self::log('error', $message, $context);
    }

    public static function warning(string $message, array $context = []): void
    {
        self::log('warning', $message, $context);
    }

    public static function info(string $message, array $context = []): void
    {
        self::log('info', $message, $context);
    }

    public static function security(string $message, array $context = []): void
    {
        $context['security_event'] = true;
        self::log('warning', "[SECURITY] {$message}", $context);
    }

    // Добавленный метод debug
    public static function debug(string $message, array $context = []): void
    {
        self::log('debug', $message, $context);
    }

    private static function log(string $level, string $message, array $context): void
    {
        // ВСЕГДА пишем в файл
        self::logToFile($level, $message, $context);
        
        // В БД пишем только если доступна
        if (self::$useDatabase) {
            try {
                self::logToDatabase($level, $message, $context);
            } catch (\Exception $e) {
                // Откладываем запись
                self::$deferredLogs[] = [
                    'level' => $level,
                    'message' => $message,
                    'context' => $context,
                    'time' => date('Y-m-d H:i:s')
                ];
            }
        }
    }

    private static function logToDatabase(string $level, string $message, array $context): void
    {
        // Предотвращаем рекурсию
        static $inDatabaseLog = false;
        if ($inDatabaseLog) {
            return;
        }
        
        $inDatabaseLog = true;
        
        try {
            if (!class_exists(Database::class)) {
                return;
            }
    
            $extra = [
                'user_id' => $_SESSION['user_id'] ?? null,
                'ip' => $_SERVER['REMOTE_ADDR'] ?? '',
                'uri' => $_SERVER['REQUEST_URI'] ?? ''
            ];
    
            Database::query(
                "INSERT INTO application_logs (level, message, context, extra, created_at) 
                 VALUES (?, ?, ?, ?, NOW())",
                [$level, $message, json_encode($context), json_encode($extra)]
            );
        } catch (\Exception $e) {
            // Игнорируем ошибки логирования в БД
        } finally {
            $inDatabaseLog = false;
        }
    }

    private static function logToFile(string $level, string $message, array $context): void
    {
        $logDir = '/var/www/www-root/data/logs';
        if (!is_dir($logDir)) {
            @mkdir($logDir, 0755, true);
        }

        $logFile = $logDir . '/app.log';
        $timestamp = date('Y-m-d H:i:s');
        $contextStr = !empty($context) ? ' ' . json_encode($context) : '';
        
        $logLine = "[{$timestamp}] {$level}: {$message}{$contextStr}" . PHP_EOL;
        @file_put_contents($logFile, $logLine, FILE_APPEND | LOCK_EX);
    }

    private static function flushDeferredLogs(): void
    {
        foreach (self::$deferredLogs as $log) {
            try {
                self::logToDatabase($log['level'], $log['message'], $log['context']);
            } catch (\Exception $e) {
                // Игнорируем ошибки
            }
        }
        self::$deferredLogs = [];
    }
}