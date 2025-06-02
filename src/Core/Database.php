<?php
namespace App\Core;

use PDO;
use PDOException;
use App\Exceptions\DatabaseException;

/**
 * Профессиональный менеджер базы данных
 * С пулом соединений, мониторингом и безопасностью
 */
class Database
{
    private static array $connections = [];
    private static array $config = [];
    private static int $queryCount = 0;
    private static float $totalQueryTime = 0.0;

    /**
     * Получить подключение к базе данных
     */
    public static function getConnection(string $name = 'default'): PDO
    {
        if (!isset(self::$connections[$name]) || !self::isConnectionAlive($name)) {
            self::$connections[$name] = self::createConnection($name);
        }

        return self::$connections[$name];
    }

    /**
     * Выполнить запрос с мониторингом производительности
     */
    public static function query(string $sql, array $params = [], string $connection = 'default'): \PDOStatement
    {
        $startTime = microtime(true);
        
        try {
            $pdo = self::getConnection($connection);
            $stmt = $pdo->prepare($sql);
            $stmt->execute($params);
            
            $executionTime = microtime(true) - $startTime;
            self::$queryCount++;
            self::$totalQueryTime += $executionTime;
            
            // Логируем медленные запросы
            if ($executionTime > 0.1) {
                Logger::warning("Slow query detected", [
                    'sql' => $sql,
                    'execution_time' => $executionTime,
                    'params' => $params
                ]);
            }
            
            return $stmt;
            
        } catch (PDOException $e) {
            error_log("Database query failed: " . $e->getMessage());
            
            throw new DatabaseException("Query execution failed: " . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Создать новое подключение
     */
    private static function createConnection(string $name): PDO
    {
        if (empty(self::$config)) {
            self::loadConfig();
        }
    
        $config = self::$config[$name] ?? self::$config['default'];
        
        if (!$config) {
            throw new DatabaseException("Database configuration for '{$name}' not found");
        }
    
        $dsn = self::buildDsn($config);
        
        try {
            $options = [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                PDO::ATTR_EMULATE_PREPARES => false,
                PDO::MYSQL_ATTR_INIT_COMMAND => "SET SESSION sql_mode='STRICT_ALL_TABLES'"
            ];
    
            $pdo = new PDO($dsn, $config['user'], $config['password'], $options);
            
            // Используем Logger только после успешного подключения
            if (class_exists(Logger::class)) {
                Logger::info("Database connection established", ['connection' => $name]);
            }
            
            return $pdo;
            
        } catch (PDOException $e) {
            // НЕ используем Logger в критических ошибках БД
            error_log("Database connection failed: " . $e->getMessage());
            throw new DatabaseException("Database connection failed: " . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Загрузить конфигурацию БД из Config
     */
    private static function loadConfig(): void
    {
        $dbConfig = Config::get('database', []);
        
        if (empty($dbConfig)) {
            throw new DatabaseException("Database configuration not found");
        }
        
        // Основное подключение
        if (isset($dbConfig['mysql'])) {
            self::$config['default'] = $dbConfig['mysql'];
        }
        
        // Подключение для чтения (если есть)
        if (isset($dbConfig['mysql_slave'])) {
            self::$config['slave'] = $dbConfig['mysql_slave'];
        }
    }

    /**
     * Построить DSN строку
     */
    private static function buildDsn(array $config): string
    {
        return sprintf(
            "mysql:host=%s;port=%d;dbname=%s;charset=%s",
            $config['host'],
            $config['port'] ?? 3306,
            $config['database'],
            $config['charset'] ?? 'utf8mb4'
        );
    }

    /**
     * Проверить активность соединения
     */
    private static function isConnectionAlive(string $name): bool
    {
        if (!isset(self::$connections[$name])) {
            return false;
        }

        try {
            self::$connections[$name]->query('SELECT 1');
            return true;
        } catch (PDOException $e) {
            Logger::warning("Database connection lost", ['connection' => $name]);
            return false;
        }
    }

    /**
     * Получить статистику производительности
     */
    public static function getStats(): array
    {
        return [
            'query_count' => self::$queryCount,
            'total_time' => self::$totalQueryTime,
            'average_time' => self::$queryCount > 0 ? self::$totalQueryTime / self::$queryCount : 0,
            'connections' => array_keys(self::$connections)
        ];
    }

    /**
     * Закрыть все подключения
     */
    public static function closeAll(): void
    {
        self::$connections = [];
        Logger::info("All database connections closed");
    }
}