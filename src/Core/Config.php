<?php
namespace App\Core;

/**
 * Безопасный менеджер конфигурации
 * Загружает настройки из защищенной директории
 */
class Config
{
    private static array $config = [];
    private static bool $loaded = false;
    private static ?string $configPath = null;

    /**
     * Получить значение конфигурации
     * @param string $key Ключ в формате 'section.key' или 'key'
     * @param mixed $default Значение по умолчанию
     * @return mixed
     */
    public static function get(string $key, $default = null)
    {
        if (!self::$loaded) {
            self::load();
        }

        $keys = explode('.', $key);
        $value = self::$config;

        foreach ($keys as $segment) {
            if (!is_array($value) || !array_key_exists($segment, $value)) {
                return $default;
            }
            $value = $value[$segment];
        }

        return $value;
    }

    /**
     * Проверить существование ключа
     */
    public static function has(string $key): bool
    {
        return self::get($key) !== null;
    }

    /**
     * Получить всю конфигурацию
     */
    public static function all(): array
    {
        if (!self::$loaded) {
            self::load();
        }
        return self::$config;
    }

    /**
     * Загрузка конфигурации из безопасной директории
     */
    private static function load(): void
    {
        try {
            // Проверяем пути конфигурации в порядке приоритета
            $configPaths = [
                '/var/www/config/vdestor',    // Новый путь для PHP
                '/etc/vdestor/config',         // Основной путь
                $_ENV['CONFIG_PATH'] ?? null,
                dirname(__DIR__, 2) . '/config',
                dirname(__DIR__, 2) . '/.config'
            ];
            
            self::$configPath = null;
            foreach (array_filter($configPaths) as $path) {
                if (is_dir($path) && is_readable($path)) {
                    self::$configPath = $path;
                    break;
                }
            }
            
            if (!self::$configPath) {
                error_log("Config directory not found, using defaults");
                // Используем дефолтную конфигурацию из ваших данных
                self::$config = [
                    'database' => [
                        'mysql' => [
                            'host' => 'localhost',
                            'user' => 'adminkjg', // из ваших данных
                            'password' => 'adQw67Ffl', // из ваших данных 
                            'database' => 'magellanjg', // из ваших данных
                            'charset' => 'utf8mb4',
                            'port' => 3306
                        ]
                    ],
                    'app' => [
                        'name' => 'VDE Store',
                        'debug' => false,
                        'url' => 'https://vdestor.ru',
                        'timezone' => 'Europe/Moscow'
                    ],
                    'session' => [
                        'save_handler' => 'db',
                        'gc_maxlifetime' => 1800,
                        'cookie_secure' => true,
                        'cookie_httponly' => true,
                        'cookie_samesite' => 'Lax',
                        'name' => 'VDE_SESSION',
                        'table' => 'sessions',
                        'regenerate_interval' => 1800
                    ]
                ];
                self::$loaded = true;
                return;
            }
            
            // Загружаем .env файл первым
            self::loadEnvironmentFile();
            
            // Загружаем все ini файлы
            self::loadIniFiles();
            
            // Заменяем переменные окружения в конфигах
            self::$config = self::replaceEnvironmentVariables(self::$config);
            
            self::$loaded = true;
            
        } catch (\Exception $e) {
            error_log("Configuration loading failed: " . $e->getMessage());
            // Используем дефолтную конфигурацию при ошибке
            self::$config = [
                'database' => [
                    'mysql' => [
                        'host' => 'localhost',
                        'user' => 'adminkjg',
                        'password' => 'adQw67Ffl',
                        'database' => 'magellanjg',
                        'charset' => 'utf8mb4',
                        'port' => 3306
                    ]
                ]
            ];
            self::$loaded = true;
        }
    }

    /**
     * Загрузка переменных окружения из .env файла
     */
    private static function loadEnvironmentFile(): void
    {
        $envFile = self::$configPath . '/.env';
        
        if (!file_exists($envFile)) {
            return; // .env файл опционален
        }

        $lines = file($envFile, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
        
        foreach ($lines as $line) {
            $line = trim($line);
            
            // Пропускаем комментарии и пустые строки
            if (empty($line) || strpos($line, '#') === 0) {
                continue;
            }
            
            // Разбираем строку KEY=VALUE
            if (strpos($line, '=') !== false) {
                [$key, $value] = explode('=', $line, 2);
                $_ENV[trim($key)] = trim($value, '"\'');
            }
        }
    }

    /**
     * Загрузка всех INI файлов из директории
     */
    private static function loadIniFiles(): void
    {
        $configFiles = [
            'database' => 'database.ini',
            'app' => 'app.ini', 
            'integrations' => 'integrations.ini',
            'security' => 'security.ini'
        ];

        foreach ($configFiles as $section => $filename) {
            $filePath = self::$configPath . '/' . $filename;
            
            if (file_exists($filePath)) {
                $config = parse_ini_file($filePath, true);
                if ($config !== false) {
                    self::$config[$section] = $config;
                }
            }
        }
    }

    /**
     * Замена переменных окружения в значениях конфигурации
     */
    private static function replaceEnvironmentVariables(array $config): array
    {
        array_walk_recursive($config, function (&$value) {
            if (is_string($value) && preg_match('/\$\{([^}]+)\}/', $value, $matches)) {
                $envKey = $matches[1];
                $envValue = $_ENV[$envKey] ?? '';
                $value = str_replace($matches[0], $envValue, $value);
            }
        });
        
        return $config;
    }

    /**
     * Получить путь к директории конфигурации
     */
    public static function getConfigPath(): ?string
    {
        if (!self::$loaded) {
            self::load();
        }
        return self::$configPath;
    }

    /**
     * Проверить безопасность конфигурации
     */
    public static function validateSecurity(): array
    {
        $issues = [];
        
        // Проверяем права доступа к директории
        $configPath = self::getConfigPath();
        if ($configPath && is_readable($configPath)) {
            $perms = fileperms($configPath) & 0777;
            if ($perms > 0700) {
                $issues[] = "Configuration directory has too permissive rights: " . decoct($perms);
            }
        }

        // Проверяем наличие обязательных настроек
        $required = [
            'database.mysql.host',
            'database.mysql.user', 
            'database.mysql.password',
            'database.mysql.database'
        ];

        foreach ($required as $key) {
            if (!self::has($key)) {
                $issues[] = "Required configuration missing: {$key}";
            }
        }

        return $issues;
    }
}