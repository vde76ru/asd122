<?php

namespace App\Core;

use App\Core\Database;
use App\Core\DBSessionHandler;
use App\Services\AuthService;
use App\Services\AuditService;
use App\Core\Logger; // Убедитесь, что класс Logger доступен

class Session
{
    public static function start(): void
    {
        // Проверяем, не запущена ли уже сессия
        if (session_status() === PHP_SESSION_ACTIVE) {
            return; // Сессия уже активна, ничего не делаем
        }
        
        // Проверяем, были ли отправлены заголовки
        if (headers_sent($file, $line)) {
            throw new \RuntimeException("Cannot start session, headers already sent in $file:$line");
        }

        // Путь к файлу конфигурации приложения
        $configFilePath = Config::getConfigPath() . '/app.ini';

        // Проверка наличия файла конфигурации
        if (!file_exists($configFilePath)) {
            throw new \RuntimeException("Config file not found: $configFilePath");
        }

        // Чтение конфигурации приложения
        $config = parse_ini_file($configFilePath, true, INI_SCANNER_TYPED);
        $sessionConfig = $config['session'] ?? [];

        // Настройки сессии
        $saveHandler = $sessionConfig['save_handler'] ?? 'files';
        $gcMaxLifetime = (int)($sessionConfig['gc_maxlifetime'] ?? 0);
        $sessionName = $sessionConfig['name'] ?? session_name();

        // Установка параметров куки сессии
        $domain = $sessionConfig['cookie_domain'] ?? '';
        $params = [
            'lifetime' => $gcMaxLifetime,
            'path'     => '/',
            'domain'   => $domain !== '' ? $domain : ($_SERVER['HTTP_HOST'] ?? ''),
            'secure'   => (bool)($sessionConfig['cookie_secure'] ?? true),
            'httponly' => (bool)($sessionConfig['cookie_httponly'] ?? true),
            'samesite' => $sessionConfig['cookie_samesite'] ?? 'Strict',
        ];
        session_set_cookie_params($params);
        session_name($sessionName);

        // Жесткая безопасность
        ini_set('session.use_strict_mode', '1');
        ini_set('session.use_only_cookies', '1');

        // Установка хендлера сессии
        if ($saveHandler === 'db') {
            // создаём и регистрируем свой хендлер
            $pdo     = Database::getConnection();
            $handler = new DBSessionHandler($pdo, $gcMaxLifetime);
            session_set_save_handler($handler, true);
            // НЕ вызывать ini_set('session.save_handler', …)
        } else {
            // встроенные хендлеры: files, memcached и т.п.
            ini_set('session.save_handler', $saveHandler);
        }

        // Устанавливаем время жизни для GC
        ini_set('session.gc_maxlifetime', (string)$gcMaxLifetime);

        // Попытка запустить сессию
        try {
            session_start();
        } catch (\Exception $e) {
            // Логируем ошибку
            error_log('Session start failed: ' . $e->getMessage());

            // Откатываемся на файловый хендлер
            ini_set('session.save_handler', 'files');
            session_start();
        }

        // Проверка времени неактивности
        $timeout = $gcMaxLifetime;
        $now = time();
        
        if (isset($_SESSION['LAST_ACTIVITY']) && ($now - $_SESSION['LAST_ACTIVITY']) > $timeout) {
            // Время ожидания истекло
            self::logout();
            session_start(); // создаём новую сессию
        }

        $_SESSION['LAST_ACTIVITY'] = $now;

        // Проверка отпечатка клиента (Fingerprint)
        $fingerprint = hash('sha256',
            ($_SERVER['REMOTE_ADDR'] ?? '') .
            ($_SERVER['HTTP_USER_AGENT'] ?? '')
        );
        if (!isset($_SESSION['FINGERPRINT'])) {
            $_SESSION['FINGERPRINT'] = $fingerprint;
        } elseif ($_SESSION['FINGERPRINT'] !== $fingerprint) {
            // Добавляем логирование перед уничтожением сессии
            Logger::warning('Session fingerprint mismatch', [
                'old' => $_SESSION['FINGERPRINT'],
                'new' => $fingerprint
            ]);
            self::logout();
            session_start(); // создаём новую сессию
        }

        // Назначение роли guest для неавторизованного пользователя
        define('ROLE_GUEST', 'guest');
        if (empty($_SESSION['user_id'])) {
            $_SESSION['role'] = ROLE_GUEST;
            $_SESSION['is_guest'] = true;
        }
    }

    /**
     * Полный выход: сброс всех данных сессии и куки
     */
    public static function logout(): void
    {
        // 1) Очистить массив $_SESSION
        $_SESSION = [];

        // 2) Уничтожить сессию на сервере
        if (session_id() !== '') {
            session_destroy();
        }

        // 3) Удалить cookie
        if (ini_get('session.use_cookies')) {
            $params = session_get_cookie_params();
            setcookie(
                session_name(),
                '',
                time() - 42000,
                $params['path'],
                $params['domain'] ?? '',
                $params['secure'],
                $params['httponly']
            );
        }
    }
}