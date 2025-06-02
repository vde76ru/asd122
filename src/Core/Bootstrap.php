<?php
namespace App\Core;

class Bootstrap 
{
    private static bool $initialized = false;
    private static array $initLog = [];
    
    public static function init(): void 
    {
        if (self::$initialized) {
            // Логируем но НЕ продолжаем инициализацию
            error_log("Bootstrap::init() called multiple times! Preventing re-initialization.");
            return; // Просто выходим, не бросаем исключение
        }
        
        try {
            // Устанавливаем флаг СРАЗУ, чтобы предотвратить рекурсию
            self::$initialized = true;
            
            // 1. Config
            self::initComponent('Config', function() {
                Config::get('app.name');
            });
            
            // 2. Logger
            self::initComponent('Logger', function() {
                Logger::initialize();
            });
            
            // 3. Database
            self::initComponent('Database', function() {
                Database::getConnection();
            });
            
            // 4. Cache
            self::initComponent('Cache', function() {
                if (class_exists('\App\Core\Cache')) {
                    Cache::init();
                }
            });
            
            // 5. Security
            self::initComponent('Security', function() {
                if (class_exists('\App\Core\SecurityManager')) {
                    SecurityManager::initialize();
                }
            });
            
            // 6. Session - только если еще не запущена
            self::initComponent('Session', function() {
                if (session_status() !== PHP_SESSION_ACTIVE) {
                    Session::start();
                }
            });
            
            error_log("Bootstrap initialized successfully");
            
        } catch (\Exception $e) {
            self::$initialized = false; // Откатываем при ошибке
            error_log("Bootstrap error: " . $e->getMessage());
            throw $e;
        }
    }
    
    private static function initComponent(string $name, callable $initializer): void
    {
        if (isset(self::$initLog[$name])) {
            error_log("Component {$name} already initialized!");
            return;
        }
        
        try {
            $initializer();
            self::$initLog[$name] = true;
        } catch (\Exception $e) {
            error_log("Failed to initialize {$name}: " . $e->getMessage());
            throw $e;
        }
    }
    
    public static function isInitialized(): bool
    {
        return self::$initialized;
    }
    
    public static function getInitializedComponents(): array
    {
        return array_keys(self::$initLog);
    }
}