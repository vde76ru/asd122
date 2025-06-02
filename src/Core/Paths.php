<?php
// src/Core/Paths.php
namespace App\Core;

/**
 * Централизованное управление путями приложения
 */
class Paths
{
    private static ?string $basePath = null;

    // Относительные пути для URL
    const ASSETS_URL = '/assets/dist';
    const IMAGES_URL = '/images';
    const UPLOADS_URL = '/uploads';

    private static function getBasePath(): string
    {
        if (self::$basePath === null) {
            // Определяем базовый путь динамически
            self::$basePath = realpath(__DIR__ . '/../../');
        }
        return self::$basePath;
    }

    /**
     * Получить полный путь к файлу
     */
    public static function get(string $type, string $path = ''): string
    {
        $basePath = match($type) {
            'base' => self::getBasePath(),
            'public' => self::getBasePath() . '/public',
            'src' => self::getBasePath() . '/src',
            'config' => $_ENV['CONFIG_PATH'] ?? '/etc/vdestor/config',
            'log' => $_ENV['LOG_PATH'] ?? '/var/log/vdestor',
            'views' => self::getBasePath() . '/src/views',
            'controllers' => self::getBasePath() . '/src/Controllers',
            'services' => self::getBasePath() . '/src/Services',
            default => self::getBasePath()
        };

        return $basePath . ($path ? '/' . ltrim($path, '/') : '');
    }

    /**
     * Получить URL для ассетов
     */
    public static function asset(string $path): string
    {
        return self::ASSETS_URL . '/' . ltrim($path, '/');
    }
    
    /**
     * Проверить существование пути
     */
    public static function exists(string $type, string $path = ''): bool
    {
        return file_exists(self::get($type, $path));
    }
}