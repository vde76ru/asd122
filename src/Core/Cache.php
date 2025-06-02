<?php
namespace App\Core;

/**
 * Упрощенная система кэширования
 * Используем только файловый кэш для предсказуемости
 */
class Cache
{
    private static string $cacheDir = '/tmp/vdestor_cache/';
    private static bool $enabled = true;
    private static array $memoryCache = []; // Кэш в памяти для текущего запроса
    
    public static function init(): void
    {
        // Создаем директорию для кэша если её нет
        if (!is_dir(self::$cacheDir)) {
            @mkdir(self::$cacheDir, 0777, true);
        }
        
        // Проверяем доступность директории
        if (!is_writable(self::$cacheDir)) {
            error_log("Cache directory not writable: " . self::$cacheDir);
            self::$enabled = false;
        }
        
        // Очищаем старые файлы кэша при инициализации (1% вероятность)
        if (mt_rand(1, 100) === 1) {
            self::cleanup();
        }
    }
    
    public static function get(string $key)
    {
        if (!self::$enabled) {
            return null;
        }
        
        // Проверяем кэш в памяти сначала
        if (isset(self::$memoryCache[$key])) {
            return self::$memoryCache[$key];
        }
        
        $filename = self::getFilename($key);
        
        if (!file_exists($filename)) {
            return null;
        }
        
        try {
            $content = @file_get_contents($filename);
            if ($content === false) {
                return null;
            }
            
            $data = @unserialize($content);
            if ($data === false) {
                @unlink($filename);
                return null;
            }
            
            // Проверяем срок действия
            if ($data['expires'] < time()) {
                @unlink($filename);
                return null;
            }
            
            // Сохраняем в памяти для быстрого доступа
            self::$memoryCache[$key] = $data['value'];
            
            return $data['value'];
            
        } catch (\Exception $e) {
            error_log("Cache read error: " . $e->getMessage());
            return null;
        }
    }
    
    public static function set(string $key, $value, int $ttl = 3600): bool
    {
        if (!self::$enabled) {
            return false;
        }
        
        // Сохраняем в памяти
        self::$memoryCache[$key] = $value;
        
        $filename = self::getFilename($key);
        $data = [
            'expires' => time() + $ttl,
            'value' => $value
        ];
        
        try {
            $content = serialize($data);
            
            // Атомарная запись через временный файл
            $tempFile = $filename . '.tmp.' . uniqid();
            if (@file_put_contents($tempFile, $content, LOCK_EX) === false) {
                return false;
            }
            
            if (!@rename($tempFile, $filename)) {
                @unlink($tempFile);
                return false;
            }
            
            return true;
            
        } catch (\Exception $e) {
            error_log("Cache write error: " . $e->getMessage());
            return false;
        }
    }
    
    public static function delete(string $key): bool
    {
        unset(self::$memoryCache[$key]);
        
        if (!self::$enabled) {
            return true;
        }
        
        $filename = self::getFilename($key);
        
        if (file_exists($filename)) {
            return @unlink($filename);
        }
        
        return true;
    }
    
    /**
     * Очистить весь кэш
     */
    public static function clear(): void
    {
        self::$memoryCache = [];
        
        if (!self::$enabled) {
            return;
        }
        
        $files = glob(self::$cacheDir . '*.cache');
        if ($files) {
            foreach ($files as $file) {
                @unlink($file);
            }
        }
    }
    
    /**
     * Очистка старых файлов кэша
     */
    public static function cleanup(): void
    {
        if (!self::$enabled) {
            return;
        }
        
        $files = glob(self::$cacheDir . '*.cache');
        if (!$files) {
            return;
        }
        
        $now = time();
        $cleaned = 0;
        
        foreach ($files as $file) {
            // Удаляем файлы старше 24 часов
            if (filemtime($file) < ($now - 86400)) {
                if (@unlink($file)) {
                    $cleaned++;
                }
            }
        }
        
        if ($cleaned > 0) {
            error_log("Cache cleanup: removed {$cleaned} old files");
        }
    }
    
    /**
     * Отключить кэш
     */
    public static function disable(): void
    {
        self::$enabled = false;
        self::$memoryCache = [];
    }
    
    /**
     * Включить кэш
     */
    public static function enable(): void
    {
        self::$enabled = true;
    }
    
    /**
     * Проверить включен ли кэш
     */
    public static function isEnabled(): bool
    {
        return self::$enabled;
    }
    
    /**
     * Получить имя файла для ключа
     */
    private static function getFilename(string $key): string
    {
        // Используем простой хэш для имени файла
        $hash = md5($key);
        return self::$cacheDir . $hash . '.cache';
    }
    
    /**
     * Получить статистику кэша
     */
    public static function getStats(): array
    {
        if (!self::$enabled) {
            return ['enabled' => false];
        }
        
        $files = glob(self::$cacheDir . '*.cache');
        $totalSize = 0;
        $validFiles = 0;
        
        if ($files) {
            foreach ($files as $file) {
                $size = filesize($file);
                $totalSize += $size;
                
                // Проверяем валидность файла
                $content = @file_get_contents($file);
                if ($content) {
                    $data = @unserialize($content);
                    if ($data && isset($data['expires']) && $data['expires'] > time()) {
                        $validFiles++;
                    }
                }
            }
        }
        
        return [
            'enabled' => true,
            'cache_dir' => self::$cacheDir,
            'total_files' => count($files ?? []),
            'valid_files' => $validFiles,
            'total_size' => $totalSize,
            'memory_items' => count(self::$memoryCache)
        ];
    }
}