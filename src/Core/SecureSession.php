<?php
namespace App\Core;

/**
 * Класс для безопасной работы с сессиями
 */
class SecureSession
{
    /**
     * Генерация безопасного fingerprint
     */
    public static function generateFingerprint(): string
    {
        $data = [
            $_SERVER['HTTP_USER_AGENT'] ?? '',
            $_SERVER['HTTP_ACCEPT_LANGUAGE'] ?? '',
            $_SERVER['HTTP_ACCEPT_ENCODING'] ?? '',
            // Добавляем дополнительные параметры для усиления
            $_SERVER['HTTP_DNT'] ?? '',
            $_SERVER['HTTP_CONNECTION'] ?? '',
            // Используем только первые 3 октета IP для защиты от смены IP в одной подсети
            implode('.', array_slice(explode('.', $_SERVER['REMOTE_ADDR'] ?? ''), 0, 3))
        ];
        
        return hash('sha256', implode('|', $data));
    }
    
    /**
     * Проверка сессии на валидность
     */
    public static function validateSession(): bool
    {
        // Проверка fingerprint
        $currentFingerprint = self::generateFingerprint();
        if (!isset($_SESSION['fingerprint'])) {
            $_SESSION['fingerprint'] = $currentFingerprint;
        } elseif ($_SESSION['fingerprint'] !== $currentFingerprint) {
            // Возможная попытка угона сессии
            session_destroy();
            return false;
        }
        
        // Проверка времени жизни
        if (isset($_SESSION['last_activity'])) {
            $inactive = time() - $_SESSION['last_activity'];
            $maxInactive = ini_get('session.gc_maxlifetime') ?: 1440;
            
            if ($inactive > $maxInactive) {
                session_destroy();
                return false;
            }
        }
        
        $_SESSION['last_activity'] = time();
        
        // Регенерация ID сессии каждые 30 минут
        if (!isset($_SESSION['regenerated'])) {
            $_SESSION['regenerated'] = time();
        } elseif (time() - $_SESSION['regenerated'] > 1800) {
            session_regenerate_id(true);
            $_SESSION['regenerated'] = time();
        }
        
        return true;
    }
}