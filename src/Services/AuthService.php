<?php
namespace App\Services;

use App\Core\Database;
use App\Core\Logger;
use App\Services\CartService;

class AuthService
{
    private const MAX_LOGIN_ATTEMPTS = 5;
    private const LOCKOUT_DURATION = 900; // 15 минут

    public static function authenticate(string $login, string $password): array
    {
        try {
            // Проверяем блокировку
            if (self::isBlocked($login)) {
                return ['success' => false, 'error' => 'Account temporarily locked'];
            }
            
            // Находим пользователя
            $user = self::findUser($login);
            if (!$user) {
                self::recordFailedAttempt($login);
                return ['success' => false, 'error' => 'Invalid credentials'];
            }

            // Проверяем пароль
            if (!password_verify($password, $user['password_hash'])) {
                self::recordFailedAttempt($login);
                return ['success' => false, 'error' => 'Invalid credentials'];
            }

            // Проверяем активность
            if (!$user['is_active']) {
                return ['success' => false, 'error' => 'Account is disabled'];
            }

            // Сбрасываем неудачные попытки
            self::clearFailedAttempts($login);
            
            // Создаем сессию
            self::createSession($user);
            
            // Логируем успешный вход
            self::logSecurityEvent('successful_login', $user['user_id']);

            return [
                'success' => true,
                'user' => self::sanitizeUserData($user)
            ];

        } catch (\Exception $e) {
            error_log("Authentication error: " . $e->getMessage());
            return ['success' => false, 'error' => 'Authentication system error'];
        }
    }

    public static function validateSession(): bool
    {
        if (session_status() !== PHP_SESSION_ACTIVE) {
            return false;
        }

        if (!isset($_SESSION['user_id'], $_SESSION['last_activity'])) {
            return false;
        }

        // Проверяем время неактивности
        $inactiveTime = time() - $_SESSION['last_activity'];
        if ($inactiveTime > 3600) { // 1 час
            self::destroySession();
            return false;
        }

        $_SESSION['last_activity'] = time();
        return true;
    }

    private static function findUser(string $login): ?array
    {
        $stmt = Database::query(
            "SELECT u.*, r.name as role_name 
             FROM users u 
             JOIN roles r ON u.role_id = r.role_id 
             WHERE (u.username = ? OR u.email = ?) 
             AND u.is_active = 1 
             LIMIT 1",
            [$login, $login]
        );

        return $stmt->fetch() ?: null;
    }

    private static function isBlocked(string $login): bool
    {
        $stmt = Database::query(
            "SELECT failed_attempts, last_attempt 
             FROM login_attempts 
             WHERE identifier = ? 
             LIMIT 1",
            [$login]
        );

        $attempts = $stmt->fetch();
        
        return $attempts && 
               $attempts['failed_attempts'] >= self::MAX_LOGIN_ATTEMPTS &&
               (time() - strtotime($attempts['last_attempt'])) < self::LOCKOUT_DURATION;
    }

    private static function recordFailedAttempt(string $login): void
    {
        Database::query(
            "INSERT INTO login_attempts (identifier, failed_attempts, last_attempt, ip_address) 
             VALUES (?, 1, NOW(), ?) 
             ON DUPLICATE KEY UPDATE 
             failed_attempts = failed_attempts + 1, 
             last_attempt = NOW(),
             ip_address = VALUES(ip_address)",
            [$login, $_SERVER['REMOTE_ADDR'] ?? '']
        );

        self::logSecurityEvent('failed_login_attempt', null, ['login' => $login]);
    }

    private static function clearFailedAttempts(string $login): void
    {
        Database::query(
            "DELETE FROM login_attempts WHERE identifier = ?",
            [$login]
        );
    }

    private static function createSession(array $user): void
    {
        session_regenerate_id(true);
        
        $_SESSION['user_id'] = $user['user_id'];
        $_SESSION['username'] = $user['username'];
        $_SESSION['role'] = $user['role_name'];
        $_SESSION['last_activity'] = time();
        $_SESSION['ip'] = $_SERVER['REMOTE_ADDR'] ?? '';
        
        // Обновляем информацию о последнем входе
        Database::query(
            "UPDATE users SET last_login_at = NOW(), last_login_ip = ? WHERE user_id = ?",
            [$_SESSION['ip'], $user['user_id']]
        );
        
        // Объединяем гостевую корзину с пользовательской
        try {
            CartService::mergeGuestCartWithUser($user['user_id']);
        } catch (\Exception $e) {
            Logger::warning('Failed to merge guest cart', [
                'user_id' => $user['user_id'],
                'error' => $e->getMessage()
            ]);
        }
    }

    private static function sanitizeUserData(array $user): array
    {
        unset($user['password_hash']);
        return $user;
    }

    private static function logSecurityEvent(string $event, ?int $userId, array $context = []): void
    {
        $context['ip'] = $_SERVER['REMOTE_ADDR'] ?? '';
        $context['user_agent'] = $_SERVER['HTTP_USER_AGENT'] ?? '';
        
        Database::query(
            "INSERT INTO audit_logs (user_id, action, object_type, details, created_at) 
             VALUES (?, ?, 'security', ?, NOW())",
            [$userId, $event, json_encode($context)]
        );
    }

    public static function destroySession(): void
    {
        if (session_status() === PHP_SESSION_ACTIVE) {
            session_unset();
            session_destroy();
        }
    }

    public static function check(): bool
    {
        return self::validateSession();
    }

    public static function user(): ?array
    {
        if (!self::check()) {
            return null;
        }
        
        return [
            'id' => $_SESSION['user_id'],
            'username' => $_SESSION['username'],
            'role' => $_SESSION['role']
        ];
    }

    public static function checkRole(string $role): bool
    {
        $user = self::user();
        return $user && ($user['role'] === $role || $user['role'] === 'admin');
    }
}