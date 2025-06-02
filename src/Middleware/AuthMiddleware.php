<?php
namespace App\Middleware;

use App\Services\AuthService;
use App\Core\Logger;

/**
 * Middleware для проверки аутентификации
 */
class AuthMiddleware
{
    /**
     * Проверить аутентификацию пользователя
     */
    public static function handle(): bool
    {
        if (!AuthService::validateSession()) {
            Logger::security("Unauthorized access attempt", [
                'uri' => $_SERVER['REQUEST_URI'] ?? '',
                'ip' => $_SERVER['REMOTE_ADDR'] ?? '',
                'user_agent' => $_SERVER['HTTP_USER_AGENT'] ?? ''
            ]);
            
            // Если это AJAX запрос, возвращаем JSON
            if (self::isAjaxRequest()) {
                http_response_code(401);
                header('Content-Type: application/json');
                echo json_encode([
                    'success' => false,
                    'error' => 'Authentication required',
                    'redirect' => '/login'
                ]);
                exit;
            }
            
            // Обычный запрос - редирект на логин
            header('Location: /login');
            exit;
        }
        
        return true;
    }

    /**
     * Проверить роль пользователя
     */
    public static function requireRole(string $role): bool
    {
        if (!self::handle()) {
            return false;
        }

        $user = AuthService::user();
        if (!$user || ($user['role'] !== $role && $user['role'] !== 'admin')) {
            Logger::security("Access denied - insufficient permissions", [
                'required_role' => $role,
                'user_role' => $user['role'] ?? 'none',
                'user_id' => $user['id'] ?? null,
                'uri' => $_SERVER['REQUEST_URI'] ?? ''
            ]);

            if (self::isAjaxRequest()) {
                http_response_code(403);
                header('Content-Type: application/json');
                echo json_encode([
                    'success' => false,
                    'error' => 'Insufficient permissions'
                ]);
                exit;
            }

            http_response_code(403);
            echo "Access denied";
            exit;
        }

        return true;
    }

    /**
     * Проверить, является ли запрос AJAX
     */
    private static function isAjaxRequest(): bool
    {
        return !empty($_SERVER['HTTP_X_REQUESTED_WITH']) &&
               strtolower($_SERVER['HTTP_X_REQUESTED_WITH']) === 'xmlhttprequest';
    }
}