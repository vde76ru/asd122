<?php
namespace App\Middleware;

use App\Services\AuthService;
use App\Core\Logger;

/**
 * Централизованная проверка безопасности для роутов
 */
class SecurityMiddleware
{
    /**
     * Роуты, требующие аутентификации
     */
    private static array $protectedRoutes = [
        '/admin' => ['admin'],
        '/specification/create' => ['client', 'admin'],
        '/specifications' => ['client', 'admin'],
        '/profile' => ['client', 'admin'],
        '/api/user/*' => ['client', 'admin']
    ];
    
    /**
     * Публичные роуты (не требуют аутентификации)
     */
    private static array $publicRoutes = [
        '/',
        '/login',
        '/register',
        '/api/search',
        '/api/autocomplete',
        '/api/availability',
        '/shop',
        '/shop/product'
    ];
    
    /**
     * Проверить доступ к роуту
     */
    public static function checkRoute(string $route): bool
    {
        // Публичные роуты - всегда доступны
        if (self::isPublicRoute($route)) {
            return true;
        }
        
        // Проверяем аутентификацию
        if (!AuthService::check()) {
            Logger::security('Unauthorized access attempt', ['route' => $route]);
            
            if (self::isApiRoute($route)) {
                http_response_code(401);
                header('Content-Type: application/json');
                echo json_encode(['error' => 'Authentication required']);
            } else {
                header('Location: /login');
            }
            exit;
        }
        
        // Проверяем роль для защищенных роутов
        $requiredRoles = self::getRequiredRoles($route);
        if (!empty($requiredRoles)) {
            $user = AuthService::user();
            if (!in_array($user['role'], $requiredRoles)) {
                Logger::security('Access denied - insufficient permissions', [
                    'route' => $route,
                    'user_role' => $user['role'],
                    'required_roles' => $requiredRoles
                ]);
                
                http_response_code(403);
                if (self::isApiRoute($route)) {
                    header('Content-Type: application/json');
                    echo json_encode(['error' => 'Insufficient permissions']);
                } else {
                    echo 'Access denied';
                }
                exit;
            }
        }
        
        return true;
    }
    
    /**
     * Проверить, является ли роут публичным
     */
    private static function isPublicRoute(string $route): bool
    {
        foreach (self::$publicRoutes as $publicRoute) {
            if ($route === $publicRoute || 
                (str_ends_with($publicRoute, '*') && 
                 str_starts_with($route, rtrim($publicRoute, '*')))) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Получить требуемые роли для роута
     */
    private static function getRequiredRoles(string $route): array
    {
        foreach (self::$protectedRoutes as $pattern => $roles) {
            if ($route === $pattern || 
                (str_ends_with($pattern, '*') && 
                 str_starts_with($route, rtrim($pattern, '*')))) {
                return $roles;
            }
        }
        return [];
    }
    
    /**
     * Проверить, является ли роут API
     */
    private static function isApiRoute(string $route): bool
    {
        return str_starts_with($route, '/api/');
    }
}