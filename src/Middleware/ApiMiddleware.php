<?php
namespace App\Middleware;

use App\Core\RateLimiter;
use App\Core\Logger;

class ApiMiddleware
{
    public static function handle(string $route, callable $next)
    {
        // Rate limiting
        $rateLimiter = new RateLimiter();
        $clientId = $_SESSION['user_id'] ?? $_SERVER['REMOTE_ADDR'];
        
        if (!$rateLimiter->check($clientId, 'api', 60, 100)) {
            http_response_code(429);
            echo json_encode(['error' => 'Too many requests']);
            exit;
        }
        
        // CORS headers
        header('Content-Type: application/json; charset=utf-8');
        header('X-Content-Type-Options: nosniff');
        
        // Logging
        $startTime = microtime(true);
        $result = $next();
        
        Logger::info('API request', [
            'route' => $route,
            'duration' => microtime(true) - $startTime
        ]);
        
        return $result;
    }
}