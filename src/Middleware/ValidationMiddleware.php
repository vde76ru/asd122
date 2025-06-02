<?php
// src/Middleware/ValidationMiddleware.php
namespace App\Middleware;

use App\Core\Validator;
use App\Core\Logger;
use App\Exceptions\ValidationException;

/**
 * Middleware для валидации входящих данных
 * 
 * Этот класс - как охранник на входе. Он проверяет все данные,
 * которые приходят от пользователей, чтобы защитить наше приложение
 * от некорректных или вредоносных данных.
 */
class ValidationMiddleware
{
    /**
     * Правила валидации для разных роутов
     * Ключ - это путь (роут), значение - правила проверки
     */
    private static array $rules = [
        // Правила для корзины
        '/cart/add' => [
            'product_id' => 'required|integer|min:1',
            'quantity' => 'required|integer|min:1|max:9999',
            'csrf_token' => 'required|string'
        ],
        
        '/cart/update' => [
            'product_id' => 'required|integer|min:1',
            'quantity' => 'required|integer|min:0|max:9999',
            'csrf_token' => 'required|string'
        ],
        
        '/cart/remove' => [
            'product_id' => 'required|integer|min:1',
            'csrf_token' => 'required|string'
        ],
        
        // Правила для авторизации
        '/login' => [
            'username' => 'required|string|min:3|max:100',
            'password' => 'required|string|min:6',
            'csrf_token' => 'required|string'
        ],
        
        // Правила для поиска
        '/api/products' => [
            'q' => 'string|max:500',
            'page' => 'integer|min:1',
            'limit' => 'integer|min:1|max:100',
            'city_id' => 'integer|min:1',
            'sort' => 'string|in:relevance,name,price_asc,price_desc,popularity'
        ],
        
        // Правила для спецификаций
        '/specification/create' => [
            'csrf_token' => 'required|string',
            'name' => 'string|max:255',
            'comment' => 'string|max:1000'
        ]
    ];
    
    /**
     * Обработка запроса
     * 
     * @param string $route Текущий маршрут
     * @param callable $next Следующий обработчик
     * @return mixed
     */
    public static function handle(string $route, callable $next)
    {
        try {
            // Получаем правила для текущего роута
            $rules = self::getRulesForRoute($route);
            
            if (empty($rules)) {
                // Если правил нет, просто пропускаем дальше
                return $next();
            }
            
            // Получаем данные запроса
            $data = self::getRequestData();
            
            // Валидируем данные
            $validator = new Validator($data, $rules);
            
            if (!$validator->passes()) {
                // Если валидация не прошла, логируем и возвращаем ошибку
                Logger::warning('Validation failed', [
                    'route' => $route,
                    'errors' => $validator->errors(),
                    'data' => self::sanitizeForLog($data)
                ]);
                
                throw new ValidationException(
                    'Данные не прошли проверку',
                    $validator->errors()
                );
            }
            
            // Сохраняем провалидированные данные для использования в контроллерах
            $_REQUEST['validated'] = $validator->validated();
            
            // Передаем управление дальше
            return $next();
            
        } catch (ValidationException $e) {
            // Обрабатываем ошибки валидации
            self::handleValidationError($e);
        } catch (\Exception $e) {
            // Обрабатываем другие ошибки
            Logger::error('Validation middleware error', [
                'route' => $route,
                'error' => $e->getMessage()
            ]);
            
            throw $e;
        }
    }
    
    /**
     * Добавить правила валидации для роута
     * 
     * Это позволяет динамически добавлять правила из контроллеров
     */
    public static function addRules(string $route, array $rules): void
    {
        self::$rules[$route] = $rules;
    }
    
    /**
     * Получить правила для конкретного роута
     */
    private static function getRulesForRoute(string $route): array
    {
        // Сначала ищем точное совпадение
        if (isset(self::$rules[$route])) {
            return self::$rules[$route];
        }
        
        // Потом ищем по шаблону (например, /product/* для /product/123)
        foreach (self::$rules as $pattern => $rules) {
            if (self::matchRoute($pattern, $route)) {
                return $rules;
            }
        }
        
        return [];
    }
    
    /**
     * Проверка соответствия роута шаблону
     */
    private static function matchRoute(string $pattern, string $route): bool
    {
        // Простая проверка с поддержкой * в конце
        if (str_ends_with($pattern, '*')) {
            $prefix = rtrim($pattern, '*');
            return str_starts_with($route, $prefix);
        }
        
        return $pattern === $route;
    }
    
    /**
     * Получить данные запроса
     */
    private static function getRequestData(): array
    {
        $data = [];
        
        // Получаем данные в зависимости от метода запроса
        switch ($_SERVER['REQUEST_METHOD']) {
            case 'GET':
                $data = $_GET;
                break;
                
            case 'POST':
                // Проверяем Content-Type
                $contentType = $_SERVER['CONTENT_TYPE'] ?? '';
                
                if (str_contains($contentType, 'application/json')) {
                    // JSON запрос
                    $json = file_get_contents('php://input');
                    $data = json_decode($json, true) ?: [];
                } else {
                    // Обычный POST
                    $data = $_POST;
                }
                break;
                
            case 'PUT':
            case 'PATCH':
            case 'DELETE':
                // Для этих методов всегда читаем из php://input
                $input = file_get_contents('php://input');
                parse_str($input, $data);
                
                // Если это JSON, декодируем
                if (str_contains($_SERVER['CONTENT_TYPE'] ?? '', 'application/json')) {
                    $data = json_decode($input, true) ?: [];
                }
                break;
        }
        
        return $data;
    }
    
    /**
     * Санитизация данных для логирования
     * Удаляем чувствительные данные перед записью в лог
     */
    private static function sanitizeForLog(array $data): array
    {
        $sensitive = ['password', 'password_confirmation', 'csrf_token', 'api_key', 'token'];
        
        foreach ($sensitive as $key) {
            if (isset($data[$key])) {
                $data[$key] = '***';
            }
        }
        
        return $data;
    }
    
    /**
     * Обработка ошибок валидации
     */
    private static function handleValidationError(ValidationException $e): void
    {
        // Определяем формат ответа
        $isAjax = !empty($_SERVER['HTTP_X_REQUESTED_WITH']) && 
                  strtolower($_SERVER['HTTP_X_REQUESTED_WITH']) === 'xmlhttprequest';
        
        $acceptsJson = str_contains($_SERVER['HTTP_ACCEPT'] ?? '', 'application/json');
        
        if ($isAjax || $acceptsJson) {
            // JSON ответ для AJAX запросов
            http_response_code(422);
            header('Content-Type: application/json; charset=utf-8');
            echo json_encode([
                'success' => false,
                'message' => $e->getMessage(),
                'errors' => $e->getErrors()
            ], JSON_UNESCAPED_UNICODE);
            exit;
        } else {
            // Для обычных запросов - редирект с ошибками
            $_SESSION['validation_errors'] = $e->getErrors();
            $_SESSION['old_input'] = self::getRequestData();
            
            header('Location: ' . $_SERVER['HTTP_REFERER'] ?? '/');
            exit;
        }
    }
}