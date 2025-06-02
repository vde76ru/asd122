<?php
namespace App\Controllers;

use App\Services\SearchService;
use App\Services\DynamicProductDataService;
use App\DTO\ProductAvailabilityDTO;
use App\Services\AuthService;
use App\Core\Logger;

class ApiController extends BaseController
{
    /**
     * GET /api/availability - Получение данных о наличии товаров
     */
    public function availabilityAction(): void
    {
        $startTime = microtime(true);
        
        try {
            // Валидация параметров с более подробными проверками
            $cityId = (int)($_GET['city_id'] ?? 1);
            $productIdsStr = trim($_GET['product_ids'] ?? '');
            
            // 🔧 Более детальная валидация
            if ($cityId < 1 || $cityId > 999999) {
                Logger::warning('Invalid city_id provided', ['city_id' => $_GET['city_id'] ?? 'null']);
                $this->error('Неверный city_id (должен быть от 1 до 999999)', 400);
                return;
            }
            
            if (empty($productIdsStr)) {
                Logger::warning('Empty product_ids provided');
                $this->error('Параметр product_ids обязателен', 400);
                return;
            }
            
            // Парсим и валидируем product_ids
            $productIds = array_map('intval', explode(',', $productIdsStr));
            $productIds = array_filter($productIds, fn($id) => $id > 0 && $id < 999999999);
            $productIds = array_unique($productIds);
            
            if (empty($productIds)) {
                Logger::warning('No valid product_ids after parsing', ['original' => $productIdsStr]);
                $this->error('Нет валидных product_ids', 400);
                return;
            }
            
            if (count($productIds) > 1000) {
                Logger::warning('Too many product_ids requested', ['count' => count($productIds)]);
                $this->error('Слишком много товаров, максимум 1000', 400);
                return;
            }
            
            // Получаем динамические данные
            $dynamicService = new DynamicProductDataService();
            $userId = AuthService::check() ? AuthService::user()['id'] : null;
            
            Logger::debug('Fetching dynamic data', [
                'product_count' => count($productIds),
                'city_id' => $cityId,
                'user_id' => $userId
            ]);
            
            $dynamicData = $dynamicService->getProductsDynamicData($productIds, $cityId, $userId);
            
            // Преобразуем в DTO формат
            $result = [];
            foreach ($productIds as $productId) {
                $data = $dynamicData[$productId] ?? [];
                
                // 🔧 Добавляем проверку на корректность DTO
                try {
                    $dto = ProductAvailabilityDTO::fromDynamicData($productId, $data);
                    $result[$productId] = $dto->toArray();
                } catch (\Exception $e) {
                    Logger::warning('DTO conversion error', [
                        'product_id' => $productId,
                        'error' => $e->getMessage()
                    ]);
                    
                    // Возвращаем базовую структуру при ошибке DTO
                    $result[$productId] = [
                        'product_id' => $productId,
                        'available' => false,
                        'price' => null,
                        'stock' => null,
                        'delivery' => null,
                        'error' => 'Ошибка обработки данных'
                    ];
                }
            }
            
            // Добавляем метаинформацию
            $response = [
                'data' => $result,
                'meta' => [
                    'requested_count' => count($productIds),
                    'returned_count' => count($result),
                    'city_id' => $cityId,
                    'query_time_ms' => round((microtime(true) - $startTime) * 1000, 2)
                ]
            ];
            
            $this->success($response);
            
        } catch (\Exception $e) {
            Logger::error('API Availability error', [
                'error' => $e->getMessage(),
                'file' => $e->getFile(),
                'line' => $e->getLine(),
                'trace' => $e->getTraceAsString(),
                'request' => $_GET
            ]);
            
            $this->error('Ошибка получения данных о наличии: ' . $e->getMessage(), 500);
        }
    }
    
    /**
     * GET /api/search - Поиск товаров
     */
    public function searchAction(): void
    {
        $startTime = microtime(true);
        
        try {
            // Собираем и валидируем параметры
            $params = [
                'q' => trim($_GET['q'] ?? ''),
                'page' => max(1, (int)($_GET['page'] ?? 1)),
                'limit' => min(100, max(1, (int)($_GET['limit'] ?? 20))),
                'city_id' => max(1, (int)($_GET['city_id'] ?? 1)),
                'sort' => $_GET['sort'] ?? 'relevance'
            ];
            
            // Валидация сортировки
            $allowedSorts = ['relevance', 'name', 'external_id', 'price_asc', 'price_desc', 'availability', 'popularity'];
            if (!in_array($params['sort'], $allowedSorts)) {
                $params['sort'] = 'relevance';
            }
            
            // Добавляем пользователя если авторизован
            if (AuthService::check()) {
                $params['user_id'] = AuthService::user()['id'];
            }
            
            // Добавляем фильтры если они есть
            $filters = ['brand_name', 'series_name', 'category'];
            foreach ($filters as $filter) {
                if (!empty($_GET[$filter])) {
                    $params[$filter] = trim($_GET[$filter]);
                }
            }
            
            // 🔧 КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: Правильно обрабатываем ответ SearchService
            $searchResult = SearchService::search($params);
            
            // Проверяем структуру ответа
            if (isset($searchResult['success']) && $searchResult['success'] === true) {
                // ✅ Поиск прошел успешно
                $data = $searchResult['data'] ?? [];
                
                // Добавляем отладочную информацию
                $data['debug'] = [
                    'source' => $data['source'] ?? 'opensearch',
                    'query_time_ms' => round((microtime(true) - $startTime) * 1000, 2),
                    'params' => $params,
                    'fallback_used' => isset($searchResult['used_fallback']) ? $searchResult['used_fallback'] : false
                ];
                
                // Логируем успешный поиск
                if (!empty($params['q'])) {
                    Logger::info('Search completed successfully', [
                        'query' => $params['q'],
                        'results_count' => $data['total'] ?? 0,
                        'source' => $data['debug']['source'],
                        'city_id' => $params['city_id'],
                        'user_id' => $params['user_id'] ?? null
                    ]);
                }
                
                $this->success($data);
                
            } else {
                // ⚠️ SearchService вернул ошибку, но у него есть fallback данные
                $fallbackData = $searchResult['data'] ?? [];
                
                Logger::warning('Search service returned error, using fallback', [
                    'error' => $searchResult['error'] ?? 'Unknown error',
                    'error_code' => $searchResult['error_code'] ?? 'UNKNOWN',
                    'has_fallback_data' => !empty($fallbackData['products']),
                    'params' => $params
                ]);
                
                // Возвращаем данные из fallback с предупреждением
                $responseData = [
                    'products' => $fallbackData['products'] ?? [],
                    'total' => $fallbackData['total'] ?? 0,
                    'page' => $fallbackData['page'] ?? $params['page'],
                    'limit' => $fallbackData['limit'] ?? $params['limit'],
                    'warning' => 'Используется резервный режим поиска',
                    'debug' => [
                        'source' => 'fallback',
                        'original_error' => $searchResult['error'] ?? 'Unknown',
                        'error_code' => $searchResult['error_code'] ?? 'UNKNOWN',
                        'query_time_ms' => round((microtime(true) - $startTime) * 1000, 2),
                        'fallback_available' => true
                    ]
                ];
                
                $this->success($responseData);
            }
            
        } catch (\Exception $e) {
            // 🚨 Критическая ошибка - логируем и возвращаем пустой результат
            Logger::error('API Search critical error', [
                'error' => $e->getMessage(),
                'file' => $e->getFile(),
                'line' => $e->getLine(),
                'params' => $_GET,
                'trace' => $e->getTraceAsString()
            ]);
            
            // Всегда возвращаем валидную структуру для UI
            $this->success([
                'products' => [],
                'total' => 0,
                'page' => (int)($_GET['page'] ?? 1),
                'limit' => (int)($_GET['limit'] ?? 20),
                'error' => 'Критическая ошибка поиска',
                'debug' => [
                    'source' => 'error_fallback',
                    'exception' => $e->getMessage(),
                    'file' => basename($e->getFile()),
                    'line' => $e->getLine(),
                    'query_time_ms' => round((microtime(true) - $startTime) * 1000, 2)
                ]
            ]);
        }
    }
    
    /**
     * GET /api/autocomplete - Автодополнение поиска
     */
    public function autocompleteAction(): void
    {
        try {
            $query = trim($_GET['q'] ?? '');
            $limit = min(20, max(1, (int)($_GET['limit'] ?? 10)));
            
            if (strlen($query) < 1) {
                $this->success(['suggestions' => []]);
                return;
            }
            
            // Убираем потенциально опасные символы
            $query = preg_replace('/[^\p{L}\p{N}\s\-_.]/u', '', $query);
            
            if (empty($query)) {
                $this->success(['suggestions' => []]);
                return;
            }
            
            $suggestions = SearchService::autocomplete($query, $limit);
            
            $this->success(['suggestions' => $suggestions]);
            
        } catch (\Exception $e) {
            Logger::warning('API Autocomplete error', [
                'error' => $e->getMessage(),
                'query' => $_GET['q'] ?? ''
            ]);
            
            // Не ломаем UI при ошибках автодополнения
            $this->success(['suggestions' => []]);
        }
    }
    
    /**
     * GET /api/test - Тестовый endpoint
     */
    public function testAction(): void
    {
        $startTime = microtime(true);
        
        // Проверяем различные компоненты системы
        $opensearchStatus = $this->checkOpenSearchStatus();
        
        // Тестируем поиск
        $searchTest = null;
        try {
            $testResult = SearchService::search([
                'q' => '',
                'page' => 1,
                'limit' => 1,
                'city_id' => 1
            ]);
            
            $searchTest = [
                'success' => $testResult['success'] ?? false,
                'has_data' => !empty($testResult['data']['products'] ?? []),
                'source' => $testResult['data']['source'] ?? 'unknown'
            ];
        } catch (\Exception $e) {
            $searchTest = [
                'success' => false,
                'error' => $e->getMessage()
            ];
        }
        
        $response = [
            'message' => 'API работает корректно',
            'timestamp' => date('Y-m-d H:i:s'),
            'query_time_ms' => round((microtime(true) - $startTime) * 1000, 2),
            'status' => [
                'user_authenticated' => AuthService::check(),
                'opensearch_available' => $opensearchStatus,
                'search_service' => $searchTest,
                'memory_usage' => round(memory_get_usage(true) / 1024 / 1024, 2) . ' MB',
                'peak_memory' => round(memory_get_peak_usage(true) / 1024 / 1024, 2) . ' MB'
            ],
            'version' => [
                'php' => PHP_VERSION,
                'server' => $_SERVER['SERVER_SOFTWARE'] ?? 'unknown'
            ]
        ];
        
        $this->success($response);
    }
    
    /**
     * Проверка статуса OpenSearch
     */
    private function checkOpenSearchStatus(): bool
    {
        try {
            // 🔧 Используем тот же клиент что и SearchService для консистентности
            $client = \OpenSearch\ClientBuilder::create()
                ->setHosts(['localhost:9200'])
                ->setConnectionParams([
                    'timeout' => 3,           // Короткий timeout для быстрого ответа
                    'connect_timeout' => 2
                ])
                ->setRetries(1)               // Только одна попытка
                ->build();
            
            // Проверяем и статус кластера
            $health = $client->cluster()->health([
                'timeout' => '2s'
            ]);
            
            $isHealthy = in_array($health['status'] ?? 'red', ['green', 'yellow']);
            
            Logger::debug('OpenSearch status check', [
                'status' => $health['status'] ?? 'unknown',
                'is_healthy' => $isHealthy
            ]);
            
            return $isHealthy;
            
        } catch (\Exception $e) {
            Logger::debug('OpenSearch status check failed', [
                'error' => $e->getMessage()
            ]);
            return false;
        }
    }
}