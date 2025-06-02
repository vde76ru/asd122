<?php
namespace App\Controllers;

use App\Core\Database;
use App\Core\Layout;
use App\Core\Logger;
use App\Services\DynamicProductDataService;
use App\Services\AuthService;
use App\Services\SearchService;

class ProductController
{
    /**
     * Просмотр одного товара с динамической загрузкой данных
     */
    public function viewAction()
    {
        $id = $_GET['id'] ?? null;
        if (!$id) {
            $this->show404();
            return;
        }
        
        $pdo = Database::getConnection();
        
        // 1. Получаем основные статические данные товара
        $stmt = $pdo->prepare("
            SELECT p.*, b.name AS brand_name, s.name AS series_name
            FROM products p
            LEFT JOIN brands b ON b.brand_id = p.brand_id
            LEFT JOIN series s ON s.series_id = p.series_id
            WHERE p.product_id = :id OR p.external_id = :id
            LIMIT 1
        ");
        $stmt->execute(['id' => $id]);
        $product = $stmt->fetch();
        
        if (!$product) {
            $this->show404();
            return;
        }
        
        $productId = $product['product_id'];
        
        // 2. Получаем дополнительные статические данные
        $images = $this->getProductImages($productId);
        $documents = $this->getProductDocuments($productId);
        $attributes = $this->getProductAttributes($productId);
        $related = $this->getRelatedProducts($productId);
        
        // 3. Получаем динамические данные (цены, остатки, доставка)
        $cityId = (int)($_COOKIE['selected_city_id'] ?? $_SESSION['city_id'] ?? 1);
        $userId = AuthService::check() ? AuthService::user()['id'] : null;
        
        $dynamicService = new DynamicProductDataService();
        $dynamicData = $dynamicService->getProductsDynamicData([$productId], $cityId, $userId);
        $productDynamic = $dynamicData[$productId] ?? [];
        
        // 4. Извлекаем данные для передачи в view
        $price = $productDynamic['price']['final'] ?? null;
        $basePrice = $productDynamic['price']['base'] ?? null;
        $hasSpecialPrice = $productDynamic['price']['has_special'] ?? false;
        $stock = $productDynamic['stock']['quantity'] ?? 0;
        $availableWarehouses = $productDynamic['stock']['warehouses'] ?? [];
        $deliveryInfo = $productDynamic['delivery'] ?? ['text' => 'Уточняйте'];
        
        // 5. Логируем просмотр товара для аналитики
        $this->logProductView($productId, $userId);
        
        // 6. Передаем все данные в view
        Layout::render('shop/product', [
            'product' => $product,
            'images' => $images,
            'documents' => $documents,
            'attributes' => $attributes,
            'price' => $price,
            'basePrice' => $basePrice,
            'hasSpecialPrice' => $hasSpecialPrice,
            'stock' => $stock,
            'availableWarehouses' => $availableWarehouses,
            'deliveryInfo' => $deliveryInfo,
            'related' => $related,
            'cityId' => $cityId,
            'productDynamic' => $productDynamic // Полные динамические данные для JS
        ]);
    }
    
    /**
     * Получение изображений товара
     */
    private function getProductImages(int $productId): array
    {
        $stmt = Database::query(
            "SELECT url, alt_text, is_main FROM product_images 
             WHERE product_id = ? 
             ORDER BY is_main DESC, sort_order ASC",
            [$productId]
        );
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }
    
    /**
     * Получение документов товара
     */
    private function getProductDocuments(int $productId): array
    {
        $stmt = Database::query(
            "SELECT * FROM product_documents 
             WHERE product_id = ? 
             ORDER BY type, document_id",
            [$productId]
        );
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }
    
    /**
     * Получение атрибутов товара
     */
    private function getProductAttributes(int $productId): array
    {
        $stmt = Database::query(
            "SELECT name, value, unit FROM product_attributes 
             WHERE product_id = ? 
             ORDER BY sort_order ASC",
            [$productId]
        );
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }
    
    /**
     * Получение связанных товаров
     */
    private function getRelatedProducts(int $productId): array
    {
        $stmt = Database::query(
            "SELECT p.product_id, p.name, p.external_id, p.sku,
                    pr.price as base_price,
                    COALESCE(pi.url, '/images/placeholder.jpg') as image_url
             FROM related_products rp 
             JOIN products p ON p.product_id = rp.related_id
             LEFT JOIN prices pr ON pr.product_id = p.product_id AND pr.is_base = 1
             LEFT JOIN product_images pi ON pi.product_id = p.product_id AND pi.is_main = 1
             WHERE rp.product_id = ?
             ORDER BY rp.sort_order, rp.relation_type",
            [$productId]
        );
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }
    
    /**
     * Логирование просмотра товара
     */
    private function logProductView(int $productId, ?int $userId): void
    {
        try {
            // Обновляем счетчик просмотров
            Database::query(
                "INSERT INTO product_metrics (product_id, views_count) 
                 VALUES (?, 1) 
                 ON DUPLICATE KEY UPDATE views_count = views_count + 1",
                [$productId]
            );
            
            // Логируем в audit_logs
            Database::query(
                "INSERT INTO audit_logs (user_id, session_id, action, object_type, object_id, created_at)
                 VALUES (?, ?, 'view', 'product', ?, NOW())",
                [$userId, session_id(), $productId]
            );
        } catch (\Exception $e) {
            Logger::warning('Failed to log product view', ['error' => $e->getMessage()]);
        }
    }
    
    /**
     * Показ 404 страницы
     */
    private function show404(): void
    {
        http_response_code(404);
        Layout::render('errors/404', []);
    }
    
    /**
     * AJAX endpoint для динамического обновления данных о товаре
     */
    public function ajaxProductInfoAction(): void
    {
        header('Content-Type: application/json; charset=utf-8');
        
        $productId = (int)($_GET['id'] ?? 0);
        $cityId = (int)($_GET['city_id'] ?? 1);
        
        if ($productId <= 0) {
            $this->jsonResponse(['success' => false, 'error' => 'Invalid product ID']);
            return;
        }
        
        $userId = AuthService::check() ? AuthService::user()['id'] : null;
        
        try {
            $dynamicService = new DynamicProductDataService();
            $dynamicData = $dynamicService->getProductsDynamicData([$productId], $cityId, $userId);
            
            $this->jsonResponse([
                'success' => true,
                'data' => $dynamicData[$productId] ?? []
            ]);
        } catch (\Exception $e) {
            Logger::error('Failed to get product info', ['error' => $e->getMessage()]);
            $this->jsonResponse(['success' => false, 'error' => 'Server error']);
        }
    }
    
    /**
     * Каталог товаров (список)
     */
    public function catalogAction(): void
    {
        Layout::render('shop/index', []);
    }
    
    /**
     * Поиск товаров
     */
    public function searchAction(): void
    {
        $query = trim($_GET['q'] ?? '');
        
        if (empty($query)) {
            Layout::render('shop/search', ['products' => [], 'query' => '']);
            return;
        }
        
        $params = [
            'q' => $query,
            'page' => (int)($_GET['page'] ?? 1),
            'limit' => (int)($_GET['limit'] ?? 20),
            'city_id' => (int)($_GET['city_id'] ?? 1),
            'sort' => $_GET['sort'] ?? 'relevance'
        ];
        
        if (AuthService::check()) {
            $params['user_id'] = AuthService::user()['id'];
        }
        
        $result = SearchService::search($params);
        
        Layout::render('shop/search', [
            'products' => $result['data']['products'] ?? [],
            'query' => $query,
            'total' => $result['data']['total'] ?? 0,
            'currentPage' => $params['page'],
            'totalPages' => ceil(($result['data']['total'] ?? 0) / $params['limit'])
        ]);
    }
    
    /**
     * Вспомогательный метод для JSON ответов
     */
    private function jsonResponse(array $data): void
    {
        echo json_encode($data, JSON_UNESCAPED_UNICODE);
        exit;
    }
}