<?php
namespace App\Controllers;

use App\Services\CartService;
use App\Services\AuthService;
use App\Core\CSRF;
use App\Core\Layout;

class CartController
{
    /**
     * POST /cart/add — добавить товар в корзину
     */
    public function addAction(): string
    {
        header('Content-Type: application/json; charset=utf-8');
        $productId = (int)($_POST['productId'] ?? $_POST['product_id'] ?? 0);
        $quantity  = (int)($_POST['quantity']  ?? 1);

        if ($productId <= 0 || $quantity <= 0) {
            http_response_code(400);
            return json_encode(['success' => false, 'message' => 'Некорректные данные']);
        }

        $userId = AuthService::check() ? AuthService::user()['id'] : null;

        try {
            CartService::add($productId, $quantity, $userId);
            return json_encode(['success' => true, 'message' => 'Товар добавлен в корзину']);
        } catch (\Exception $e) {
            http_response_code(400);
            return json_encode(['success' => false, 'message' => $e->getMessage()]);
        }
    }

    /**
     * GET /cart — страница корзины
     */
    public function viewAction(): void
    {
        $userId = AuthService::check() ? AuthService::user()['id'] : null;
        $data = CartService::getWithProducts($userId);
        
        $rows = [];
        foreach ($data['cart'] as $pid => $item) {
            $product = $data['products'][$pid] ?? null;
            if (!$product) continue;
            
            $rows[] = [
                'product_id' => $pid,
                'name' => $product['name'],
                'external_id' => $product['external_id'],
                'quantity' => $item['quantity'],
                'base_price' => $product['base_price'] ?? 0,
            ];
        }
    
        Layout::render('cart/view', [
            'cartRows' => $rows,
            'cart' => $data['cart'],
            'products' => $data['products']
        ]);
    }

    /**
     * POST /cart/remove — удалить товар из корзины
     */
    public function removeAction(): string
    {
        header('Content-Type: application/json; charset=utf-8');
        if ($_SERVER['REQUEST_METHOD'] !== 'POST' || !CSRF::validate($_POST['csrf_token'] ?? '')) {
            http_response_code(403);
            return json_encode(['success' => false, 'message' => 'Недоступно']);
        }

        $productId = (int)($_POST['productId'] ?? 0);
        if ($productId <= 0) {
            http_response_code(400);
            return json_encode(['success' => false, 'message' => 'Некорректные данные']);
        }

        $userId = AuthService::check() ? AuthService::user()['id'] : null;
        CartService::remove($productId, $userId);

        return json_encode(['success' => true, 'message' => 'Товар удален из корзины']);
    }

    /**
     * POST /cart/clear — очистить корзину
     */
    public function clearAction(): string
    {
        header('Content-Type: application/json; charset=utf-8');
        if ($_SERVER['REQUEST_METHOD'] !== 'POST' || !CSRF::validate($_POST['csrf_token'] ?? '')) {
            http_response_code(403);
            return json_encode(['success' => false, 'message' => 'Недоступно']);
        }

        $userId = AuthService::check() ? AuthService::user()['id'] : null;
        CartService::clear($userId);

        return json_encode(['success' => true, 'message' => 'Корзина очищена']);
    }

    /**
     * GET /cart/json — получить корзину в JSON формате
     */
    public function getJsonAction(): string
    {
        header('Content-Type: application/json; charset=utf-8');
        $userId = AuthService::check() ? AuthService::user()['id'] : null;
        $cart = CartService::get($userId);
        return json_encode(['cart' => $cart]);
    }
}