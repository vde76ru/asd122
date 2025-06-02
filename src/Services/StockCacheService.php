<?php
namespace App\Services;

use App\Core\Cache;
use App\Core\Database;

class StockCacheService 
{
    const CACHE_TTL = 300; // 5 минут
    
    public static function getStock(int $productId): array 
    {
        $cacheKey = "stock_{$productId}";
        $cached = Cache::get($cacheKey);
        
        if ($cached !== null) {
            return $cached;
        }
        
        return self::updateProductStock($productId);
    }
    
    public static function updateProductStock(int $productId): array 
    {
        $stmt = Database::query("
            SELECT 
                SUM(CASE WHEN sb.quantity > sb.reserved THEN sb.quantity - sb.reserved ELSE 0 END) as total,
                GROUP_CONCAT(DISTINCT cwm.city_id) as cities
            FROM stock_balances sb
            LEFT JOIN city_warehouse_mapping cwm ON sb.warehouse_id = cwm.warehouse_id
            WHERE sb.product_id = ?
        ", [$productId]);
        
        $data = $stmt->fetch();
        
        $result = [
            'total' => (int)($data['total'] ?? 0),
            'cities' => $data['cities'] ? array_map('intval', explode(',', $data['cities'])) : [],
            'has_stock' => (int)($data['total'] ?? 0) > 0
        ];
        
        Cache::set("stock_{$productId}", $result, self::CACHE_TTL);
        
        return $result;
    }
}