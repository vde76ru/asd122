<?php
namespace App\Services;

use App\Core\Database;
use App\Core\Cache;
use App\Core\Logger;
use PDO;

/**
 * Сервис для получения динамических данных товаров
 * - Цены (базовые, клиентские, акционные)
 * - Остатки по складам и городам
 * - Информация о доставке
 */
class DynamicProductDataService
{
    private const CACHE_TTL = 300; // 5 минут
    private const MAX_BATCH_SIZE = 1000;
    
    /**
     * Получить динамические данные для товаров
     */
    public function getProductsDynamicData(array $productIds, int $cityId, ?int $userId = null): array
    {
        try {
            // Валидация входных данных
            $productIds = array_values(array_unique(array_filter($productIds, 'is_numeric')));
            if (empty($productIds) || count($productIds) > self::MAX_BATCH_SIZE) {
                Logger::warning('Invalid product IDs', ['count' => count($productIds)]);
                return [];
            }

            // Проверка существования города
            if (!$this->validateCity($cityId)) {
                Logger::error('Invalid city', ['city_id' => $cityId]);
                // Возвращаем данные с дефолтными значениями
                return $this->getDefaultDataForProducts($productIds);
            }

            // Проверка кеша
            $cacheKey = $this->getCacheKey($productIds, $cityId, $userId);
            $cached = Cache::get($cacheKey);
            if ($cached !== null) {
                return $cached;
            }

            // Получаем организацию пользователя для клиентских цен
            $orgId = null;
            if ($userId) {
                $orgId = $this->getUserOrganization($userId);
            }

            // Собираем все данные
            $result = [];
            foreach ($productIds as $productId) {
                $result[$productId] = [
                    'price' => $this->getProductPrice($productId, $orgId),
                    'stock' => $this->getProductStock($productId, $cityId),
                    'delivery' => $this->getDeliveryInfo($productId, $cityId),
                    'available' => false // Будет обновлено ниже
                ];
                
                // Обновляем доступность на основе остатков
                $result[$productId]['available'] = $result[$productId]['stock']['quantity'] > 0;
            }

            // Кешируем результат
            Cache::set($cacheKey, $result, self::CACHE_TTL);
            
            return $result;
            
        } catch (\Exception $e) {
            Logger::error('DynamicProductDataService error', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            
            return $this->getDefaultDataForProducts($productIds);
        }
    }
    
    /**
     * Получить цену товара с учетом всех правил
     */
    private function getProductPrice(int $productId, ?int $orgId): array
    {
        $pdo = Database::getConnection();
        
        // 1. Сначала проверяем клиентскую цену (если есть организация)
        if ($orgId) {
            $stmt = $pdo->prepare("
                SELECT price 
                FROM client_prices 
                WHERE org_id = ? AND product_id = ? 
                AND (valid_to IS NULL OR valid_to >= CURDATE())
                ORDER BY valid_from DESC 
                LIMIT 1
            ");
            $stmt->execute([$orgId, $productId]);
            
            if ($clientPrice = $stmt->fetchColumn()) {
                return [
                    'base' => (float)$clientPrice,
                    'final' => (float)$clientPrice,
                    'has_special' => false,
                    'discount_percent' => 0,
                    'price_type' => 'client'
                ];
            }
        }
        
        // 2. Получаем базовую цену
        $stmt = $pdo->prepare("
            SELECT price 
            FROM prices 
            WHERE product_id = ? AND is_base = 1 
            AND (valid_to IS NULL OR valid_to >= CURDATE())
            ORDER BY valid_from DESC 
            LIMIT 1
        ");
        $stmt->execute([$productId]);
        $basePrice = (float)($stmt->fetchColumn() ?: 0);
        
        // 3. Проверяем акционную цену
        $stmt = $pdo->prepare("
            SELECT price, valid_from, valid_to 
            FROM prices 
            WHERE product_id = ? AND is_base = 0 
            AND valid_from <= NOW() 
            AND (valid_to IS NULL OR valid_to >= NOW())
            ORDER BY price ASC 
            LIMIT 1
        ");
        $stmt->execute([$productId]);
        $specialPrice = $stmt->fetch();
        
        if ($specialPrice && $specialPrice['price'] < $basePrice) {
            $discount = round((1 - $specialPrice['price'] / $basePrice) * 100);
            return [
                'base' => $basePrice,
                'final' => (float)$specialPrice['price'],
                'has_special' => true,
                'discount_percent' => $discount,
                'price_type' => 'special',
                'special_until' => $specialPrice['valid_to']
            ];
        }
        
        return [
            'base' => $basePrice,
            'final' => $basePrice,
            'has_special' => false,
            'discount_percent' => 0,
            'price_type' => 'base'
        ];
    }
    
    /**
     * Получить остатки товара для города
     */
    private function getProductStock(int $productId, int $cityId): array
    {
        $pdo = Database::getConnection();
        
        // Получаем склады города с учетом маппинга
        $stmt = $pdo->prepare("
            SELECT w.warehouse_id, w.name, w.address,
                   COALESCE(sb.quantity - sb.reserved, 0) as available
            FROM warehouses w
            INNER JOIN city_warehouse_mapping cwm ON w.warehouse_id = cwm.warehouse_id
            LEFT JOIN stock_balances sb ON sb.warehouse_id = w.warehouse_id AND sb.product_id = ?
            WHERE cwm.city_id = ? AND w.is_active = 1
            ORDER BY available DESC
        ");
        $stmt->execute([$productId, $cityId]);
        
        $warehouses = [];
        $totalQuantity = 0;
        
        while ($row = $stmt->fetch()) {
            if ($row['available'] > 0) {
                $warehouses[] = [
                    'id' => $row['warehouse_id'],
                    'name' => $row['name'],
                    'address' => $row['address'],
                    'quantity' => (int)$row['available']
                ];
                $totalQuantity += $row['available'];
            }
        }
        
        return [
            'quantity' => $totalQuantity,
            'warehouses' => $warehouses,
            'reserved' => $this->getReservedQuantity($productId)
        ];
    }
    
    /**
     * Получить информацию о доставке
     */
    private function getDeliveryInfo(int $productId, int $cityId): array
    {
        $pdo = Database::getConnection();
        
        // Проверяем наличие товара на складах города
        $stock = $this->getProductStock($productId, $cityId);
        
        if ($stock['quantity'] > 0) {
            // Товар есть в наличии - быстрая доставка
            $deliveryDate = $this->calculateDeliveryDate($cityId, true);
            return [
                'date' => $deliveryDate->format('d.m.Y'),
                'text' => $this->getDeliveryText($deliveryDate, true),
                'type' => 'stock',
                'days' => $this->getBusinessDaysDiff(new \DateTime(), $deliveryDate)
            ];
        }
        
        // Товар под заказ - проверяем доставку с центрального склада
        $stmt = $pdo->prepare("
            SELECT SUM(sb.quantity - sb.reserved) as total
            FROM stock_balances sb
            WHERE sb.product_id = ? AND sb.quantity > sb.reserved
        ");
        $stmt->execute([$productId]);
        
        $totalStock = (int)$stmt->fetchColumn();
        
        if ($totalStock > 0) {
            // Есть на других складах
            $deliveryDate = $this->calculateDeliveryDate($cityId, false);
            return [
                'date' => $deliveryDate->format('d.m.Y'),
                'text' => $this->getDeliveryText($deliveryDate, false),
                'type' => 'order',
                'days' => $this->getBusinessDaysDiff(new \DateTime(), $deliveryDate)
            ];
        }
        
        // Нет в наличии нигде
        return [
            'date' => null,
            'text' => 'Под заказ',
            'type' => 'request',
            'days' => null
        ];
    }
    
    /**
     * Рассчитать дату доставки с учетом рабочих дней и расписания
     */
    private function calculateDeliveryDate(int $cityId, bool $isStock): \DateTime
    {
        $date = new \DateTime();
        
        // Получаем настройки города
        $stmt = Database::query(
            "SELECT delivery_base_days, cutoff_time, working_days 
             FROM cities WHERE city_id = ?",
            [$cityId]
        );
        $city = $stmt->fetch();
        
        if (!$city) {
            // Дефолтные значения
            $baseDays = $isStock ? 1 : 3;
            $cutoffTime = '15:00:00';
            $workingDays = [1, 2, 3, 4, 5]; // Пн-Пт
        } else {
            $baseDays = $isStock ? 1 : ($city['delivery_base_days'] ?: 3);
            $cutoffTime = $city['cutoff_time'] ?: '15:00:00';
            $workingDays = json_decode($city['working_days'] ?: '[1,2,3,4,5]', true);
        }
        
        // Проверяем время отсечки
        $now = new \DateTime();
        $cutoff = new \DateTime($now->format('Y-m-d') . ' ' . $cutoffTime);
        
        if ($now > $cutoff) {
            // Заказ после времени отсечки - добавляем день
            $baseDays++;
        }
        
        // Добавляем рабочие дни
        $daysAdded = 0;
        while ($daysAdded < $baseDays) {
            $date->modify('+1 day');
            if (in_array($date->format('N'), $workingDays)) {
                $daysAdded++;
            }
        }
        
        return $date;
    }
    
    /**
     * Получить текст доставки
     */
    private function getDeliveryText(\DateTime $date, bool $isStock): string
    {
        $today = new \DateTime();
        $tomorrow = (clone $today)->modify('+1 day');
        
        if ($date->format('Y-m-d') === $today->format('Y-m-d')) {
            return 'Сегодня';
        } elseif ($date->format('Y-m-d') === $tomorrow->format('Y-m-d')) {
            return 'Завтра';
        } else {
            $days = $this->getBusinessDaysDiff($today, $date);
            if ($days <= 5) {
                return $date->format('d.m') . ' (' . $this->getDayName($date) . ')';
            } else {
                return $date->format('d.m.Y');
            }
        }
    }
    
    /**
     * Получить количество рабочих дней между датами
     */
    private function getBusinessDaysDiff(\DateTime $from, \DateTime $to): int
    {
        $days = 0;
        $current = clone $from;
        
        while ($current < $to) {
            if ($current->format('N') < 6) { // Пн-Пт
                $days++;
            }
            $current->modify('+1 day');
        }
        
        return $days;
    }
    
    /**
     * Получить название дня недели
     */
    private function getDayName(\DateTime $date): string
    {
        $days = ['Вс', 'Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб'];
        return $days[$date->format('w')];
    }
    
    /**
     * Получить зарезервированное количество товара
     */
    private function getReservedQuantity(int $productId): int
    {
        $stmt = Database::query(
            "SELECT SUM(reserved) FROM stock_balances WHERE product_id = ?",
            [$productId]
        );
        return (int)$stmt->fetchColumn();
    }
    
    /**
     * Получить организацию пользователя
     */
    private function getUserOrganization(int $userId): ?int
    {
        $stmt = Database::query(
            "SELECT org_id FROM clients_organizations WHERE user_id = ? LIMIT 1",
            [$userId]
        );
        $orgId = $stmt->fetchColumn();
        return $orgId ? (int)$orgId : null;
    }
    
    /**
     * Проверить существование города
     */
    private function validateCity(int $cityId): bool
    {
        $stmt = Database::query("SELECT 1 FROM cities WHERE city_id = ?", [$cityId]);
        return (bool)$stmt->fetch();
    }
    
    /**
     * Получить дефолтные данные для товаров
     */
    private function getDefaultDataForProducts(array $productIds): array
    {
        $result = [];
        
        foreach ($productIds as $productId) {
            $result[$productId] = [
                'price' => [
                    'base' => null,
                    'final' => null,
                    'has_special' => false,
                    'discount_percent' => 0,
                    'price_type' => 'none'
                ],
                'stock' => [
                    'quantity' => 0,
                    'warehouses' => [],
                    'reserved' => 0
                ],
                'delivery' => [
                    'date' => null,
                    'text' => 'Уточняйте',
                    'type' => 'unknown',
                    'days' => null
                ],
                'available' => false
            ];
        }
        
        return $result;
    }
    
    /**
     * Получить ключ кеша
     */
    private function getCacheKey(array $productIds, int $cityId, ?int $userId): string
    {
        sort($productIds);
        return 'dynamic:' . md5(implode(',', $productIds) . ':' . $cityId . ':' . ($userId ?? 0));
    }
}