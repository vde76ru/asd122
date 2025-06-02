import { showToast } from "./utils.js";

/**
 * Сервис для работы с наличием товаров
 * Оптимизированная версия с батчингом и кешированием
 */
class AvailabilityService {
    constructor() {
        this.cache = new Map();
        this.cacheTimeout = 5 * 60 * 1000; // 5 минут
        this.batchSize = 100;
        this.apiUrl = '/api/availability';
    }

    /**
     * Загрузить данные о наличии для массива товаров
     */
    async loadAvailability(productIds) {
        if (!Array.isArray(productIds) || !productIds.length) {
            console.warn('loadAvailability: нет товаров для проверки');
            return {};
        }

        const cityId = this.getCurrentCityId();
        const uniqueIds = [...new Set(productIds)];
        
        // Проверяем кеш
        const cached = this.checkCache(uniqueIds, cityId);
        const idsToLoad = cached.missing;
        
        if (!idsToLoad.length) {
            this.updateUI(cached.data);
            return cached.data;
        }

        // Загружаем отсутствующие в кеше
        const loadedData = await this.fetchBatched(idsToLoad, cityId);
        
        // Объединяем с кешированными
        const allData = { ...cached.data, ...loadedData };
        
        // Обновляем UI
        this.updateUI(allData);
        
        return allData;
    }

    /**
     * Загрузка с разбивкой на батчи
     */
    async fetchBatched(productIds, cityId) {
        const batches = this.createBatches(productIds);
        const results = await Promise.all(
            batches.map(batch => this.fetchBatch(batch, cityId))
        );
        
        return Object.assign({}, ...results);
    }

    /**
     * Загрузка одного батча
     */
    async fetchBatch(productIds, cityId) {
        try {
            // Создаем параметры URL
            const params = new URLSearchParams({
                city_id: cityId,
                product_ids: productIds.join(',')
            });
            
            // Формируем URL с параметрами
            const url = `${this.apiUrl}?${params.toString()}`;
    
            // Правильный GET-запрос без тела
            const response = await fetch(url, {
                method: 'GET',
                headers: {
                    'Accept': 'application/json',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                credentials: 'same-origin'
            });
    
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
    
            const result = await response.json();
            
            if (result.success && result.data) {
                this.saveToCache(result.data, cityId);
                return result.data;
            }
            
            return {};
            
        } catch (error) {
            console.error('Ошибка загрузки батча:', error);
            return {};
        }
    }

    /**
     * Обновление UI элементов
     */
    updateUI(data) {
        Object.entries(data).forEach(([productId, info]) => {
            // Ищем элементы как по data-product-id атрибуту, так и по классам
            const rows = document.querySelectorAll(`[data-product-id="${productId}"]`);
            
            rows.forEach(row => {
                // Наличие - используем единый формат
                const availCells = row.querySelectorAll('.availability-cell, .col-availability span, .product-availability .availability-text');
                availCells.forEach(cell => {
                    if (cell) {
                        const text = info.availability_text || (info.quantity > 0 ? `${info.quantity} шт.` : 'Нет');
                        cell.textContent = text;
                        
                        // Обновляем классы для визуального отображения
                        if (cell.classList.contains('availability-cell') || cell.parentElement.classList.contains('col-availability')) {
                            cell.className = info.quantity > 10 ? 'text-success' : info.quantity > 0 ? 'text-warning' : 'text-danger';
                        }
                        
                        // Для карточек товаров
                        const availabilityBlock = cell.closest('.product-availability');
                        if (availabilityBlock) {
                            availabilityBlock.classList.remove('in-stock', 'out-of-stock');
                            availabilityBlock.classList.add(info.quantity > 0 ? 'in-stock' : 'out-of-stock');
                        }
                    }
                });
    
                // Дата доставки
                const dateCells = row.querySelectorAll('.delivery-date-cell, .col-delivery-date span');
                dateCells.forEach(cell => {
                    if (cell) {
                        cell.textContent = info.delivery_text || info.delivery_date || '—';
                    }
                });
                
                // Обновляем цены если они пришли
                if (info.price !== undefined && info.price !== null) {
                    const priceCells = row.querySelectorAll('.product-price, .col-price span');
                    priceCells.forEach(cell => {
                        if (cell) {
                            cell.textContent = `${info.price.toFixed(2)} ₽`;
                        }
                    });
                }
                
                // Обновляем кнопку "В корзину"
                const addToCartBtns = row.querySelectorAll('.btn-add-to-cart, .add-to-cart-btn');
                addToCartBtns.forEach(btn => {
                    if (btn) {
                        btn.disabled = info.quantity === 0;
                    }
                });
            });
        });
    }

    /**
     * Работа с кешем
     */
    checkCache(productIds, cityId) {
        const data = {};
        const missing = [];
        const now = Date.now();

        productIds.forEach(id => {
            const key = `${cityId}_${id}`;
            const cached = this.cache.get(key);
            
            if (cached && (now - cached.timestamp < this.cacheTimeout)) {
                data[id] = cached.data;
            } else {
                missing.push(id);
            }
        });

        return { data, missing };
    }

    saveToCache(data, cityId) {
        const now = Date.now();
        Object.entries(data).forEach(([productId, info]) => {
            const key = `${cityId}_${productId}`;
            this.cache.set(key, {
                data: info,
                timestamp: now
            });
        });

        // Ограничиваем размер кеша
        if (this.cache.size > 1000) {
            const oldestKeys = Array.from(this.cache.entries())
                .sort((a, b) => a[1].timestamp - b[1].timestamp)
                .slice(0, 100)
                .map(([key]) => key);
            
            oldestKeys.forEach(key => this.cache.delete(key));
        }
    }

    /**
     * Утилиты
     */
    getCurrentCityId() {
        return document.getElementById('citySelect')?.value || '1';
    }

    createBatches(items) {
        const batches = [];
        for (let i = 0; i < items.length; i += this.batchSize) {
            batches.push(items.slice(i, i + this.batchSize));
        }
        return batches;
    }

    clearCache() {
        this.cache.clear();
    }
}

// Экспорт синглтона
export const availabilityService = new AvailabilityService();

// Обратная совместимость
export async function loadAvailability(ids) {
    return availabilityService.loadAvailability(ids);
}