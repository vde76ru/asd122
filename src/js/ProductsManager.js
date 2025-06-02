// src/js/ProductsManager.js
// Исправленный менеджер для работы с товарами

import { showToast, showLoadingIndicator, hideLoadingIndicator } from './utils.js';

// ===== API Сервис =====
class ProductAPIService {
    constructor() {
        this.baseUrl = '/api';
        this.activeRequests = new Map();
        this.cache = new Map();
        this.cacheTimeout = 60000; // 1 минута
    }

    async search(params) {
        const url = new URL(`${this.baseUrl}/search`, window.location.origin);
        
        // Очищаем пустые параметры
        Object.entries(params).forEach(([key, value]) => {
            if (value !== null && value !== undefined && value !== '') {
                url.searchParams.append(key, value);
            }
        });

        const requestKey = url.toString();
        
        // Проверяем кеш
        const cached = this.getFromCache(requestKey);
        if (cached) {
            console.log('📦 Using cached results for:', requestKey);
            return cached;
        }

        // Проверяем активные запросы
        if (this.activeRequests.has(requestKey)) {
            console.log('⏳ Waiting for existing request:', requestKey);
            return this.activeRequests.get(requestKey);
        }

        try {
            const requestPromise = fetch(url, {
                headers: {
                    'Accept': 'application/json',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                signal: AbortSignal.timeout(30000) // 30 секунд таймаут
            }).then(async response => {
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                return response.json();
            });

            this.activeRequests.set(requestKey, requestPromise);
            const result = await requestPromise;
            
            // Сохраняем в кеш только успешные результаты
            if (result.success || result.data) {
                this.saveToCache(requestKey, result);
            }
            
            this.activeRequests.delete(requestKey);
            return result;

        } catch (error) {
            this.activeRequests.delete(requestKey);
            console.error('Search API error:', error);
            
            // Возвращаем структуру с пустыми данными
            return {
                success: false,
                error: error.message,
                data: { 
                    products: [], 
                    total: 0,
                    page: params.page || 1,
                    limit: params.limit || 20
                }
            };
        }
    }

    getFromCache(key) {
        const cached = this.cache.get(key);
        if (cached && (Date.now() - cached.timestamp < this.cacheTimeout)) {
            return cached.data;
        }
        this.cache.delete(key);
        return null;
    }

    saveToCache(key, data) {
        this.cache.set(key, {
            data: data,
            timestamp: Date.now()
        });
        
        // Ограничиваем размер кеша
        if (this.cache.size > 50) {
            const firstKey = this.cache.keys().next().value;
            this.cache.delete(firstKey);
        }
    }

    clearCache() {
        this.cache.clear();
    }
}

// ===== Главный менеджер =====
export class ProductsManager {
    constructor() {
        if (window.__productsManagerInstance) {
            console.warn('ProductsManager already exists, returning existing instance');
            return window.__productsManagerInstance;
        }

        this.api = new ProductAPIService();
        
        // Состояние
        this.products = [];
        this.currentPage = 1;
        this.itemsPerPage = 20;
        this.totalProducts = 0;
        this.sortColumn = 'name';
        this.sortDirection = 'asc';
        this.appliedFilters = {};
        this.isLoading = false;
        this.searchDebounceTimer = null;
        this.initialized = false;

        // Восстанавливаем состояние из sessionStorage
        this.restoreState();
        
        this.init();
        
        window.__productsManagerInstance = this;
    }

    init() {
        if (this.initialized) {
            console.warn('⚠️ ProductsManager already initialized');
            return;
        }
        
        // Проверяем параметры URL при инициализации
        this.checkUrlParams();
        
        this.bindEvents();
        this.syncGlobalVariables();
        this.initialized = true;
        
        console.log('✅ ProductsManager initialized');
    }

    checkUrlParams() {
        const urlParams = new URLSearchParams(window.location.search);
        
        // Обрабатываем параметр search из URL (от глобального поиска)
        const searchParam = urlParams.get('search');
        if (searchParam) {
            this.appliedFilters.search = searchParam;
            sessionStorage.setItem('search', searchParam);
            
            // Обновляем поле поиска если оно есть
            const searchInput = document.getElementById('searchInput');
            if (searchInput) {
                searchInput.value = searchParam;
            }
            
            // Убираем параметр из URL чтобы не дублировать
            const newUrl = new URL(window.location);
            newUrl.searchParams.delete('search');
            window.history.replaceState({}, '', newUrl);
        }
        
        // Обрабатываем другие параметры
        const page = urlParams.get('page');
        if (page) {
            this.currentPage = parseInt(page) || 1;
        }
        
        const limit = urlParams.get('limit');
        if (limit) {
            this.itemsPerPage = parseInt(limit) || 20;
        }
    }

    restoreState() {
        // Восстанавливаем настройки
        this.currentPage = parseInt(sessionStorage.getItem('currentPage') || '1');
        this.itemsPerPage = parseInt(sessionStorage.getItem('itemsPerPage') || '20');
        this.sortColumn = sessionStorage.getItem('sortColumn') || 'name';
        this.sortDirection = sessionStorage.getItem('sortDirection') || 'asc';
        
        // Восстанавливаем фильтры
        Object.keys(sessionStorage).forEach(key => {
            if (!['itemsPerPage', 'sortColumn', 'sortDirection', 'currentPage'].includes(key)) {
                this.appliedFilters[key] = sessionStorage.getItem(key);
            }
        });
    }

    bindEvents() {
        // Поиск с дебаунсом
        const searchInput = document.getElementById('searchInput');
        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                clearTimeout(this.searchDebounceTimer);
                const value = e.target.value.trim();
                
                this.searchDebounceTimer = setTimeout(() => {
                    if (value !== (this.appliedFilters.search || '')) {
                        if (value) {
                            this.appliedFilters.search = value;
                            sessionStorage.setItem('search', value);
                        } else {
                            delete this.appliedFilters.search;
                            sessionStorage.removeItem('search');
                        }
                        
                        this.currentPage = 1;
                        this.fetchProducts();
                    }
                }, 300);
            });

            // Устанавливаем начальное значение
            if (this.appliedFilters.search) {
                searchInput.value = this.appliedFilters.search;
            }
        }

        // Количество на странице
        ['itemsPerPageSelect', 'itemsPerPageSelectBottom'].forEach(id => {
            const el = document.getElementById(id);
            if (el) {
                el.value = this.itemsPerPage;
                el.addEventListener('change', (e) => {
                    this.itemsPerPage = parseInt(e.target.value);
                    sessionStorage.setItem('itemsPerPage', this.itemsPerPage);
                    this.currentPage = 1;
                    this.fetchProducts();
                });
            }
        });

        // Выбор города
        const citySelect = document.getElementById('citySelect');
        if (citySelect) {
            citySelect.addEventListener('change', () => {
                // Очищаем кеш при смене города
                this.api.clearCache();
                this.fetchProducts(true);
            });
        }
    }

    syncGlobalVariables() {
        // Синхронизация с глобальными переменными для обратной совместимости
        window.productsData = this.products;
        window.currentPage = this.currentPage;
        window.itemsPerPage = this.itemsPerPage;
        window.totalProducts = this.totalProducts;
        window.sortColumn = this.sortColumn;
        window.sortDirection = this.sortDirection;
        window.appliedFilters = this.appliedFilters;
    }

    async fetchProducts(force = false) {
        if (this.isLoading && !force) {
            console.log('⏳ Fetch already in progress');
            return;
        }
        
        this.isLoading = true;
        showLoadingIndicator();
        
        try {
            // Собираем параметры для API
            const params = {
                q: this.appliedFilters.search || '',
                page: this.currentPage,
                limit: this.itemsPerPage,
                sort: this.convertSortToApiFormat(),
                city_id: document.getElementById('citySelect')?.value || '1'
            };
            
            // Добавляем другие фильтры
            ['brand_name', 'series_name', 'category'].forEach(filter => {
                if (this.appliedFilters[filter]) {
                    params[filter] = this.appliedFilters[filter];
                }
            });
            
            console.log('🔍 Fetching products with params:', params);
            
            const result = await this.api.search(params);
            
            // Обрабатываем результат
            if (result.success !== false) {
                const data = result.data || result;
                this.products = data.products || [];
                this.totalProducts = data.total || 0;
                
                console.log(`✅ Loaded ${this.products.length} products, total: ${this.totalProducts}`);
                
                this.syncGlobalVariables();
                this.renderProducts();
                this.updatePaginationDisplay();
                
                // Загружаем динамические данные если есть товары
                if (this.products.length > 0) {
                    this.loadDynamicData();
                }
            } else {
                console.error('❌ Search failed:', result.error);
                this.handleError(result.error || 'Неизвестная ошибка');
            }
            
        } catch (error) {
            console.error('❌ Fetch error:', error);
            this.handleError(error.message);
        } finally {
            this.isLoading = false;
            hideLoadingIndicator();
        }
    }

    async loadDynamicData() {
        const productIds = this.products.map(p => p.product_id).filter(id => id > 0);
        if (productIds.length === 0) return;
        
        // Используем существующий сервис для загрузки наличия
        if (window.loadAvailability) {
            try {
                await window.loadAvailability(productIds);
            } catch (error) {
                console.error('Failed to load availability:', error);
            }
        }
    }

    renderProducts() {
        if (typeof window.renderProductsTable === 'function') {
            window.renderProductsTable();
        } else {
            console.error('renderProductsTable function not found');
        }
    }

    handleError(message) {
        this.products = [];
        this.totalProducts = 0;
        this.syncGlobalVariables();
        this.renderProducts();
        showToast('Ошибка: ' + message, true);
    }

    convertSortToApiFormat() {
        // Конвертируем сортировку в формат API
        const sortMap = {
            'name': 'name',
            'external_id': 'external_id',
            'price': this.sortDirection === 'asc' ? 'price_asc' : 'price_desc',
            'base_price': this.sortDirection === 'asc' ? 'price_asc' : 'price_desc',
            'availability': 'availability',
            'orders_count': 'popularity'
        };
        
        return sortMap[this.sortColumn] || 'relevance';
    }

    updatePaginationDisplay() {
        const totalPages = Math.ceil(this.totalProducts / this.itemsPerPage);
        
        // Обновляем все элементы пагинации
        document.querySelectorAll('#currentPage, #currentPageBottom').forEach(el => {
            if (el) el.textContent = this.currentPage;
        });
        
        document.querySelectorAll('#totalPages, #totalPagesBottom').forEach(el => {
            if (el) el.textContent = totalPages;
        });
        
        document.querySelectorAll('#totalProductsText, #totalProductsTextBottom').forEach(el => {
            if (el) el.textContent = `Найдено товаров: ${this.totalProducts}`;
        });
        
        // Управление кнопками
        document.querySelectorAll('.prev-btn').forEach(btn => {
            if (btn) btn.disabled = this.currentPage <= 1;
        });
        
        document.querySelectorAll('.next-btn').forEach(btn => {
            if (btn) btn.disabled = this.currentPage >= totalPages;
        });
        
        // Обновляем поля ввода страницы
        document.querySelectorAll('#pageInput, #pageInputBottom').forEach(el => {
            if (el) el.value = this.currentPage;
        });
    }

    sortProducts(column) {
        if (this.sortColumn === column) {
            this.sortDirection = this.sortDirection === 'asc' ? 'desc' : 'asc';
        } else {
            this.sortColumn = column;
            this.sortDirection = 'asc';
        }
        
        sessionStorage.setItem('sortColumn', this.sortColumn);
        sessionStorage.setItem('sortDirection', this.sortDirection);
        
        this.syncGlobalVariables();
        this.currentPage = 1;
        this.fetchProducts();
    }

    loadPage(page) {
        const totalPages = Math.ceil(this.totalProducts / this.itemsPerPage);
        page = Math.max(1, Math.min(page, totalPages));
        
        if (this.currentPage === page) return;
        
        this.currentPage = page;
        sessionStorage.setItem('currentPage', page);
        this.syncGlobalVariables();
        this.fetchProducts();
    }

    applyFilter(key, value) {
        if (value) {
            this.appliedFilters[key] = value;
            sessionStorage.setItem(key, value);
        } else {
            delete this.appliedFilters[key];
            sessionStorage.removeItem(key);
        }
        
        this.currentPage = 1;
        this.api.clearCache(); // Очищаем кеш при изменении фильтров
        this.fetchProducts();
    }

    clearAllFilters() {
        // Очищаем все фильтры
        Object.keys(this.appliedFilters).forEach(key => {
            sessionStorage.removeItem(key);
        });
        this.appliedFilters = {};
        
        // Очищаем поле поиска
        const searchInput = document.getElementById('searchInput');
        if (searchInput) {
            searchInput.value = '';
        }
        
        this.currentPage = 1;
        this.api.clearCache();
        this.fetchProducts();
    }
}

// Экспорт синглтона
export const productsManager = new ProductsManager();