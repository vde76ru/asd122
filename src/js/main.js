// src/js/main.js

import "../css/main.css";
import "../css/shop.css";

// ===== ПРОВЕРКА НА ПОВТОРНУЮ ИНИЦИАЛИЗАЦИЮ =====
if (window.__APP_INITIALIZED__) {
    console.warn('⚠️ App already initialized, preventing duplicate initialization');
    throw new Error('App already initialized');
}
window.__APP_INITIALIZED__ = true;

// ===== ИМПОРТЫ МОДУЛЕЙ =====
import { showToast } from './utils.js';
import { changeItemsPerPage, changePage, handlePageInputKeydown } from './pagination.js';
import { filterByBrandOrSeries, applyFilters, clearAllFilters } from './filters.js';
import { loadAvailability } from './availability.js';
import { addToCart, clearCart, removeFromCart, fetchCart, updateCartItem } from './cart.js';
import { renderProductsTable, copyText } from './renderProducts.js';
import { createSpecification } from './specification.js';
import { productsManager } from './ProductsManager.js';
import { cartBadge } from './cart-badge.js';
import { exportToWindow } from './utils/globalExports.js';

// ===== ЭКСПОРТ ГЛОБАЛЬНЫХ ФУНКЦИЙ =====
exportToWindow({
    showToast,
    renderProductsTable,
    copyText,
    createSpecification,
    loadAvailability,
    addToCart,
    clearCart,
    removeFromCart,
    fetchCart,
    filterByBrandOrSeries,
    applyFilters,
    clearAllFilters,
    productsManager,
    fetchProducts: () => productsManager.fetchProducts(),
    sortProducts: (column) => productsManager.sortProducts(column),
    loadPage: (page) => productsManager.loadPage(page),
    updateCartBadge: () => cartBadge.update(),
    updateCartItem
});

// ===== CSRF TOKEN =====
window.CSRF_TOKEN = window.APP_CONFIG?.csrfToken || '';

// ===== ПОИСК =====
class SearchManager {
    constructor() {
        this.searchInput = null;
        this.globalSearchInput = null;
        this.searchSuggestions = null;
        this.initialized = false;
        this.currentSuggestionIndex = -1;
    }

    init() {
        if (this.initialized) {
            console.warn('SearchManager already initialized');
            return;
        }
        
        this.searchInput = document.getElementById('searchInput');
        this.globalSearchInput = document.getElementById('globalSearch');
        
        if (this.globalSearchInput) {
            this.setupGlobalSearch(this.globalSearchInput);
            this.createSuggestionsDropdown();
        }
        
        this.initialized = true;
    }

    createSuggestionsDropdown() {
        // Создаем контейнер для подсказок
        this.searchSuggestions = document.createElement('div');
        this.searchSuggestions.className = 'search-suggestions';
        this.searchSuggestions.style.cssText = `
            position: absolute;
            top: 100%;
            left: 0;
            right: 0;
            background: white;
            border: 1px solid #e1e8ed;
            border-top: none;
            border-radius: 0 0 8px 8px;
            max-height: 400px;
            overflow-y: auto;
            display: none;
            z-index: 1000;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        `;
        
        // Позиционируем относительно поля поиска
        const searchBox = this.globalSearchInput.parentElement;
        searchBox.style.position = 'relative';
        searchBox.appendChild(this.searchSuggestions);
    }

    setupGlobalSearch(input) {
        let debounceTimer = null;
        
        // Обработка ввода с автодополнением
        input.addEventListener('input', (e) => {
            clearTimeout(debounceTimer);
            const query = e.target.value.trim();
            
            if (query.length < 2) {
                this.hideSuggestions();
                return;
            }
            
            debounceTimer = setTimeout(() => {
                this.fetchSuggestions(query);
            }, 300);
        });
        
        // Обработка Enter
        input.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                
                // Если выбрана подсказка
                if (this.currentSuggestionIndex >= 0) {
                    const suggestions = this.searchSuggestions.querySelectorAll('.suggestion-item');
                    if (suggestions[this.currentSuggestionIndex]) {
                        suggestions[this.currentSuggestionIndex].click();
                        return;
                    }
                }
                
                // Иначе обычный поиск
                const query = input.value.trim();
                if (query) {
                    this.performSearch(query);
                }
            }
            
            // Навигация по подсказкам
            if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
                e.preventDefault();
                this.navigateSuggestions(e.key === 'ArrowDown' ? 1 : -1);
            }
            
            // Закрытие по Escape
            if (e.key === 'Escape') {
                this.hideSuggestions();
                input.blur();
            }
        });
        
        // Закрытие при клике вне
        document.addEventListener('click', (e) => {
            if (!input.contains(e.target) && !this.searchSuggestions.contains(e.target)) {
                this.hideSuggestions();
            }
        });
        
        // Фокус показывает историю
        input.addEventListener('focus', () => {
            const query = input.value.trim();
            if (query.length < 2) {
                this.showSearchHistory();
            }
        });
    }

    async fetchSuggestions(query) {
        try {
            const response = await fetch(`/api/autocomplete?q=${encodeURIComponent(query)}&limit=10`);
            const data = await response.json();
            
            if (data.success && data.suggestions) {
                this.showSuggestions(data.suggestions, query);
            }
        } catch (error) {
            console.error('Autocomplete error:', error);
        }
    }

    showSuggestions(suggestions, query) {
        this.searchSuggestions.innerHTML = '';
        this.currentSuggestionIndex = -1;
        
        if (suggestions.length === 0) {
            this.searchSuggestions.innerHTML = `
                <div style="padding: 15px; color: #666;">
                    Ничего не найдено
                </div>
            `;
            this.searchSuggestions.style.display = 'block';
            return;
        }
        
        // Группируем подсказки по типу
        const grouped = this.groupSuggestions(suggestions);
        
        // Отображаем группы
        Object.entries(grouped).forEach(([type, items]) => {
            if (items.length === 0) return;
            
            // Заголовок группы
            const header = document.createElement('div');
            header.style.cssText = `
                padding: 8px 15px;
                font-size: 12px;
                color: #666;
                background: #f5f5f5;
                font-weight: 600;
                text-transform: uppercase;
            `;
            header.textContent = this.getGroupTitle(type);
            this.searchSuggestions.appendChild(header);
            
            // Элементы группы
            items.forEach(suggestion => {
                const item = this.createSuggestionItem(suggestion, query);
                this.searchSuggestions.appendChild(item);
            });
        });
        
        this.searchSuggestions.style.display = 'block';
    }

    groupSuggestions(suggestions) {
        const groups = {
            history: [],
            product: [],
            suggest: []
        };
        
        suggestions.forEach(s => {
            const type = s.type || 'suggest';
            if (groups[type]) {
                groups[type].push(s);
            }
        });
        
        return groups;
    }

    getGroupTitle(type) {
        const titles = {
            history: 'История поиска',
            product: 'Товары',
            suggest: 'Предложения'
        };
        return titles[type] || type;
    }

    createSuggestionItem(suggestion, query) {
        const item = document.createElement('div');
        item.className = 'suggestion-item';
        item.style.cssText = `
            padding: 12px 15px;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 10px;
            transition: background 0.2s;
            border-bottom: 1px solid #f0f0f0;
        `;
        
        // Иконка
        const icon = document.createElement('span');
        icon.style.cssText = 'width: 20px; flex-shrink: 0; opacity: 0.6;';
        icon.innerHTML = this.getSuggestionIcon(suggestion.type);
        
        // Текст с подсветкой
        const text = document.createElement('span');
        text.style.cssText = 'flex: 1;';
        text.innerHTML = this.highlightText(suggestion.text, query);
        
        // Дополнительная информация
        if (suggestion.external_id) {
            const code = document.createElement('span');
            code.style.cssText = 'font-size: 12px; color: #666;';
            code.textContent = suggestion.external_id;
            item.appendChild(code);
        }
        
        item.appendChild(icon);
        item.appendChild(text);
        
        // Hover эффект
        item.addEventListener('mouseenter', () => {
            item.style.background = '#f5f5f5';
        });
        item.addEventListener('mouseleave', () => {
            item.style.background = '';
        });
        
        // Клик
        item.addEventListener('click', () => {
            this.globalSearchInput.value = suggestion.text;
            this.performSearch(suggestion.text);
        });
        
        return item;
    }

    getSuggestionIcon(type) {
        const icons = {
            history: '<svg width="16" height="16" fill="currentColor" viewBox="0 0 20 20"><path d="M10 18a8 8 0 100-16 8 8 0 000 16zm1-12a1 1 0 10-2 0v4a1 1 0 00.293.707l2.828 2.829a1 1 0 101.415-1.415L11 9.586V6z"/></svg>',
            product: '<svg width="16" height="16" fill="currentColor" viewBox="0 0 20 20"><path d="M4 3a2 2 0 100 4h12a2 2 0 100-4H4z"/><path d="M3 8h14v7a2 2 0 01-2 2H5a2 2 0 01-2-2V8zm5 3a1 1 0 011-1h2a1 1 0 110 2H9a1 1 0 01-1-1z"/></svg>',
            suggest: '<svg width="16" height="16" fill="currentColor" viewBox="0 0 20 20"><path d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 0 1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 0 012 8z"/></svg>'
        };
        return icons[type] || icons.suggest;
    }

    highlightText(text, query) {
        if (!query) return text;
        
        const regex = new RegExp(`(${this.escapeRegex(query)})`, 'gi');
        return text.replace(regex, '<strong>$1</strong>');
    }

    escapeRegex(str) {
        return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }

    navigateSuggestions(direction) {
        const items = this.searchSuggestions.querySelectorAll('.suggestion-item');
        if (items.length === 0) return;
        
        // Убираем текущую подсветку
        if (this.currentSuggestionIndex >= 0) {
            items[this.currentSuggestionIndex].style.background = '';
        }
        
        // Новый индекс
        this.currentSuggestionIndex += direction;
        
        // Циклическая навигация
        if (this.currentSuggestionIndex < 0) {
            this.currentSuggestionIndex = items.length - 1;
        } else if (this.currentSuggestionIndex >= items.length) {
            this.currentSuggestionIndex = 0;
        }
        
        // Подсвечиваем новый элемент
        items[this.currentSuggestionIndex].style.background = '#f5f5f5';
        items[this.currentSuggestionIndex].scrollIntoView({ block: 'nearest' });
        
        // Обновляем текст в поле ввода
        const text = items[this.currentSuggestionIndex].querySelector('span:nth-child(2)').textContent;
        this.globalSearchInput.value = text;
    }

    showSearchHistory() {
        const history = this.getSearchHistory();
        if (history.length === 0) return;
        
        const suggestions = history.map(text => ({
            text,
            type: 'history',
            score: 100
        }));
        
        this.showSuggestions(suggestions.slice(0, 5), '');
    }

    hideSuggestions() {
        this.searchSuggestions.style.display = 'none';
        this.currentSuggestionIndex = -1;
    }

    performSearch(query) {
        this.hideSuggestions();
        this.saveToSearchHistory(query);
        
        // Переход на страницу поиска с правильным параметром
        window.location.href = `/shop?search=${encodeURIComponent(query)}`;
    }

    getSearchHistory() {
        try {
            const saved = localStorage.getItem('globalSearchHistory');
            return saved ? JSON.parse(saved) : [];
        } catch (e) {
            return [];
        }
    }

    saveToSearchHistory(query) {
        let history = this.getSearchHistory();
        
        // Удаляем дубликаты
        history = history.filter(q => q !== query);
        
        // Добавляем в начало
        history.unshift(query);
        
        // Ограничиваем размер
        if (history.length > 10) {
            history = history.slice(0, 10);
        }
        
        try {
            localStorage.setItem('globalSearchHistory', JSON.stringify(history));
        } catch (e) {
            console.warn('Failed to save search history');
        }
    }
}

// ===== МЕНЕДЖЕР СОСТОЯНИЯ =====
class AppStateManager {
    constructor() {
        this.initialized = false;
        this.initialFetchDone = false;
        this.modules = {
            search: null,
            cart: null,
            products: null
        };
    }
    
    async initialize() {
        if (this.initialized) {
            console.warn('⚠️ AppStateManager already initialized');
            return;
        }
        
        console.log('🚀 Initializing App State Manager...');
        
        // Инициализация глобальных переменных
        window.productsData = [];
        window.currentPage = 1;
        window.itemsPerPage = 20;
        window.totalProducts = 0;
        window.sortColumn = 'name';
        window.sortDirection = 'asc';
        window.appliedFilters = {};
        
        // Инициализация модулей
        this.modules.search = new SearchManager();
        this.modules.search.init();
        
        cartBadge.init();
        
        // Настройка обработчиков
        this.setupEventHandlers();
        
        // Загрузка начальных данных
        await this.loadInitialData();
        
        this.initialized = true;
        console.log('✅ App State Manager initialized');
    }
    
    setupEventHandlers() {
        // Город
        const citySelect = document.getElementById('citySelect');
        if (citySelect) {
            citySelect.value = localStorage.getItem('selected_city_id') || '1';
            citySelect.addEventListener('change', () => {
                localStorage.setItem('selected_city_id', citySelect.value);
                showToast(`Город изменен на ${citySelect.options[citySelect.selectedIndex].text}`, false);
                
                // Перезагружаем товары при смене города
                if (this.initialFetchDone) {
                    productsManager.fetchProducts(true); // force = true
                }
            });
        }
        
        // Количество товаров на странице
        ['itemsPerPageSelect', 'itemsPerPageSelectBottom'].forEach(id => {
            const el = document.getElementById(id);
            if (el) {
                el.value = window.itemsPerPage;
                el.addEventListener('change', changeItemsPerPage);
            }
        });
        
        // Ввод номера страницы
        ['pageInput', 'pageInputBottom'].forEach(id => {
            const el = document.getElementById(id);
            if (el) {
                el.addEventListener('change', changePage);
                el.addEventListener('keydown', handlePageInputKeydown);
            }
        });
        
        // Обработчики кликов
        document.body.addEventListener('click', this.handleBodyClick.bind(this));
        
        // Кнопки пагинации
        document.querySelectorAll('.prev-btn').forEach(btn => {
            btn.addEventListener('click', evt => {
                evt.preventDefault();
                productsManager.loadPage(Math.max(1, window.currentPage - 1));
            });
        });
        
        document.querySelectorAll('.next-btn').forEach(btn => {
            btn.addEventListener('click', evt => {
                evt.preventDefault();
                const totalPages = Math.ceil(window.totalProducts / window.itemsPerPage);
                productsManager.loadPage(Math.min(totalPages, window.currentPage + 1));
            });
        });
    }
    
    async loadInitialData() {
        // Загрузка товаров только если есть таблица
        if (document.querySelector('.product-table') && !this.initialFetchDone) {
            this.initialFetchDone = true;
            await productsManager.fetchProducts();
        }
        
        // Загрузка корзины
        if (document.querySelector('.cart-container') || document.getElementById('cartBadge')) {
            try {
                await fetchCart();
            } catch (error) {
                console.error('Failed to load cart:', error);
            }
        }
    }
    
    handleBodyClick(e) {
        const target = e.target;
        
        // Добавить в корзину
        if (target.closest('.add-to-cart-btn')) {
            const btn = target.closest('.add-to-cart-btn');
            const productId = btn.dataset.productId;
            const quantityInput = btn.closest('tr')?.querySelector('.quantity-input');
            const quantity = parseInt(quantityInput?.value || '1', 10);
            addToCart(productId, quantity);
            return;
        }
        
        // Удалить из корзины
        if (target.closest('.remove-from-cart-btn')) {
            const btn = target.closest('.remove-from-cart-btn');
            removeFromCart(btn.dataset.productId);
            return;
        }
        
        // Очистить корзину
        if (target.matches('#clearCartBtn')) {
            if (confirm('Очистить корзину?')) {
                clearCart();
            }
            return;
        }
        
        // Создать спецификацию
        if (target.closest('.create-specification-btn, #createSpecLink')) {
            e.preventDefault();
            createSpecification();
            return;
        }
        
        // Сортировка
        const sortableHeader = target.closest('th.sortable');
        if (sortableHeader && sortableHeader.dataset.column) {
            productsManager.sortProducts(sortableHeader.dataset.column);
            return;
        }
        
        // Фильтры по бренду/серии
        if (target.closest('.brand-name, .series-name')) {
            const element = target.closest('.brand-name, .series-name');
            const filterType = element.classList.contains('brand-name') ? 'brand_name' : 'series_name';
            const value = element.textContent.trim();
            filterByBrandOrSeries(filterType, value);
            return;
        }
    }
}

// ===== ИНИЦИАЛИЗАЦИЯ =====
const appState = new AppStateManager();

document.addEventListener('DOMContentLoaded', function() {
    console.log('🚀 DOMContentLoaded - Starting app initialization...');
    
    // Запускаем инициализацию только один раз
    appState.initialize().then(() => {
        console.log('✅ App fully initialized');
    }).catch(error => {
        console.error('❌ App initialization failed:', error);
    });
});

// Удалено import.meta.hot - не поддерживается в production