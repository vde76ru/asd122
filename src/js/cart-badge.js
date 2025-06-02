// src/js/cart-badge.js
// Единый модуль для управления бейджем корзины

class CartBadgeManager {
    constructor() {
        this.badgeElement = null;
        this.updateController = null;
        this.isUpdating = false;
    }

    init() {
        this.badgeElement = document.getElementById('cartBadge');
        if (this.badgeElement) {
            this.update();
        }
    }

    async update() {
        // Отменяем предыдущий запрос если он выполняется
        if (this.updateController) {
            this.updateController.abort();
        }
        
        if (this.isUpdating) {
            return;
        }

        this.updateController = new AbortController();
        this.isUpdating = true;

        try {
            const response = await fetch('/cart/json', {
                signal: this.updateController.signal
            });
            
            const data = await response.json();
            const cart = data.cart || {};
            const totalItems = Object.values(cart).reduce((sum, item) => sum + (item.quantity || 0), 0);

            if (this.badgeElement) {
                if (totalItems > 0) {
                    this.badgeElement.textContent = totalItems;
                    this.badgeElement.style.display = 'block';
                } else {
                    this.badgeElement.style.display = 'none';
                }
            }
        } catch (error) {
            if (error.name !== 'AbortError') {
                console.warn('Cart badge update error:', error);
            }
        } finally {
            this.isUpdating = false;
            this.updateController = null;
        }
    }
}

// Экспорт синглтона
export const cartBadge = new CartBadgeManager();

// Глобальный экспорт для обратной совместимости
window.updateCartBadge = () => cartBadge.update();