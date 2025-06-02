// src/js/cart.js
// Единый модуль для работы с корзиной

import { showToast } from "./utils.js";
import { cartBadge } from "./cart-badge.js";

export async function fetchCart() {
    try {
        const res = await fetch("/cart/json");
        if (!res.ok) throw new Error("Ошибка загрузки корзины");
        const data = await res.json();
        window.cart = data.cart || {};
        return window.cart;
    } catch (err) {
        showToast("Ошибка при загрузке корзины", true);
        throw err;
    }
}

export async function addToCart(productId, quantity) {
    try {
        const formData = new FormData();
        formData.append('product_id', productId);
        formData.append('quantity', quantity);
        formData.append('csrf_token', window.APP_CONFIG?.csrfToken || window.CSRF_TOKEN || '');
        
        const res = await fetch("/cart/add", {
            method: "POST",
            body: formData
        });
        
        if (!res.ok) throw new Error("Ошибка добавления в корзину");
        
        const data = await res.json();
        if (data.success) {
            showToast("Товар добавлен в корзину");
            cartBadge.update();
        } else {
            showToast(data.message || "Ошибка при добавлении в корзину", true);
        }
    } catch (err) {
        showToast("Ошибка при добавлении в корзину", true);
    }
}

export async function removeFromCart(productId) {
    try {
        const formData = new FormData();
        formData.append('productId', productId);
        formData.append('csrf_token', window.APP_CONFIG?.csrfToken || '');
        
        const res = await fetch("/cart/remove", {
            method: "POST",
            body: formData
        });
        
        if (!res.ok) throw new Error("Ошибка удаления из корзины");
        
        const data = await res.json();
        if (data.success) {
            showToast("Товар удален из корзины");
            cartBadge.update();
            
            // Перезагружаем страницу корзины если мы на ней
            if (window.location.pathname === '/cart') {
                window.location.reload();
            }
        } else {
            showToast(data.message || "Ошибка при удалении из корзины", true);
        }
    } catch (err) {
        showToast("Ошибка при удалении из корзины", true);
    }
}

export async function clearCart() {
    try {
        const formData = new FormData();
        formData.append('csrf_token', window.APP_CONFIG?.csrfToken || window.CSRF_TOKEN || '');
        
        const res = await fetch("/cart/clear", {
            method: "POST",
            body: formData
        });
        
        if (!res.ok) throw new Error("Ошибка очистки корзины");
        
        const data = await res.json();
        if (data.success) {
            showToast("Корзина очищена");
            cartBadge.update();
            
            // Перезагружаем страницу корзины если мы на ней
            if (window.location.pathname === '/cart') {
                window.location.reload();
            }
        } else {
            showToast(data.message || "Ошибка при очистке корзины", true);
        }
    } catch (err) {
        showToast("Ошибка при очистке корзины", true);
    }
}

export async function updateCartItem(productId, quantity) {
    try {
        const formData = new FormData();
        formData.append('productId', productId);
        formData.append('quantity', quantity);
        formData.append('csrf_token', window.APP_CONFIG?.csrfToken || window.CSRF_TOKEN || '');
        
        const res = await fetch("/cart/update", {
            method: "POST",
            body: formData
        });
        
        if (!res.ok) throw new Error("Ошибка обновления корзины");
        
        const data = await res.json();
        if (data.success) {
            showToast("Корзина обновлена");
            cartBadge.update();
        } else {
            showToast(data.message || "Ошибка при обновлении корзины", true);
        }
    } catch (err) {
        showToast("Ошибка при обновлении корзины", true);
    }
}

// Экспорт для обратной совместимости
window.updateCartItem = updateCartItem;