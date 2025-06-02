import { showToast } from "./utils.js";
import { fetchCart } from "./cart.js";

export async function createSpecification() {
    const formData = new FormData();
    formData.append('csrf_token', window.APP_CONFIG?.csrfToken || '');

    try {
        const res = await fetch('/specification/create', {
            method: 'POST',
            body: formData
        });
        const text = await res.text();
        let data;
        try {
            data = JSON.parse(text);
        } catch (e) {
            showToast('Ошибка сервера: неверный ответ. Попробуйте позже.', true);
            return;
        }
        if (data.success) {
            showToast('Спецификация создана');
            window.cart = {};
            fetchCart();
            setTimeout(() => {
                window.location.href = `/specification/${data.specification_id}`;
            }, 1000);
        } else {
            showToast(data.message || 'Ошибка создания спецификации', true);
        }
    } catch (err) {
        showToast('Ошибка соединения с сервером', true);
    }
}