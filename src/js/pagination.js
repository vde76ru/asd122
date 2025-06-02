import { renderProductsTable } from "./renderProducts.js";
import { productsManager } from "./ProductsManager.js";

export function updatePaginationDisplay() {
    const total = Math.ceil(window.totalProducts / window.itemsPerPage);
    document.querySelectorAll('.pagination-info').forEach(el => {
        el.textContent = `Страница ${window.currentPage} из ${total}`;
    });
    document.querySelectorAll('.pageInput').forEach(el => {
        el.value = window.currentPage;
    });
}

export function changeItemsPerPage(event) {
    window.itemsPerPage = parseInt(event.target.value, 10) || 20;
    sessionStorage.setItem('itemsPerPage', window.itemsPerPage);
    window.currentPage = 1;
    fetchProducts();
}

export function changePage(event) {
    let page = parseInt(event.target.value, 10) || 1;
    const total = Math.ceil(window.totalProducts / window.itemsPerPage);
    page = Math.max(1, Math.min(page, total));
    window.currentPage = page;
    fetchProducts();
}

export function handlePageInputKeydown(event) {
    if (event.key === "Enter") {
        changePage(event);
    }
}

export function loadPage(page) {
    window.currentPage = page;
    fetchProducts();
}