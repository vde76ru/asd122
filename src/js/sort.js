import { productsManager } from "./ProductsManager.js";

export function sortProducts(column) {
    if (window.sortColumn === column) {
        window.sortDirection = window.sortDirection === "asc" ? "desc" : "asc";
    } else {
        window.sortColumn = column;
        window.sortDirection = "asc";
    }
    sessionStorage.setItem('sortColumn', window.sortColumn);
    sessionStorage.setItem('sortDirection', window.sortDirection);
    fetchProducts();
}