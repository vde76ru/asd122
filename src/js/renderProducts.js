import { filterByBrandOrSeries, renderAppliedFilters, highlightFilteredWords } from "./filters.js";
import { loadAvailability } from "./availability.js";
import { showToast } from "./utils.js";

export function copyText(text) {
    if (!text) {
        showToast('Нечего копировать', true);
        return;
    }
    if (!navigator.clipboard) {
        showToast('Clipboard API не поддерживается', true);
        return;
    }
    navigator.clipboard.writeText(text)
        .then(() => showToast(`Скопировано: ${text}`))
        .catch(() => showToast('Не удалось скопировать', true));
}

export function bindSortableHeaders() {
    const table = document.querySelector('.product-table');
    if (!table) return;
    table.removeEventListener('click', sortableClickHandler);
    table.addEventListener('click', sortableClickHandler);
}

function sortableClickHandler(e) {
    const th = e.target.closest('th.sortable');
    if (th && window.sortProducts) window.sortProducts(th.dataset.column);
}

export function renderProductsTable() {
    console.log('🎨 renderProductsTable called, products:', window.productsData);
    
    const tbody = document.querySelector('.product-table tbody');
    if (!tbody) {
        console.error('❌ Table tbody not found!');
        return;
    }
    
    // ВАЖНО: Очищаем таблицу перед заполнением
    tbody.innerHTML = '';

    if (!window.productsData || window.productsData.length === 0) {
        console.warn('⚠️ No products to render');
        // Обратите внимание: исправлено количество колонок с 13 на 15!
        tbody.innerHTML = '<tr><td colspan="15" class="text-center p-4">Товары не найдены</td></tr>';
        return;
    }

    console.log(`📊 Rendering ${window.productsData.length} products`);
    
    // Используем DocumentFragment для оптимизации производительности
    const fragment = document.createDocumentFragment();
    
    window.productsData.forEach((product, index) => {
        try {
            const row = createProductRow(product);
            fragment.appendChild(row);
        } catch (error) {
            console.error(`❌ Error creating row for product ${index}:`, error, product);
        }
    });
    
    tbody.appendChild(fragment);
    
    // Обновляем интерфейс после рендера
    updateUI();
    loadMissingAvailability();
    initializeColResizable();
    
    console.log('✅ Products rendered successfully');
}

function createProductRow(product) {
    console.log('Creating row for product:', product); // Отладка
    
    const row = document.createElement('tr');
    row.setAttribute('data-product-id', product.product_id);
    row.className = 'product-row'; // Добавляем класс для стилизации

    // === 1. ЧЕКБОКС ===
    const selectCell = createCell('col-select text-center');
    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.className = 'product-checkbox';
    checkbox.setAttribute('data-product-id', product.product_id);
    selectCell.appendChild(checkbox);
    row.appendChild(selectCell);
    
    // === 2. КОД ===
    const codeCell = createCell('col-code');
    const codeValue = product.external_id || product.code || '';
    if (codeValue) {
        const codeContainer = createCopyableField(codeValue);
        codeCell.appendChild(codeContainer);
    } else {
        codeCell.textContent = '—';
    }
    row.appendChild(codeCell);
    
    // === 3. ФОТО ===
    const imageCell = createCell('col-image text-center');
    const imageContainer = createProductImage(product);
    imageCell.appendChild(imageContainer);
    row.appendChild(imageCell);
    
    // === 4. НАЗВАНИЕ ===
    const nameCell = createCell('col-name');
    const nameContainer = createProductName(product);
    nameCell.appendChild(nameContainer);
    row.appendChild(nameCell);
    
    // === 5. SKU ===
    const skuCell = createCell('col-sku');
    const skuValue = product.sku || '';
    if (skuValue) {
        const skuContainer = createCopyableField(skuValue);
        skuCell.appendChild(skuContainer);
    } else {
        skuCell.textContent = '—';
    }
    row.appendChild(skuCell);
    
    // === 6. БРЕНД/СЕРИЯ ===
    const brandCell = createCell('col-brand-series');
    const brandContainer = createBrandSeriesField(product);
    brandCell.appendChild(brandContainer);
    row.appendChild(brandCell);
    
    // === 7. СТАТУС ===
    const statusCell = createCell('col-status text-center');
    const statusBadge = createStatusBadge(product.status);
    statusCell.appendChild(statusBadge);
    row.appendChild(statusCell);
    
    // === 8. КРАТНОСТЬ/ЕД. ИЗМ ===
    const minSaleCell = createCell('col-min-sale-unit text-center');
    const minSaleContainer = createMinSaleField(product);
    minSaleCell.appendChild(minSaleContainer);
    row.appendChild(minSaleCell);
    
    // === 9. НАЛИЧИЕ ===
    const availCell = createCell('col-availability text-center');
    const availContainer = createAvailabilityField(product);
    availCell.appendChild(availContainer);
    row.appendChild(availCell);
    
    // === 10. ДАТА ДОСТАВКИ ===
    const deliveryCell = createCell('col-delivery-date text-center');
    const deliveryContainer = createDeliveryField(product);
    deliveryCell.appendChild(deliveryContainer);
    row.appendChild(deliveryCell);
    
    // === 11. ЦЕНА ===
    const priceCell = createCell('col-price text-end');
    const priceContainer = createPriceField(product, 'main');
    priceCell.appendChild(priceContainer);
    row.appendChild(priceCell);
    
    // === 12. РОЗНИЧНАЯ ЦЕНА ===
    const retailPriceCell = createCell('col-retail-price text-end');
    const retailPriceContainer = createPriceField(product, 'retail');
    retailPriceCell.appendChild(retailPriceContainer);
    row.appendChild(retailPriceCell);
    
    // === 13. КОРЗИНА ===
    const cartCell = createCell('col-cart');
    const cartContainer = createCartField(product);
    cartCell.appendChild(cartContainer);
    row.appendChild(cartCell);
    
    // === 14. ДОП. ДЕЙСТВИЯ ===
    const additionalCell = createCell('col-additional text-center');
    const additionalContainer = createAdditionalField(product);
    additionalCell.appendChild(additionalContainer);
    row.appendChild(additionalCell);
    
    // === 15. КУПЛЕНО ===
    const ordersCell = createCell('col-orders-count text-center');
    const ordersContainer = createOrdersField(product);
    ordersCell.appendChild(ordersContainer);
    row.appendChild(ordersCell);
    
    return row;
}

// Вспомогательные функции для создания элементов
function createCell(className) {
    const cell = document.createElement('td');
    cell.className = className;
    return cell;
}

function createCopyableField(text) {
    const container = document.createElement('div');
    container.className = 'd-flex align-items-center justify-content-between';
    
    const textSpan = document.createElement('span');
    textSpan.textContent = text;
    textSpan.className = 'text-truncate';
    textSpan.title = text; // Показываем полный текст при наведении
    
    const copyBtn = createCopyIcon(text);
    
    container.appendChild(textSpan);
    container.appendChild(copyBtn);
    
    return container;
}

function createProductImage(product) {
    const container = document.createElement('div');
    container.className = 'image-container position-relative';
    
    const images = getProductImages(product);
    const imageUrl = images[0] || '/images/placeholder.jpg';
    
    const img = document.createElement('img');
    img.src = imageUrl;
    img.alt = product.name || 'Товар';
    img.className = 'product-image img-thumbnail';
    img.style.cssText = `
        width: 50px; 
        height: 50px; 
        object-fit: contain; 
        cursor: pointer;
        transition: transform 0.2s;
    `;
    
    // Добавляем эффект при наведении
    img.addEventListener('mouseenter', () => {
        img.style.transform = 'scale(1.1)';
    });
    
    img.addEventListener('mouseleave', () => {
        img.style.transform = 'scale(1)';
    });
    
    const link = document.createElement('a');
    link.href = `/shop/product?id=${product.external_id || product.product_id}`;
    link.appendChild(img);
    
    container.appendChild(link);
    return container;
}

function createProductName(product) {
    const link = document.createElement('a');
    link.href = `/shop/product?id=${product.external_id || product.product_id}`;
    link.className = 'text-decoration-none product-name-link';
    link.style.color = 'inherit';
    
    const nameText = product.name || 'Название не указано';
    
    if (product._highlight && product._highlight.name) {
        link.innerHTML = product._highlight.name[0];
    } else {
        link.textContent = nameText;
    }
    
    link.title = nameText; // Полное название при наведении
    
    return link;
}

function createBrandSeriesField(product) {
    const container = document.createElement('div');
    
    if (product.brand_name) {
        const brandSpan = document.createElement('span');
        brandSpan.className = 'brand-name d-block text-primary cursor-pointer';
        brandSpan.style.fontSize = '0.9em';
        brandSpan.textContent = product.brand_name;
        brandSpan.title = `Фильтр по бренду: ${product.brand_name}`;
        brandSpan.addEventListener('click', () => {
            if (typeof filterByBrandOrSeries === 'function') {
                filterByBrandOrSeries('brand_name', product.brand_name);
            }
        });
        container.appendChild(brandSpan);
    }
    
    if (product.series_name) {
        const seriesSpan = document.createElement('span');
        seriesSpan.className = 'series-name d-block text-secondary cursor-pointer';
        seriesSpan.style.fontSize = '0.8em';
        seriesSpan.textContent = product.series_name;
        seriesSpan.title = `Фильтр по серии: ${product.series_name}`;
        seriesSpan.addEventListener('click', () => {
            if (typeof filterByBrandOrSeries === 'function') {
                filterByBrandOrSeries('series_name', product.series_name);
            }
        });
        container.appendChild(seriesSpan);
    }
    
    if (!product.brand_name && !product.series_name) {
        container.textContent = '—';
        container.className = 'text-muted';
    }
    
    return container;
}

function createStatusBadge(status) {
    const badge = document.createElement('span');
    badge.className = 'badge';
    
    switch(status?.toLowerCase()) {
        case 'active':
        case 'активен':
            badge.classList.add('bg-success');
            badge.textContent = 'АКТИВЕН';
            break;
        case 'inactive':
        case 'неактивен':
            badge.classList.add('bg-secondary');
            badge.textContent = 'НЕАКТИВЕН';
            break;
        case 'discontinued':
        case 'снят':
            badge.classList.add('bg-warning');
            badge.textContent = 'СНЯТ';
            break;
        default:
            badge.classList.add('bg-success');
            badge.textContent = 'АКТИВЕН';
    }
    
    return badge;
}

function createMinSaleField(product) {
    const container = document.createElement('div');
    container.className = 'text-center';
    
    const minSale = product.min_sale || 1;
    const unit = product.unit || 'шт';
    
    const quantitySpan = document.createElement('div');
    quantitySpan.className = 'fw-bold';
    quantitySpan.textContent = minSale;
    
    const unitSpan = document.createElement('small');
    unitSpan.className = 'text-muted d-block';
    unitSpan.textContent = unit;
    
    container.appendChild(quantitySpan);
    container.appendChild(unitSpan);
    
    return container;
}

function createAvailabilityField(product) {
    const badge = document.createElement('span');
    badge.className = 'badge availability-badge';
    
    if (product.stock && typeof product.stock.quantity !== 'undefined') {
        const qty = parseInt(product.stock.quantity);
        if (qty > 0) {
            badge.classList.add('bg-success');
            badge.textContent = `${qty} шт`;
        } else {
            badge.classList.add('bg-danger');
            badge.textContent = 'Нет';
        }
    } else {
        badge.classList.add('bg-secondary');
        badge.textContent = '...';
        // Помечаем для последующей загрузки
        badge.setAttribute('data-needs-loading', 'true');
    }
    
    return badge;
}

function createDeliveryField(product) {
    const span = document.createElement('span');
    span.className = 'delivery-date';
    
    if (product.delivery && product.delivery.date) {
        span.textContent = product.delivery.date;
        span.className += ' text-info';
    } else {
        span.textContent = '...';
        span.className += ' text-muted';
        // Помечаем для последующей загрузки
        span.setAttribute('data-needs-loading', 'true');
    }
    
    return span;
}

function createPriceField(product, type = 'main') {
    const container = document.createElement('div');
    
    if (type === 'main') {
        // Основная цена
        if (product.price && product.price.final) {
            const priceSpan = document.createElement('span');
            priceSpan.className = 'fw-bold';
            priceSpan.textContent = `${parseFloat(product.price.final).toFixed(2)} ₽`;
            
            if (product.price.has_special) {
                priceSpan.className += ' text-success';
            }
            
            container.appendChild(priceSpan);
        } else if (product.base_price) {
            const priceSpan = document.createElement('span');
            priceSpan.textContent = `${parseFloat(product.base_price).toFixed(2)} ₽`;
            container.appendChild(priceSpan);
        } else {
            container.textContent = '—';
            container.className = 'text-muted';
        }
    } else {
        // Розничная цена
        if (product.price && product.price.base && product.price.has_special) {
            const oldPriceSpan = document.createElement('span');
            oldPriceSpan.className = 'text-decoration-line-through text-muted';
            oldPriceSpan.textContent = `${parseFloat(product.price.base).toFixed(2)} ₽`;
            container.appendChild(oldPriceSpan);
        } else if (product.retail_price) {
            const retailSpan = document.createElement('span');
            retailSpan.textContent = `${parseFloat(product.retail_price).toFixed(2)} ₽`;
            container.appendChild(retailSpan);
        } else {
            container.textContent = '—';
            container.className = 'text-muted';
        }
    }
    
    return container;
}

function createCartField(product) {
    const container = document.createElement('div');
    container.className = 'd-flex align-items-center gap-2';
    
    const input = document.createElement('input');
    input.type = 'number';
    input.className = 'form-control form-control-sm quantity-input';
    input.style.width = '70px';
    input.value = product.min_sale || 1;
    input.min = product.min_sale || 1;
    input.step = product.min_sale || 1;
    input.title = `Минимальное количество: ${product.min_sale || 1}`;
    
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'btn btn-sm btn-primary add-to-cart-btn';
    button.setAttribute('data-product-id', product.product_id);
    button.innerHTML = '<i class="fas fa-cart-plus"></i>';
    button.title = 'Добавить в корзину';
    
    // Добавляем обработчик события
    button.addEventListener('click', function() {
        const quantity = parseInt(input.value) || 1;
        console.log(`Adding to cart: product ${product.product_id}, quantity: ${quantity}`);
        
        // Здесь должна быть функция добавления в корзину
        if (typeof addToCart === 'function') {
            addToCart(product.product_id, quantity);
        } else {
            showToast(`Товар "${product.name}" добавлен в корзину (${quantity} шт.)`);
        }
    });
    
    container.appendChild(input);
    container.appendChild(button);
    
    return container;
}

function createAdditionalField(product) {
    const dropdown = document.createElement('div');
    dropdown.className = 'dropdown';
    
    const button = document.createElement('button');
    button.className = 'btn btn-sm btn-outline-secondary';
    button.setAttribute('data-bs-toggle', 'dropdown');
    button.innerHTML = '<i class="fas fa-ellipsis-h"></i>';
    button.title = 'Дополнительные действия';
    
    const menu = document.createElement('ul');
    menu.className = 'dropdown-menu dropdown-menu-end';
    
    const actions = [
        {
            text: 'Подробнее',
            icon: 'fas fa-info-circle',
            href: `/shop/product?id=${product.external_id || product.product_id}`
        },
        {
            text: 'В избранное',
            icon: 'fas fa-heart',
            action: () => console.log('Add to favorites:', product.product_id)
        },
        {
            text: 'Сравнить',
            icon: 'fas fa-balance-scale',
            action: () => console.log('Add to compare:', product.product_id)
        }
    ];
    
    actions.forEach(actionItem => {
        const li = document.createElement('li');
        const a = document.createElement('a');
        a.className = 'dropdown-item';
        a.innerHTML = `<i class="${actionItem.icon} me-2"></i>${actionItem.text}`;
        
        if (actionItem.href) {
            a.href = actionItem.href;
        } else {
            a.href = '#';
            a.addEventListener('click', (e) => {
                e.preventDefault();
                if (actionItem.action) actionItem.action();
            });
        }
        
        li.appendChild(a);
        menu.appendChild(li);
    });
    
    dropdown.appendChild(button);
    dropdown.appendChild(menu);
    
    return dropdown;
}

function createOrdersField(product) {
    const badge = document.createElement('span');
    const count = parseInt(product.orders_count) || 0;
    
    if (count > 0) {
        badge.className = 'badge bg-info';
        badge.textContent = count.toString();
        badge.title = `Куплено раз: ${count}`;
    } else {
        badge.className = 'text-muted';
        badge.textContent = '0';
        badge.title = 'Еще не покупали';
    }
    
    return badge;
}



function createSelectCell(product) {
    const cell = document.createElement('td');
    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.classList.add('product-checkbox');
    cell.appendChild(checkbox);
    return cell;
}

function createCodeCell(product) {
    const cell = document.createElement('td');
    cell.classList.add('col-code');
    const codeItem = document.createElement('div');
    codeItem.className = 'item-code';
    const codeSpan = document.createElement('span');
    codeSpan.textContent = product.external_id || '';
    codeItem.appendChild(codeSpan);
    const copyIcon = createCopyIcon(product.external_id);
    codeItem.appendChild(copyIcon);
    cell.appendChild(codeItem);
    return cell;
}

function createImageCell(product) {
    const cell = document.createElement('td');
    const urls = getProductImages(product);
    const firstUrl = urls[0] || '/images/placeholder.jpg';
    const container = document.createElement('div');
    container.className = 'image-container';
    container.style.position = 'relative';
    const thumb = document.createElement('img');
    thumb.src = firstUrl;
    thumb.alt = product.name || '';
    thumb.style.width = '50px';
    thumb.style.cursor = 'pointer';
    thumb.style.transition = 'opacity 0.3s ease';
    const zoom = document.createElement('img');
    zoom.className = 'zoom-image';
    zoom.src = firstUrl;
    zoom.alt = product.name || '';
    zoom.style.width = '350px';
    zoom.style.position = 'absolute';
    zoom.style.top = '0';
    zoom.style.left = '60px';
    zoom.style.opacity = '0';
    zoom.style.transition = 'opacity 0.3s ease';
    zoom.style.pointerEvents = 'none';
    zoom.style.zIndex = '1000';
    zoom.style.boxShadow = '0 4px 8px rgba(0,0,0,0.2)';
    zoom.style.backgroundColor = 'white';
    zoom.style.padding = '5px';
    zoom.style.border = '1px solid #ddd';
    zoom.style.borderRadius = '4px';

    thumb.addEventListener('mouseenter', () => {
        zoom.style.opacity = '1';
        zoom.style.pointerEvents = 'auto';
    });
    thumb.addEventListener('mouseleave', () => {
        zoom.style.opacity = '0';
        zoom.style.pointerEvents = 'none';
    });
    container.appendChild(thumb);
    container.appendChild(zoom);
    const link = document.createElement('a');
    link.href = `/shop/product?id=${product.external_id}`;
    link.appendChild(container);
    cell.appendChild(link);
    return cell;
}

function createNameCell(product) {
    const cell = document.createElement('td');
    cell.className = 'name-cell';
    const link = document.createElement('a');
    link.href = `/shop/product?id=${product.external_id}`;
    link.style.color = 'inherit';
    link.style.textDecoration = 'none';
    const nameItem = document.createElement('div');
    nameItem.className = 'item-code';
    const nameSpan = document.createElement('span');

    if (product._highlight && product._highlight.name) {
        nameSpan.innerHTML = product._highlight.name[0];
    } else {
        nameSpan.textContent = product.name || '';
    }

    nameItem.appendChild(nameSpan);
    const copyIcon = createCopyIcon(product.name);
    nameItem.appendChild(copyIcon);
    link.appendChild(nameItem);
    cell.appendChild(link);
    return cell;
}

function createSkuCell(product) {
    const cell = document.createElement('td');
    const skuItem = document.createElement('div');
    skuItem.className = 'item-code';
    const skuSpan = document.createElement('span');
    skuSpan.textContent = product.sku || '';
    skuItem.appendChild(skuSpan);
    const copyIcon = createCopyIcon(product.sku);
    skuItem.appendChild(copyIcon);
    cell.appendChild(skuItem);
    return cell;
}

function createBrandSeriesCell(product) {
    const cell = document.createElement('td');
    const div = document.createElement('div');
    const brandSpan = document.createElement('span');
    brandSpan.className = 'brand-name';
    brandSpan.textContent = product.brand_name || '';
    brandSpan.style.cursor = 'pointer';
    brandSpan.addEventListener('click', () => filterByBrandOrSeries('brand_name', product.brand_name));
    const seriesSpan = document.createElement('span');
    seriesSpan.className = 'series-name';
    seriesSpan.textContent = product.series_name || '';
    seriesSpan.style.cursor = 'pointer';
    seriesSpan.addEventListener('click', () => filterByBrandOrSeries('series_name', product.series_name));
    if (brandSpan.textContent && seriesSpan.textContent) {
        brandSpan.textContent += ' / ';
    }
    div.appendChild(brandSpan);
    div.appendChild(seriesSpan);
    cell.appendChild(div);
    return cell;
}

function createStatusCell(product) {
    const cell = document.createElement('td');
    const span = document.createElement('span');
    span.textContent = product.status || 'Активен';
    cell.appendChild(span);
    return cell;
}

function createMinSaleUnitCell(product) {
    const cell = document.createElement('td');
    const minSaleSpan = document.createElement('span');
    minSaleSpan.textContent = product.min_sale || '';
    const unitSpan = document.createElement('span');
    unitSpan.textContent = product.unit ? ` / ${product.unit}` : '';
    cell.appendChild(minSaleSpan);
    cell.appendChild(unitSpan);
    return cell;
}

function createAvailabilityCell(product) {
    const cell = document.createElement('td');
    cell.classList.add('col-availability');
    const span = document.createElement('span');
    if (product.stock) {
        const qty = product.stock.quantity || 0;
        span.textContent = qty > 0 ? `${qty} шт.` : "Нет";
        span.classList.toggle('in-stock', qty > 0);
        span.classList.toggle('out-of-stock', qty === 0);
    } else {
        span.textContent = '…';
    }
    cell.appendChild(span);
    return cell;
}

function createDeliveryDateCell(product) {
    const cell = document.createElement('td');
    cell.classList.add('col-delivery-date');
    const span = document.createElement('span');
    if (product.delivery) {
        span.textContent = product.delivery.date || product.delivery.text || '—';
    } else {
        span.textContent = '…';
    }
    cell.appendChild(span);
    return cell;
}

function createPriceCell(product) {
    const cell = document.createElement('td');
    const span = document.createElement('span');
    if (product.price && product.price.final) {
        span.textContent = `${product.price.final.toFixed(2)} руб.`;
        if (product.price.has_special) {
            span.innerHTML = `<span class="price-current">${product.price.final.toFixed(2)} руб.</span>`;
        }
    } else if (product.base_price) {
        span.textContent = `${product.base_price.toFixed(2)} руб.`;
    } else {
        span.textContent = 'Нет цены';
    }
    cell.appendChild(span);
    cell.setAttribute('data-fulltext', span.textContent);
    return cell;
}

function createRetailPriceCell(product) {
    const cell = document.createElement('td');
    const span = document.createElement('span');
    if (product.price && product.price.base && product.price.has_special) {
        span.innerHTML = `<span class="price-old">${product.price.base.toFixed(2)} руб.</span>`;
    } else if (product.retail_price) {
        span.textContent = `${product.retail_price.toFixed(2)} руб.`;
    } else {
        span.textContent = '—';
    }
    cell.appendChild(span);
    cell.setAttribute('data-fulltext', span.textContent);
    return cell;
}

function createCartCell(product) {
    const cell = document.createElement('td');
    const input = document.createElement('input');
    input.className = 'form-control quantity-input';
    input.type = 'number';
    input.value = 1;
    input.min = 1;
    const button = document.createElement('button');
    button.className = 'add-to-cart-btn';
    button.innerHTML = '<i class="fas fa-shopping-cart"></i>';
    button.dataset.productId = product.product_id;
    cell.appendChild(input);
    cell.appendChild(button);
    return cell;
}

function createAdditionalFieldsCell() {
    const cell = document.createElement('td');
    const span = document.createElement('span');
    span.textContent = 'Доп. информация';
    cell.appendChild(span);
    return cell;
}

function createOrdersCountCell(product) {
    const cell = document.createElement('td');
    const span = document.createElement('span');
    span.textContent = product.orders_count || '0';
    cell.appendChild(span);
    return cell;
}

function createCopyIcon(text) {
    const icon = document.createElement('button');
    icon.className = 'btn btn-link btn-sm p-0 ms-2 copy-icon';
    icon.setAttribute('data-text-to-copy', text || '');
    icon.innerHTML = '<i class="far fa-copy"></i>';
    icon.title = 'Копировать';
    icon.style.fontSize = '12px';
    icon.style.opacity = '0.6';
    icon.addEventListener('click', e => {
        e.preventDefault();
        e.stopPropagation();
        copyText(text);
    });
    return icon;
}

function getProductImages(product) {
    let urls = [];
    if (product.images && Array.isArray(product.images)) {
        urls = product.images;
    } else if (typeof product.image_urls === 'string' && product.image_urls.trim()) {
        urls = product.image_urls.split(',').map(u => u.trim());
    }
    return urls;
}

function updateUI() {
    if (typeof window.updatePaginationDisplay === "function") window.updatePaginationDisplay();
    if (typeof window.renderAppliedFilters === "function") window.renderAppliedFilters();
    if (typeof window.highlightFilteredWords === "function") window.highlightFilteredWords();
}

function loadMissingAvailability() {
    const productsNeedingAvailability = window.productsData.filter(p => !p.stock && !p.delivery);
    if (productsNeedingAvailability.length > 0) {
        const ids = productsNeedingAvailability.map(p => p.product_id);
        loadAvailability(ids);
    }
}

function initializeColResizable() {
    try {
        if (typeof jQuery !== 'undefined' && jQuery.fn.colResizable) {
            const $table = jQuery('#productTable');
            if ($table.length > 0) {
                $table.colResizable('destroy');
                $table.colResizable({
                    liveDrag: true,
                    minWidth: 30,
                    hoverCursor: "col-resize"
                });
            }
        }
    } catch (e) {
        console.warn('colResizable не инициализирован:', e.message);
    }
}