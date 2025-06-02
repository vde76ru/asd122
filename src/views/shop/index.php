<div class="main-content">
        <div class="product-container" id="productContainer">
            <?php include __DIR__ . '/search_form.html'; ?>
            <div id="filters" class="applied-filters"></div>
            <table class="product-table" id="productTable">
                <thead>
                    <tr class="controls-row">
                        <th colspan="15">
                            <div class="controls">
                                <div class="controls-left">
                                    <button class="prev-btn"><i class="fas fa-angle-left"></i></button>
                                    <input type="number" id="pageInput" min="1" placeholder="Введите номер страницы">
                                    <button class="next-btn"><i class="fas fa-angle-right"></i></button>
                                    <span>Страница: <span id="currentPage">1</span> из <span id="totalPages">1</span></span>
                                </div>
                                <div class="controls-center">
                                    <span id="totalProductsText">Найдено товаров: 0</span>
                                </div>
                                <div class="controls-right">
                                    <label for="itemsPerPageSelect">Товаров на странице:</label>
                                    <select id="itemsPerPageSelect">
                                        <option value="20">20</option>
                                        <option value="40">40</option>
                                        <option value="80">80</option>
                                        <option value="100">100</option>
                                    </select>
                                </div>
                            </div>
                        </th>
                    </tr>
                    <tr>
                        <th class="col-select"><input type="checkbox" id="selectAll"></th>
                        <th class="col-code sortable" data-column="external_id">Код</th>
                        <th class="col-image">Фото</th>
                        <th class="col-name sortable" data-column="name">Название</th>
                        <th class="col-sku sortable" data-column="sku">SKU</th>
                        <th class="col-brand-series" data-column="brand_series">Бренд/Серия</th>
                        <th class="col-status" data-column="status">Статус</th>
                        <th class="col-min-sale-unit" data-column="min_sale_unit">Кратность/ед. изм</th>
                        <th class="col-availability sortable" data-column="availability">Наличие</th>
                        <th class="col-delivery-date sortable" data-column="delivery_date">Дата доставки</th>
                        <th class="col-price sortable" data-column="price">Цена</th>
                        <th class="col-retail-price sortable" data-column="retail_price">Розничная</th>
                        <th class="col-cart">Корзина</th>
                        <th class="col-additional">Доп.</th>
                        <th class="col-orders-count sortable" data-column="orders_count">Куплено</th>
                    </tr>
                </thead>
                <tbody>
                    <!-- Товары будут добавляться сюда -->
                </tbody>
                <tfoot>
                    <tr class="controls-row">
                        <th colspan="15">
                            <div class="controls">
                                <div class="controls-left">
                                    <button class="prev-btn" onclick="loadPage(currentPage - 1)"><i class="fas fa-angle-left"></i></button>
                                    <input type="number" id="pageInputBottom" min="1" placeholder="Введите номер страницы">
                                    <button class="next-btn" onclick="loadPage(currentPage + 1)"><i class="fas fa-angle-right"></i></button>
                                    <span>Страница: <span id="currentPageBottom">1</span> из <span id="totalPagesBottom">1</span></span>
                                </div>
                                <div class="controls-center">
                                    <span id="totalProductsTextBottom">Найдено товаров: 0</span>
                                </div>
                                <div class="controls-right">
                                    <label for="itemsPerPageSelectBottom">Товаров на странице:</label>
                                    <select id="itemsPerPageSelectBottom">
                                        <option value="20">20</option>
                                        <option value="40">40</option>
                                        <option value="80">80</option>
                                        <option value="100">100</option>
                                    </select>
                                </div>
                            </div>
                        </th>
                    </tr>
                </tfoot>
            </table>
            <div class="cart-container"></div>
        </div>
    </div>