<?php
/** @var array $product */
/** @var array $images */
/** @var array $documents */
/** @var array $attributes */
/** @var string|float $price */
/** @var string|int $stock */
/** @var array $related */
?>
<link rel="stylesheet" href="/assets/css/product-card.css">

<div class="breadcrumbs">
    <a href="/">Главная</a> <span>/</span>
    <a href="/shop">Каталог</a> <span>/</span>
    <span><?= htmlspecialchars($product['name']) ?></span>
</div>

<div class="product-card" data-product-id="<?= (int)$product['product_id'] ?>">
    <!-- ГАЛЕРЕЯ -->
    <div class="product-card__gallery">
        <div class="gallery__main-image">
            <img id="mainProductImage" src="<?= htmlspecialchars($images[0]['url'] ?? '/images/placeholder.jpg') ?>"
                alt="<?= htmlspecialchars($images[0]['alt_text'] ?? $product['name']) ?>">
        </div>
        <?php if (count($images) > 1): ?>
            <div class="gallery__thumbnails">
                <?php foreach ($images as $img): ?>
                    <img class="gallery__thumbnail" src="<?= htmlspecialchars($img['url']) ?>"
                        alt="<?= htmlspecialchars($img['alt_text'] ?? $product['name']) ?>"
                        onclick="document.getElementById('mainProductImage').src=this.src">
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
        <?php if ($documents): ?>
            <div class="gallery__docs">
                <?php foreach ($documents as $doc): ?>
                    <?php if (isset($doc['type']) && $doc['type'] === 'certificate'): ?>
                        <a href="<?= htmlspecialchars($doc['url']) ?>" target="_blank" title="Сертификат" class="gallery__doc-icon">
                            <img src="/assets/img/certificate-icon.svg" alt="Сертификат">
                        </a>
                    <?php endif; ?>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
    </div>

    <!-- ОСНОВНОЙ БЛОК СВЕДЕНИЙ -->
    <div class="product-card__main">
        <h1 class="product-card__title"><?= htmlspecialchars($product['name']) ?></h1>
        <div class="product-card__meta">
            <div class="meta__line">
                <span class="meta__label">Артикул:</span>
                <span class="meta__value"><?= htmlspecialchars($product['external_id']) ?></span>
            </div>
            <?php if (!empty($product['sku'])): ?>
                <div class="meta__line">
                    <span class="meta__label">SKU:</span>
                    <span class="meta__value"><?= htmlspecialchars($product['sku']) ?></span>
                </div>
            <?php endif; ?>
            <?php if (!empty($product['brand_name'])): ?>
                <div class="meta__line">
                    <span class="meta__label">Бренд:</span>
                    <span class="meta__value"><?= htmlspecialchars($product['brand_name']) ?></span>
                </div>
            <?php endif; ?>
            <?php if (!empty($product['series_name'])): ?>
                <div class="meta__line">
                    <span class="meta__label">Серия:</span>
                    <span class="meta__value"><?= htmlspecialchars($product['series_name']) ?></span>
                </div>
            <?php endif; ?>
        </div>
        <div class="product-card__shortdesc"><?= nl2br(htmlspecialchars($product['short_description'] ?? $product['description'] ?? '')) ?></div>
    </div>

    <!-- БЛОК ПОКУПКИ -->
    <div class="product-card__buy">
        <div class="product-card__price-row">
            <span class="product-card__price-label">Цена:</span>
            <span class="product-card__price-value"><?= $price ? number_format($price, 2, ',', ' ') . ' руб.' : '—' ?></span>
        </div>
        <div class="product-card__stock-row">
            <span class="product-card__stock-label">В наличии:</span>
            <span class="product-card__stock-value"><?= $stock !== null ? (int)$stock : '—' ?></span>
        </div>
        <form class="product-card__cart-form" onsubmit="addToCartHandler(event, <?= (int)$product['product_id'] ?>)">
            <label>
                <span>Количество:</span>
                <input type="number" name="quantity" min="1" value="1" class="quantity-input">
            </label>
            <button class="btn btn-primary" type="submit">В корзину</button>
        </form>
        <button class="btn btn-secondary product-card__quickbuy-btn">Быстрый заказ</button>
        <div class="product-card__min-sale">
            <?php if (!empty($product['min_sale'])): ?>
                <span>Мин. партия: <?= (int)$product['min_sale'] ?> <?= htmlspecialchars($product['unit'] ?? 'шт.') ?></span>
            <?php endif; ?>
        </div>
    </div>

    <!-- ХАРАКТЕРИСТИКИ / ОСНОВНЫЕ ПАРАМЕТРЫ -->
    <div class="product-card__features">
        <h2>Краткие характеристики</h2>
        <table>
            <?php if (!empty($product['weight'])): ?>
                <tr><td>Вес</td><td><?= htmlspecialchars($product['weight']) ?> кг</td></tr>
            <?php endif; ?>
            <?php if (!empty($product['dimensions'])): ?>
                <tr><td>Размеры</td><td><?= htmlspecialchars($product['dimensions']) ?></td></tr>
            <?php endif; ?>
            <?php if ($attributes): ?>
                <?php foreach ($attributes as $attr): ?>
                    <tr>
                        <td><?= htmlspecialchars($attr['name']) ?></td>
                        <td><?= htmlspecialchars($attr['value']) ?><?= !empty($attr['unit']) ? ' ' . htmlspecialchars($attr['unit']) : '' ?></td>
                    </tr>
                <?php endforeach; ?>
            <?php endif; ?>
        </table>
    </div>

    <!-- ОПИСАНИЕ -->
    <div class="product-card__description">
        <h2>Описание</h2>
        <div><?= nl2br(htmlspecialchars($product['description'] ?? '')) ?></div>
        <?php if ($documents): ?>
            <?php foreach ($documents as $doc): ?>
                <?php if (isset($doc['type']) && $doc['type'] === 'drawing'): ?>
                    <div class="product-card__brochure">
                        <a href="<?= htmlspecialchars($doc['url']) ?>" target="_blank">
                            📄 <?= htmlspecialchars($doc['name'] ?? 'Брошюра/каталог') ?>
                        </a>
                    </div>
                <?php endif; ?>
            <?php endforeach; ?>
        <?php endif; ?>
    </div>

    <!-- ДОКУМЕНТЫ И СЕРТИФИКАТЫ -->
    <div class="product-card__documents">
        <h2>Документы и сертификаты</h2>
        <?php if ($documents): ?>
            <ul>
                <?php foreach ($documents as $doc): ?>
                    <li>
                        <a href="<?= htmlspecialchars($doc['url']) ?>" target="_blank">
                            <?= htmlspecialchars($doc['name'] ?? 'Документ') ?>
                        </a>
                    </li>
                <?php endforeach; ?>
            </ul>
        <?php else: ?>
            <div class="empty-block">Документов пока нет.</div>
        <?php endif; ?>
    </div>

    <!-- ДОСТАВКА И ОПЛАТА (пустой блок) -->
    <div class="product-card__delivery">
        <h2>Доставка и оплата</h2>
        <div class="empty-block">Информация появится скоро.</div>
    </div>

    <!-- ОТЗЫВЫ (пустой блок) -->
    <div class="product-card__reviews">
        <h2>Отзывы покупателей</h2>
        <div class="empty-block">Здесь будут отзывы ваших покупателей.</div>
    </div>

    <!-- ВОПРОС-ОТВЕТ (пустой блок) -->
    <div class="product-card__faq">
        <h2>Вопросы и ответы</h2>
        <div class="empty-block">Пока нет вопросов по этому товару.</div>
    </div>

    <!-- РЕКОМЕНДАЦИИ -->
    <div class="product-card__related">
        <h2>С этим товаром покупают</h2>
        <?php if ($related): ?>
            <div class="related-products-list">
                <?php foreach ($related as $rel): ?>
                    <div class="related-product-card">
                        <a href="/shop/product?id=<?= urlencode($rel['external_id']) ?>">
                            <?= htmlspecialchars($rel['name']) ?>
                        </a>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php else: ?>
            <div class="empty-block">Рекомендаций пока нет.</div>
        <?php endif; ?>
    </div>

    <!-- ДЛЯ РАЗВИТИЯ: ВИДЕО/3D/КАЛЬКУЛЯТОР/ДРУГИЕ ИНФО-БЛОКИ (ПУСТО) -->
    <div class="product-card__future">
        <h2>Дополнительные возможности</h2>
        <div class="empty-block">Здесь появятся: видеообзоры, 3D-модель, калькулятор стоимости и другие полезные функции.</div>
    </div>
</div>

<script>
function addToCartHandler(e, productId) {
    e.preventDefault();
    const qty = e.target.quantity.value || 1;
    fetch('/cart/add', {
        method: 'POST',
        body: new URLSearchParams({
            productId: productId,
            quantity: qty,
            csrf_token: window.CSRF_TOKEN || ''
        })
    })
    .then(res => res.json())
    .then(data => {
        if (data.success) alert('Товар добавлен в корзину!');
        else alert('Ошибка: ' + (data.message || 'Неизвестная ошибка'));
    })
    .catch(() => alert('Ошибка добавления в корзину'));
}
</script>