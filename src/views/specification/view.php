<?php
/**
 * @var array|null $specification
 * @var array      $items
 * @var bool       $guest
 */
?>
<div class="container mt-5">
    <?php if (!$specification): ?>
        <h2>Спецификация не найдена</h2>
        <a href="/cart" class="btn btn-primary mt-3">Вернуться в корзину</a>
    <?php else: ?>
        <h2>
            <?= $guest ? 'Гостевая спецификация' : 'Спецификация №' . htmlspecialchars($specification['specification_id']) ?>
        </h2>
        <div>
            <b>Дата создания:</b> <?= htmlspecialchars($specification['created_at']) ?>
        </div>
        <table class="table table-bordered mt-3">
            <thead>
                <tr>
                    <th>#</th>
                    <th>Товар</th>
                    <th>Количество</th>
                    <th>Цена</th>
                    <th>Сумма</th>
                </tr>
            </thead>
            <tbody>
            <?php
            $grandTotal = 0;
            if ($items): foreach ($items as $i => $item):
                $qty = (int)($item['quantity'] ?? 0);
                $price = (isset($item['price']) && is_numeric($item['price'])) ? (float)$item['price'] : 0;
                $sum = $price * $qty;
                $grandTotal += $sum;
            ?>
                <tr>
                    <td><?= $i + 1 ?></td>
                    <td><?= htmlspecialchars($item['name'] ?? $item['product_id'] ?? '—') ?></td>
                    <td><?= $qty ?></td>
                    <td><?= number_format($price, 2) ?></td>
                    <td><?= number_format($sum, 2) ?></td>
                </tr>
            <?php endforeach; ?>
                <tr>
                    <th colspan="4" class="text-end">Итого</th>
                    <th><?= number_format($grandTotal, 2) ?></th>
                </tr>
            <?php else: ?>
                <tr><td colspan="5">Нет товаров в спецификации</td></tr>
            <?php endif; ?>
            </tbody>
        </table>
        <a href="/cart" class="btn btn-secondary">Вернуться в корзину</a>
    <?php endif; ?>
</div>