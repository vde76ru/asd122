<?php
use App\Services\AuthService;
?>
<div class="container mt-5">
  <h1>Корзина</h1>
  <?php if (empty($cartRows)): ?>
    <p>Корзина пуста</p>
  <?php else: ?>
    <table class="table table-striped">
      <thead>
        <tr>
          <th>Код товара</th>
          <th>Название</th>
          <th>Кол-во</th>
          <th>Цена</th>
          <th>Сумма</th>
          <th>Наличие</th>
          <th>Дата доставки</th>
          <th>Действие</th>
        </tr>
      </thead>
      <tbody>
        <?php 
        $total = 0;
        foreach ($cartRows as $row):
          $pid  = $row['product_id'];
          $qty  = $cart[$pid]['quantity'] ?? 0;
          $price = $row['base_price'];
          $sum = $price * $qty;
          $total += $sum;
          
          // Получаем external_id из products массива
          $external_id = $products[$pid]['external_id'] ?? '';
        ?>
        <tr data-product-id="<?= $pid ?>">
          <td><?= htmlspecialchars($external_id) ?></td>
          <td><?= htmlspecialchars($row['name'], ENT_QUOTES) ?></td>
          <td>
            <input type="number" class="form-control quantity-input"
                   value="<?= $qty ?>" min="1" data-product-id="<?= $pid ?>">
          </td>
          <td>
            <?= is_numeric($price) ? number_format($price, 2) . ' руб.' : '—' ?>
          </td>
          <td class="sum-cell">
            <?= is_numeric($price) ? number_format($sum, 2) . ' руб.' : '—' ?>
          </td>
          <td class="availability-cell" data-product-id="<?= $pid ?>">загрузка...</td>
          <td class="delivery-date-cell" data-product-id="<?= $pid ?>">загрузка...</td>
          <td>
            <button class="btn btn-sm btn-danger remove-from-cart-btn"
                    data-product-id="<?= $pid ?>">
              Удалить
            </button>
          </td>
        </tr>
        <?php endforeach; ?>
      </tbody>
      <tfoot>
        <tr>
          <td colspan="4"><strong>Итого:</strong></td>
          <td colspan="4"><strong><?= number_format($total, 2) ?> руб.</strong></td>
        </tr>
      </tfoot>
    </table>
    
    <div class="cart-actions mt-3">
        <form method="post" action="/specification/create" style="display: inline;">
            <input type="hidden" name="csrf_token" value="<?= \App\Core\CSRF::token() ?>">
            <button type="submit" class="btn btn-primary">Создать спецификацию</button>
        </form>
        
        <button id="clearCartBtn" class="btn btn-warning">Очистить корзину</button>
    </div>
  <?php endif; ?>
</div>

<script>
// Обработка изменения количества
document.addEventListener('change', function(e) {
    if (e.target.classList.contains('quantity-input')) {
        const productId = e.target.dataset.productId;
        const quantity = e.target.value;
        
        // Используем глобальную функцию updateCartItem
        if (window.updateCartItem) {
            window.updateCartItem(productId, quantity);
        }
    }
});

// Загрузка наличия товаров при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    const productIds = [];
    document.querySelectorAll('tr[data-product-id]').forEach(row => {
        productIds.push(row.dataset.productId);
    });
    
    if (productIds.length > 0 && window.loadAvailability) {
        // Используем глобальную функцию loadAvailability из availability.js
        window.loadAvailability(productIds);
    }
});
</script>