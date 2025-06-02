<?php
// src/views/emails/order_confirmation.php
// Подтверждение заказа
?>
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Заказ принят в обработку</title>
    <style>
        body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
        .container { max-width: 600px; margin: 0 auto; padding: 20px; }
        .header { background: #016241; color: white; padding: 20px; text-align: center; }
        .content { padding: 20px; background: #f9f9f9; }
        .order-info { 
            background: white; 
            padding: 15px; 
            border-radius: 4px; 
            margin: 15px 0; 
            border-left: 4px solid #016241; 
        }
        .button { 
            display: inline-block; 
            background: #016241; 
            color: white; 
            padding: 12px 24px; 
            text-decoration: none; 
            border-radius: 4px; 
            margin: 10px 0; 
        }
        .footer { text-align: center; padding: 20px; color: #666; font-size: 12px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>✅ Заказ принят в обработку</h1>
        </div>
        
        <div class="content">
            <h2>Здравствуйте, <?= htmlspecialchars($username ?? 'Пользователь') ?>!</h2>
            
            <p>Спасибо за ваш заказ! Мы получили вашу заявку и уже начали её обработку.</p>
            
            <div class="order-info">
                <h3>Детали заказа:</h3>
                <p><strong>Номер заказа:</strong> <?= htmlspecialchars($order_id ?? 'N/A') ?></p>
                <p><strong>Дата:</strong> <?= date('d.m.Y H:i') ?></p>
                <p><strong>Сумма:</strong> <?= number_format($total_amount ?? 0, 2) ?> руб.</p>
                <p><strong>Статус:</strong> В обработке</p>
            </div>
            
            <h3>Что происходит дальше:</h3>
            <ol>
                <li>🔍 Проверяем наличие товаров на складе</li>
                <li>💰 Формируем окончательную стоимость</li>
                <li>📞 Связываемся с вами для подтверждения</li>
                <li>📦 Отгружаем товар</li>
                <li>🚚 Доставляем по указанному адресу</li>
            </ol>
            
            <p><strong>Ожидаемое время обработки:</strong> 1-2 рабочих дня</p>
        </div>
        
        <div class="footer">
            <p>С уважением, команда VDestor B2B<br>
            Email: vde76ru@yandex.ru | Телефон: +7 (XXX) XXX-XX-XX</p>
        </div>
    </div>
</body>
</html>