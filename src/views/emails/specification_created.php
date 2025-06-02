<?php
// src/views/emails/specification_created.php  
// Уведомление о создании спецификации
?>
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Спецификация создана</title>
    <style>
        body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
        .container { max-width: 600px; margin: 0 auto; padding: 20px; }
        .header { background: #016241; color: white; padding: 20px; text-align: center; }
        .content { padding: 20px; background: #f9f9f9; }
        .spec-info { 
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
            <h1>📋 Спецификация создана</h1>
        </div>
        
        <div class="content">
            <h2>Здравствуйте, <?= htmlspecialchars($username ?? 'Пользователь') ?>!</h2>
            
            <p>Ваша спецификация успешно создана и готова к использованию.</p>
            
            <div class="spec-info">
                <h3>Информация о спецификации:</h3>
                <p><strong>Номер:</strong> <?= htmlspecialchars($specification_id ?? 'N/A') ?></p>
                <p><strong>Дата создания:</strong> <?= date('d.m.Y H:i') ?></p>
                <p><strong>Количество позиций:</strong> <?= (int)($items_count ?? 0) ?> шт.</p>
                <p><strong>Общая сумма:</strong> <?= number_format($total_amount ?? 0, 2) ?> руб.</p>
            </div>
            
            <p>Что дальше:</p>
            <ul>
                <li>💾 Скачайте спецификацию в PDF</li>
                <li>📧 Отправьте менеджеру для просчета</li>
                <li>🛒 Оформите заказ</li>
            </ul>
            
            <div style="text-align: center; margin: 30px 0;">
                <a href="https://vdestor.ru/specification/<?= urlencode($specification_id ?? '') ?>" class="button">
                    Просмотреть спецификацию
                </a>
            </div>
        </div>
        
        <div class="footer">
            <p>С уважением, команда VDestor B2B<br>
            Email: vde76ru@yandex.ru | Сайт: vdestor.ru</p>
        </div>
    </div>
</body>
</html>
