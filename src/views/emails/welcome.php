<?php
// src/views/emails/welcome.php
// Приветственное письмо для новых пользователей
?>
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Добро пожаловать в VDestor B2B!</title>
    <style>
        body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
        .container { max-width: 600px; margin: 0 auto; padding: 20px; }
        .header { background: #016241; color: white; padding: 20px; text-align: center; }
        .content { padding: 20px; background: #f9f9f9; }
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
            <h1>Добро пожаловать в VDestor B2B!</h1>
        </div>
        
        <div class="content">
            <h2>Здравствуйте, <?= htmlspecialchars($username ?? 'Пользователь') ?>!</h2>
            
            <p>Спасибо за регистрацию в нашей системе. Теперь у вас есть доступ к:</p>
            
            <ul>
                <li>🔧 Более 10,000 наименований электротехнического оборудования</li>
                <li>📊 Специальным B2B ценам</li>
                <li>🚚 Быстрой доставке по всей России</li>
                <li>📋 Удобной системе спецификаций</li>
                <li>🧮 Инженерным калькуляторам</li>
            </ul>
            
            <div style="text-align: center; margin: 30px 0;">
                <a href="https://vdestor.ru/shop" class="button">Перейти в каталог</a>
            </div>
            
            <h3>Ваши данные для входа:</h3>
            <p><strong>Email:</strong> <?= htmlspecialchars($email ?? '') ?><br>
            <strong>Пароль:</strong> тот, который вы указали при регистрации</p>
            
            <p>Если у вас есть вопросы, наша служба поддержки всегда готова помочь!</p>
        </div>
        
        <div class="footer">
            <p>С уважением, команда VDestor B2B<br>
            Email: vde76ru@yandex.ru | Сайт: vdestor.ru</p>
        </div>
    </div>
</body>
</html>
