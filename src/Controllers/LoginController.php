<?php
namespace App\Controllers;

use App\Core\Layout;
use App\Services\AuthService;
use App\Core\CSRF;

class LoginController
{
    public function loginAction(): void
    {
        $error = '';

        if ($_SERVER['REQUEST_METHOD'] === 'POST') {
            // Проверка CSRF-токена
            if (!CSRF::validate($_POST['csrf_token'] ?? null)) {
                $error = 'Неверный CSRF-токен';
            } else {
                // Получение данных из POST-запроса
                $usernameOrEmail = trim($_POST['username'] ?? '');
                $password = $_POST['password'] ?? '';

                // Аутентификация пользователя
                $result = AuthService::authenticate($usernameOrEmail, $password);

                if ($result['success']) {
                    // Успешная авторизация
                    header('Location: /admin');
                    exit;
                } else {
                    // Ошибка авторизации
                    $error = $result['error'] ?? 'Неверные учетные данные';
                }
            }
        }

        // Рендеринг страницы входа с возможным сообщением об ошибке
        Layout::render('auth/login', ['error' => $error]);
    }
}