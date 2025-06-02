<?php
// src/Core/Layout.php
namespace App\Core;

use App\Services\AuthService;

/**
 * Класс для рендеринга шаблонов с общей структурой страницы
 * 
 * Этот класс помогает нам не дублировать код header и footer
 * на каждой странице. Вместо этого мы просто вызываем Layout::render()
 * и передаем туда название шаблона и данные.
 */
class Layout
{
    /**
     * Рендерит вьюху внутри общего шаблона с шапкой и подвалом
     * 
     * @param string $viewPath Путь к шаблону относительно views/, например 'cart/view'
     * @param array  $params   Данные, которые будут доступны в шаблоне
     * @param array  $options  Дополнительные опции рендеринга
     */
    public static function render(string $viewPath, array $params = [], array $options = []): void
    {
        // Добавляем глобальные переменные, доступные во всех шаблонах
        $params = array_merge($params, self::getGlobalParams());
        
        // Извлекаем переменные, чтобы они были доступны в шаблоне
        // EXTR_SKIP означает, что если переменная уже существует, она не будет перезаписана
        extract($params, EXTR_SKIP);
        
        // Начинаем буферизацию вывода
        // Это позволяет нам "поймать" весь HTML и обработать его перед отправкой
        ob_start();
        
        try {
            // Подключаем header
            $headerPath = Paths::get('public', 'header.php');
            if (!file_exists($headerPath)) {
                throw new \RuntimeException("Header файл не найден: {$headerPath}");
            }
            require $headerPath;
            
            // Подключаем основной контент
            $viewFile = Paths::get('views', $viewPath . '.php');
            if (!file_exists($viewFile)) {
                throw new \RuntimeException("View файл не найден: {$viewFile}");
            }
            require $viewFile;
            
            // Подключаем footer
            $footerPath = Paths::get('public', 'footer.php');
            if (!file_exists($footerPath)) {
                throw new \RuntimeException("Footer файл не найден: {$footerPath}");
            }
            require $footerPath;
            
            // Получаем весь накопленный HTML
            $content = ob_get_clean();
            
            // Применяем пост-обработку, если нужно
            if (!empty($options['minify'])) {
                $content = self::minifyHTML($content);
            }
            
            // Выводим результат
            echo $content;
            
        } catch (\Exception $e) {
            // Если произошла ошибка, очищаем буфер
            ob_end_clean();
            
            // Логируем ошибку
            Logger::error('Ошибка рендеринга шаблона', [
                'view' => $viewPath,
                'error' => $e->getMessage()
            ]);
            
            // Показываем страницу ошибки
            self::renderError($e);
        }
    }
    
    /**
     * Рендерит только контент без header/footer
     * Полезно для AJAX запросов
     */
    public static function renderPartial(string $viewPath, array $params = []): string
    {
        $params = array_merge($params, self::getGlobalParams());
        extract($params, EXTR_SKIP);
        
        ob_start();
        
        try {
            $viewFile = Paths::get('views', $viewPath . '.php');
            if (!file_exists($viewFile)) {
                throw new \RuntimeException("Partial view не найден: {$viewFile}");
            }
            require $viewFile;
            
            return ob_get_clean();
            
        } catch (\Exception $e) {
            ob_end_clean();
            Logger::error('Ошибка рендеринга partial', [
                'view' => $viewPath,
                'error' => $e->getMessage()
            ]);
            return '';
        }
    }
    
    /**
     * Рендерит JSON ответ
     * Удобно для API endpoints
     */
    public static function renderJson(array $data, int $statusCode = 200): void
    {
        http_response_code($statusCode);
        header('Content-Type: application/json; charset=utf-8');
        
        // Добавляем метаданные
        $response = [
            'success' => $statusCode >= 200 && $statusCode < 300,
            'timestamp' => date('c'),
            'data' => $data
        ];
        
        echo json_encode($response, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);
        exit;
    }
    
    /**
     * Глобальные параметры, доступные во всех шаблонах
     */
    private static function getGlobalParams(): array
    {
        return [
            // Информация о пользователе
            'currentUser' => AuthService::user(),
            'isLoggedIn' => AuthService::check(),
            'isAdmin' => AuthService::checkRole('admin'),
            
            // CSRF токен для форм
            'csrfToken' => CSRF::token(),
            
            // Текущий URL и путь
            'currentUrl' => $_SERVER['REQUEST_URI'] ?? '/',
            'currentPath' => parse_url($_SERVER['REQUEST_URI'] ?? '/', PHP_URL_PATH),
            
            // Вспомогательные функции для шаблонов
            'asset' => function($path) { return Paths::asset($path); },
            'url' => function($path) { return '/' . ltrim($path, '/'); },
            'old' => function($key, $default = '') { 
                return $_SESSION['old_input'][$key] ?? $default; 
            }
        ];
    }
    
    /**
     * Рендерит страницу ошибки
     */
    private static function renderError(\Exception $e): void
    {
        $isDevelopment = Config::get('app.debug', false);
        
        // В режиме разработки показываем подробную информацию
        if ($isDevelopment) {
            http_response_code(500);
            echo '<h1>Ошибка рендеринга</h1>';
            echo '<p><strong>Сообщение:</strong> ' . htmlspecialchars($e->getMessage()) . '</p>';
            echo '<p><strong>Файл:</strong> ' . htmlspecialchars($e->getFile()) . ':' . $e->getLine() . '</p>';
            echo '<pre>' . htmlspecialchars($e->getTraceAsString()) . '</pre>';
        } else {
            // В продакшене показываем красивую страницу ошибки
            http_response_code(500);
            echo '<h1>Произошла ошибка</h1>';
            echo '<p>Мы уже работаем над решением проблемы. Попробуйте обновить страницу через несколько минут.</p>';
        }
    }
    
    /**
     * Минификация HTML (удаление лишних пробелов)
     */
    private static function minifyHTML(string $html): string
    {
        // Удаляем комментарии (кроме IE условных)
        $html = preg_replace('/<!--(?!\[if).*?-->/s', '', $html);
        
        // Удаляем лишние пробелы
        $html = preg_replace('/\s+/', ' ', $html);
        
        // Удаляем пробелы вокруг тегов
        $html = preg_replace('/>\s+</', '><', $html);
        
        return trim($html);
    }
}