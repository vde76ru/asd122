<?php
// Файл: src/Services/EmailService.php
namespace App\Services;

use App\Core\Database;

class EmailService
{
    private static $fromEmail = 'vde76ru@yandex.ru';
    private static $fromName = 'VDestor B2B';
    
    /**
     * Отправить email через очередь
     * Это круто, потому что не блокирует основной поток!
     */
    public static function queue(string $to, string $subject, string $template, array $data = [], int $priority = 5): int
    {
        return QueueService::push('email', [
            'to' => $to,
            'subject' => $subject,
            'template' => $template,
            'data' => $data
        ], $priority);
    }
    
    /**
     * Отправить email сразу (для критичных писем)
     */
    public static function sendNow(string $to, string $subject, string $html, string $text = ''): bool
    {
        // Подготавливаем заголовки
        $headers = [
            'MIME-Version: 1.0',
            'Content-type: text/html; charset=utf-8',
            'From: ' . self::$fromName . ' <' . self::$fromEmail . '>',
            'Reply-To: ' . self::$fromEmail,
            'X-Mailer: PHP/' . phpversion()
        ];
        
        // Логируем отправку
        self::logEmail($to, $subject, 'direct');
        
        // Отправляем
        return mail($to, $subject, $html, implode("\r\n", $headers));
    }
    
    /**
     * Обработать email из очереди
     * Вызывается из воркера очереди
     */
    public static function processQueuedEmail(array $payload): bool
    {
        $to = $payload['to'];
        $subject = $payload['subject'];
        $template = $payload['template'];
        $data = $payload['data'] ?? [];
        
        // Генерируем контент из шаблона
        $content = self::renderTemplate($template, $data);
        
        if (!$content) {
            throw new \Exception("Не удалось загрузить шаблон: $template");
        }
        
        // Отправляем
        return self::sendNow($to, $subject, $content['html'], $content['text'] ?? '');
    }
    
    /**
     * Рендерим email шаблон
     */
    private static function renderTemplate(string $template, array $data): array
    {
        $templatePath = __DIR__ . '/../views/emails/' . $template . '.php';
        
        if (!file_exists($templatePath)) {
            throw new \Exception("Шаблон не найден: $template");
        }
        
        // Извлекаем переменные для шаблона
        extract($data);
        
        // Буферизируем вывод
        ob_start();
        include $templatePath;
        $html = ob_get_clean();
        
        // Генерируем текстовую версию
        $text = strip_tags(str_replace(['<br>', '<br/>', '<br />'], "\n", $html));
        
        return [
            'html' => $html,
            'text' => $text
        ];
    }
    
    /**
     * Логируем все отправленные письма
     */
    private static function logEmail(string $to, string $subject, string $type): void
    {
        $pdo = Database::getConnection();
        $stmt = $pdo->prepare("
            INSERT INTO email_logs (recipient, subject, type, sent_at)
            VALUES (:to, :subject, :type, NOW())
        ");
        
        $stmt->execute([
            'to' => $to,
            'subject' => $subject,
            'type' => $type
        ]);
    }
    
    /**
     * Примеры использования:
     * 
     * // Добавить в очередь (рекомендуется)
     * EmailService::queue(
     *     'user@example.com',
     *     'Добро пожаловать!',
     *     'welcome',
     *     ['username' => 'Иван']
     * );
     * 
     * // Отправить сразу (для критичных)
     * EmailService::sendNow(
     *     'admin@example.com',
     *     'Критическая ошибка!',
     *     '<h1>Ошибка!</h1><p>Что-то сломалось!</p>'
     * );
     */
}

// SQL для таблицы логов:
/*
CREATE TABLE `email_logs` (
  `id` int NOT NULL AUTO_INCREMENT,
  `recipient` varchar(255) NOT NULL,
  `subject` varchar(255) NOT NULL,
  `type` varchar(50) NOT NULL,
  `sent_at` datetime NOT NULL,
  `opened_at` datetime DEFAULT NULL,
  `clicked_at` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_recipient` (`recipient`),
  KEY `idx_sent_at` (`sent_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
*/