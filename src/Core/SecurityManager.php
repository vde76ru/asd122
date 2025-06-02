<?php
namespace App\Core;

class SecurityManager
{
    public static function initialize(): void
    {
        self::setSecurityHeaders();
        self::setupCSP();
        self::validateRequest();
    }

    private static function setSecurityHeaders(): void
    {
        $headers = [
            'X-Content-Type-Options' => 'nosniff',
            'X-Frame-Options' => 'DENY',
            'X-XSS-Protection' => '1; mode=block',
            'Referrer-Policy' => 'strict-origin-when-cross-origin',
            'Permissions-Policy' => 'camera=(), microphone=(), geolocation=()'
        ];

        foreach ($headers as $header => $value) {
            header("{$header}: {$value}");
        }

        if (self::isHttps()) {
            header('Strict-Transport-Security: max-age=31536000; includeSubDomains');
        }
    }

    private static function setupCSP(): void
    {
        $csp = [
            "default-src" => "'self'",
            "script-src" => "'self' 'unsafe-inline'",
            "style-src" => "'self' 'unsafe-inline'",
            "img-src" => "'self' data: https:",
            "connect-src" => "'self'",
            "font-src" => "'self' data:",
            "object-src" => "'none'",
            "base-uri" => "'self'",
            "form-action" => "'self'"
        ];

        $cspString = implode('; ', array_map(
            fn($key, $value) => "{$key} {$value}",
            array_keys($csp),
            $csp
        ));

    //   header("Content-Security-Policy: {$cspString}");
    }

    private static function validateRequest(): void
    {
        // Проверка размера запроса
        $contentLength = (int)($_SERVER['CONTENT_LENGTH'] ?? 0);
        if ($contentLength > 10485760) { // 10MB
            http_response_code(413);
            exit('Request too large');
        }

        // Проверка метода запроса
        $allowedMethods = ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'HEAD'];
        if (!in_array($_SERVER['REQUEST_METHOD'], $allowedMethods)) {
            http_response_code(405);
            exit('Method not allowed');
        }
    }

    private static function isHttps(): bool
    {
        return (!empty($_SERVER['HTTPS']) && $_SERVER['HTTPS'] !== 'off') ||
               $_SERVER['SERVER_PORT'] == 443;
    }
}