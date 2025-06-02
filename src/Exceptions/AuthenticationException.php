<?php
namespace App\Exceptions;

/**
 * Исключение аутентификации
 */
class AuthenticationException extends \Exception
{
    public function __construct(string $message = "Authentication failed", int $code = 401, ?\Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }
}