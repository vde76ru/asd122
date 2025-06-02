<?php
namespace App\Exceptions;

/**
 * Исключение базы данных
 */
class DatabaseException extends \Exception
{
    public function __construct(string $message = "Database error", int $code = 500, ?\Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }
}