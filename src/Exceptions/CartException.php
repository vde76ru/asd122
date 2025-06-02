<?php
namespace App\Exceptions;

class CartException extends \Exception
{
    public function __construct(string $message = "Cart operation failed", int $code = 400, ?\Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }
}