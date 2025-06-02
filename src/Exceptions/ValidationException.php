<?php
namespace App\Exceptions;

/**
 * Исключение валидации данных
 */
class ValidationException extends \Exception
{
    private array $errors;

    public function __construct(string $message, array $errors = [], int $code = 0, ?\Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);
        $this->errors = $errors;
    }

    /**
     * Получить ошибки валидации
     */
    public function getErrors(): array
    {
        return $this->errors;
    }

    /**
     * Получить первую ошибку
     */
    public function getFirstError(): string
    {
        if (empty($this->errors)) {
            return $this->getMessage();
        }

        $firstField = array_key_first($this->errors);
        $firstError = $this->errors[$firstField];
        
        return is_array($firstError) ? $firstError[0] : $firstError;
    }

    /**
     * Преобразовать в JSON ответ
     */
    public function toResponse(): array
    {
        return [
            'success' => false,
            'message' => $this->getMessage(),
            'errors' => $this->errors
        ];
    }
}