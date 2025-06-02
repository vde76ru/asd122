<?php
// src/Core/Validator.php - простая версия для исправления ошибки 500
namespace App\Core;

/**
 * Простой валидатор данных
 * Минимальная реализация для работы BaseController
 */
class Validator
{
    private array $data;
    private array $rules;
    private array $errors = [];
    private array $validated = [];

    public function __construct(array $data, array $rules)
    {
        $this->data = $data;
        $this->rules = $rules;
    }

    /**
     * Выполнить валидацию
     */
    public function passes(): bool
    {
        $this->errors = [];
        $this->validated = [];

        foreach ($this->rules as $field => $rules) {
            $this->validateField($field, $rules);
        }

        return empty($this->errors);
    }

    /**
     * Проверить, не прошла ли валидация
     */
    public function fails(): bool
    {
        return !$this->passes();
    }

    /**
     * Получить ошибки валидации
     */
    public function errors(): array
    {
        return $this->errors;
    }

    /**
     * Получить провалидированные данные
     */
    public function validated(): array
    {
        return $this->validated;
    }

    /**
     * Валидация отдельного поля
     */
    private function validateField(string $field, $rules): void
    {
        if (is_string($rules)) {
            $rules = explode('|', $rules);
        }

        $value = $this->data[$field] ?? null;
        $isRequired = in_array('required', $rules);

        // Если поле обязательное и пустое
        if ($isRequired && $this->isEmpty($value)) {
            $this->addError($field, "Поле {$field} обязательно для заполнения");
            return;
        }

        // Если поле необязательное и пустое, пропускаем остальные правила
        if (!$isRequired && $this->isEmpty($value)) {
            return;
        }

        // Применяем правила валидации
        foreach ($rules as $rule) {
            if ($rule === 'required') continue;

            $this->applyRule($field, $value, $rule);
        }

        // Если нет ошибок, добавляем в провалидированные данные
        if (!isset($this->errors[$field])) {
            $this->validated[$field] = $value;
        }
    }

    /**
     * Применить конкретное правило валидации
     */
    private function applyRule(string $field, $value, string $rule): void
    {
        // Разбираем правило с параметрами (например, min:3)
        $parts = explode(':', $rule, 2);
        $ruleName = $parts[0];
        $parameter = $parts[1] ?? null;

        switch ($ruleName) {
            case 'string':
                if (!is_string($value)) {
                    $this->addError($field, "Поле {$field} должно быть строкой");
                }
                break;

            case 'integer':
            case 'int':
                if (!is_numeric($value) || (int)$value != $value) {
                    $this->addError($field, "Поле {$field} должно быть целым числом");
                } else {
                    // Преобразуем в число для validated
                    $this->data[$field] = (int)$value;
                }
                break;

            case 'email':
                if (!filter_var($value, FILTER_VALIDATE_EMAIL)) {
                    $this->addError($field, "Поле {$field} должно быть корректным email адресом");
                }
                break;

            case 'min':
                if ($parameter === null) {
                    break;
                }
                
                if (is_string($value) && strlen($value) < (int)$parameter) {
                    $this->addError($field, "Поле {$field} должно содержать не менее {$parameter} символов");
                } elseif (is_numeric($value) && $value < (float)$parameter) {
                    $this->addError($field, "Поле {$field} должно быть не менее {$parameter}");
                }
                break;

            case 'max':
                if ($parameter === null) {
                    break;
                }
                
                if (is_string($value) && strlen($value) > (int)$parameter) {
                    $this->addError($field, "Поле {$field} должно содержать не более {$parameter} символов");
                } elseif (is_numeric($value) && $value > (float)$parameter) {
                    $this->addError($field, "Поле {$field} должно быть не более {$parameter}");
                }
                break;

            case 'in':
                if ($parameter === null) {
                    break;
                }
                
                $allowedValues = explode(',', $parameter);
                if (!in_array($value, $allowedValues)) {
                    $this->addError($field, "Поле {$field} должно быть одним из: " . implode(', ', $allowedValues));
                }
                break;

            // Добавляем другие правила по мере необходимости
            default:
                // Неизвестное правило игнорируем
                break;
        }
    }

    /**
     * Добавить ошибку валидации
     */
    private function addError(string $field, string $message): void
    {
        if (!isset($this->errors[$field])) {
            $this->errors[$field] = [];
        }
        $this->errors[$field][] = $message;
    }

    /**
     * Проверить, пустое ли значение
     */
    private function isEmpty($value): bool
    {
        return $value === null || $value === '' || (is_array($value) && empty($value));
    }

    /**
     * Статический метод для быстрой валидации
     */
    public static function make(array $data, array $rules): self
    {
        return new self($data, $rules);
    }
}