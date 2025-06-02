<?php
namespace App\Controllers;

use App\Core\Logger;
use App\Services\AuthService;

abstract class BaseController
{
    /**
     * Прямая валидация без класса Validator
     */
    protected function validate(array $data, array $rules): array
    {
        $errors = [];
        $validated = [];
        
        foreach ($rules as $field => $rule) {
            $value = $data[$field] ?? null;
            $rulesList = explode('|', $rule);
            
            foreach ($rulesList as $singleRule) {
                if ($singleRule === 'required' && empty($value)) {
                    $errors[$field] = "Поле $field обязательно";
                    break;
                }
                
                if ($singleRule === 'integer' && $value !== null && !is_numeric($value)) {
                    $errors[$field] = "Поле $field должно быть числом";
                    break;
                }
            }
            
            if (!isset($errors[$field])) {
                $validated[$field] = $value;
            }
        }
        
        if (!empty($errors)) {
            throw new \InvalidArgumentException(json_encode($errors));
        }
        
        return $validated;
    }

    protected function jsonResponse(array $data, int $statusCode = 200): void
    {
        http_response_code($statusCode);
        header('Content-Type: application/json; charset=utf-8');
        header('X-Content-Type-Options: nosniff');
        echo json_encode($data, JSON_UNESCAPED_UNICODE);
        exit;
    }

    protected function success($data = null, string $message = 'Success'): void
    {
        $this->jsonResponse([
            'success' => true,
            'message' => $message,
            'data' => $data
        ]);
    }

    protected function error(string $message, int $statusCode = 400, array $errors = []): void
    {
        $this->jsonResponse([
            'success' => false,
            'message' => $message,
            'errors' => $errors
        ], $statusCode);
    }

    protected function getInput(): array
    {
        return array_merge($_GET, $_POST);
    }

    protected function requireAuth(): array
    {
        if (!AuthService::validateSession()) {
            $this->error('Authentication required', 401);
        }
        
        return [
            'user_id' => $_SESSION['user_id'],
            'username' => $_SESSION['username'],
            'role' => $_SESSION['role']
        ];
    }

    protected function requireRole(string $role): void
    {
        $user = $this->requireAuth();
        if ($user['role'] !== $role && $user['role'] !== 'admin') {
            $this->error('Insufficient permissions', 403);
        }
    }
}