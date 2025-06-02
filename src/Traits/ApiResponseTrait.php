<?php
namespace App\Traits;

trait ApiResponseTrait
{
    protected function apiSuccess($data = null, string $message = 'Success'): void
    {
        $this->jsonResponse([
            'success' => true,
            'message' => $message,
            'data' => $data,
            'timestamp' => date('c')
        ]);
    }
    
    protected function apiError(string $message, int $code = 400, array $errors = []): void
    {
        $this->jsonResponse([
            'success' => false,
            'message' => $message,
            'errors' => $errors,
            'timestamp' => date('c')
        ], $code);
    }
    
    protected function jsonResponse(array $data, int $statusCode = 200): void
    {
        http_response_code($statusCode);
        header('Content-Type: application/json; charset=utf-8');
        header('X-Content-Type-Options: nosniff');
        echo json_encode($data, JSON_UNESCAPED_UNICODE);
        exit;
    }
}