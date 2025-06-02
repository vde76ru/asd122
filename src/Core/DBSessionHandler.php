<?php
namespace App\Core;

use SessionHandlerInterface;
use PDO;

class DBSessionHandler implements SessionHandlerInterface
{
    private PDO $pdo;
    private int $lifetime;

    public function __construct(PDO $pdo, int $lifetime)
    {
        $this->pdo      = $pdo;
        $this->lifetime = $lifetime;
    }

    public function open($savePath, $sessionName): bool
    {
        return true;
    }

    public function close(): bool
    {
        return true;
    }

    public function read($sessionId): string
    {
        error_log("[DBSessionHandler] ▶ read() SID={$sessionId}");
        $stmt = $this->pdo->prepare(
            "SELECT data FROM sessions WHERE session_id = :sid AND expires_at > NOW() LIMIT 1"
        );
        $stmt->execute(['sid' => $sessionId]);
        $data = (string)$stmt->fetchColumn();
        error_log("[DBSessionHandler] ◀ read() returned length=".strlen($data));
        return $data;
    }

    public function write($sessionId, $data): bool
    {
        error_log("[DBSessionHandler] ▶ write() SID={$sessionId} length=".strlen($data));
        $expires = date('Y-m-d H:i:s', time() + $this->lifetime);
        $stmt = $this->pdo->prepare("
            INSERT INTO sessions (session_id, data, created_at, expires_at)
            VALUES (:sid, :data, NOW(), :expires)
            ON DUPLICATE KEY UPDATE
                data       = VALUES(data),
                expires_at = VALUES(expires_at)
        ");
        $res = $stmt->execute([
            'sid'     => $sessionId,
            'data'    => $data,
            'expires' => $expires,
        ]);
        error_log("[DBSessionHandler] ◀ write() result=" . ($res ? 'OK' : 'FAIL'));
        return $res;
    }

    public function destroy($sessionId): bool
    {
        $stmt = $this->pdo->prepare("DELETE FROM sessions WHERE session_id = :sid");
        return $stmt->execute(['sid' => $sessionId]);
    }

    public function gc($maxlifetime): int|false
    {
        $stmt = $this->pdo->prepare("DELETE FROM sessions WHERE expires_at < NOW()");
        return $stmt->execute() ? $stmt->rowCount() : false;
    }
}