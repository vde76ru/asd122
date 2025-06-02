<?php
namespace App\Services;

use App\Core\Database;


class AuditService
{
    /**
     * Записывает событие в таблицу audit_logs
     *
     * @param int|null $userId     ID пользователя, если есть (или null для гостя)
     * @param string   $action     Код действия, например 'add_to_cart'
     * @param string   $objectType Тип объекта, например 'cart'
     * @param int|null $objectId   ID связанного объекта (например spec_id), или null
     * @param array    $details    Дополнительные данные события
     */
    public static function log(?int $userId, string $action, string $objectType, ?int $objectId, array $details = []): void
    {
        $pdo = Database::getConnection();
        $stmt = $pdo->prepare("
            INSERT INTO audit_logs 
                (user_id, session_id, action, object_type, object_id, details, created_at)
            VALUES 
                (:uid, :sid, :act, :otype, :oid, :det, NOW())
        ");
        $stmt->execute([
            'uid'   => $userId,             // может быть NULL
            'sid'   => session_id(),        // всегда есть
            'act'   => $action,
            'otype' => $objectType,
            'oid'   => $objectId,
            'det'   => json_encode($details, JSON_UNESCAPED_UNICODE),
        ]);
    }
}