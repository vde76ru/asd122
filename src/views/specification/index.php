<?php
use App\Services\AuthService;
?>
<div class="container mt-5">
    <h1>Мои спецификации</h1>

    <?php if (empty($specs)): ?>
        <p>Вы ещё не создавали спецификаций.</p>
    <?php else: ?>
        <table class="table table-striped">
            <thead>
                <tr>
                    <th>№</th>
                    <th>Дата создания</th>
                    <th>Позиций</th>
                    <th>Сумма</th>
                    <th>Действие</th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($specs as $spec): 
                    $id    = $spec['specification_id'];
                    $date  = date('d.m.Y H:i', strtotime($spec['created_at']));
                    $total = number_format($summaries[$id], 2);
                    $count = $itemsCount[$id] ?? 0;
                ?>
                <tr>
                    <td><?= $id ?></td>
                    <td><?= $date ?></td>
                    <td><?= $count ?></td>
                    <td><?= $total ?> руб.</td>
                    <td>
                        <a href="/specification/<?= $id ?>" class="btn btn-sm btn-primary">
                            Открыть
                        </a>
                    </td>
                </tr>
                <?php endforeach; ?>
            </tbody>
        </table>
    <?php endif; ?>
</div>