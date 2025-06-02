<div class="container mt-5">
    <h1>Вход</h1>
    <form method="post" autocomplete="off">
        <input type="hidden" name="csrf_token" value="<?= htmlspecialchars(\App\Core\CSRF::token(), ENT_QUOTES) ?>">
        <div class="mb-3">
            <label class="form-label">Имя пользователя или Email</label>
            <input type="text" name="username" class="form-control" required>
        </div>
        <div class="mb-3">
            <label class="form-label">Пароль</label>
            <input type="password" name="password" class="form-control" required>
        </div>
        <?php if ($error): ?>
            <div class="alert alert-danger"><?= htmlspecialchars($error, ENT_QUOTES) ?></div>
        <?php endif; ?>
        <button type="submit" class="btn btn-primary">Войти</button>
    </form>
</div>