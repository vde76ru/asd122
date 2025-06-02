<?php
namespace App\Controllers;

use App\Core\Layout;
use App\Services\AuthService;

class AdminController
{
    public function indexAction(): void
    {
        if (!AuthService::checkRole('admin')) {
            header('Location: /login');
            exit;
        }
        $user = AuthService::user();
        Layout::render('admin/index', ['user' => $user]);
    }

}