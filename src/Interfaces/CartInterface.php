<?php
namespace App\Interfaces;

interface CartInterface
{
    public function get(): array;
    public function add(int $productId, int $quantity): array;
    public function update(int $productId, int $quantity): array;
    public function remove(int $productId): array;
    public function clear(): void;
    public function getStats(): array;
}