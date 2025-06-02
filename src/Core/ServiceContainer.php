<?php
namespace App\Core;

class ServiceContainer
{
    private static array $services = [];
    private static array $factories = [];
    
    public static function register(string $name, callable $factory): void
    {
        self::$factories[$name] = $factory;
    }
    
    public static function get(string $name)
    {
        if (!isset(self::$services[$name])) {
            if (!isset(self::$factories[$name])) {
                throw new \RuntimeException("Service {$name} not registered");
            }
            self::$services[$name] = self::$factories[$name]();
        }
        
        return self::$services[$name];
    }
}