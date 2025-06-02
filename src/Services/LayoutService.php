<?php
namespace App\Services;

class LayoutService
{
    public static function getCities(): array
    {
        static $cities = null;
        
        if ($cities === null) {
            try {
                $stmt = Database::query("SELECT city_id, name FROM cities ORDER BY name");
                $cities = $stmt->fetchAll();
            } catch (\Exception $e) {
                Logger::error('Failed to load cities', ['error' => $e->getMessage()]);
                $cities = [];
            }
        }
        
        return $cities;
    }
    
    public static function getCSPDirectives(): array
    {
        return [
            "default-src" => "'self'",
            "script-src" => "'self' 'unsafe-inline' https://cdnjs.cloudflare.com",
            "style-src" => "'self' 'unsafe-inline' https://fonts.googleapis.com",
            "font-src" => "'self' https://fonts.gstatic.com data:",
            "img-src" => "'self' data: https: blob:",
            "connect-src" => "'self'",
            "frame-src" => "'self'",
            "object-src" => "'none'",
            "base-uri" => "'self'",
            "form-action" => "'self'"
        ];
    }
}
