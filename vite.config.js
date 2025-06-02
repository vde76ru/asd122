import { defineConfig } from "vite";
import path from 'path';

export default defineConfig({
  // Корневая директория - там где находится vite.config.js
  root: path.resolve(__dirname),
  
  // РЕШЕНИЕ ПРОБЛЕМЫ: отключаем копирование public директории
  publicDir: false,
  
  // Настройки сборки
  build: {
    // Выходная директория для скомпилированных файлов
    outDir: path.resolve(__dirname, 'public/assets/dist'),
    
    // Очищаем директорию перед сборкой
    emptyOutDir: true,
    
    // Настройки для rollup
    rollupOptions: {
      input: {
        // Основной файл входа
        main: path.resolve(__dirname, 'src/js/main.js'),
      },
      
      output: {
        // Настройки именования файлов
        entryFileNames: 'assets/[name]-[hash].js',
        chunkFileNames: 'assets/[name]-[hash].js',
        assetFileNames: (assetInfo) => {
          // Группируем ассеты по типам
          if (assetInfo.name.endsWith('.css')) {
            return 'assets/[name]-[hash][extname]';
          }
          if (/\.(png|jpe?g|gif|svg|webp|ico)$/i.test(assetInfo.name)) {
            return 'images/[name]-[hash][extname]';
          }
          if (/\.(woff2?|eot|ttf|otf)$/i.test(assetInfo.name)) {
            return 'fonts/[name]-[hash][extname]';
          }
          return 'assets/[name]-[hash][extname]';
        }
      }
    },
    
    // Настройки минификации
    minify: 'terser',
    terserOptions: {
      compress: {
        drop_console: true, // Удаляем console.log в продакшене
        drop_debugger: true
      }
    },
    
    // Генерируем source maps только для разработки
    sourcemap: process.env.NODE_ENV !== 'production',
    
    // Настройка чанков
    chunkSizeWarningLimit: 1000, // Предупреждение при размере чанка > 1MB
  },
  
  // Алиасы для удобных импортов
  resolve: {
    alias: {
      '@': path.resolve(__dirname, 'src'),
      '@js': path.resolve(__dirname, 'src/js'),
      '@css': path.resolve(__dirname, 'src/css'),
      '@components': path.resolve(__dirname, 'src/js/components'),
      '@services': path.resolve(__dirname, 'src/js/services'),
      '@utils': path.resolve(__dirname, 'src/js/utils')
    }
  },
  
  // Настройки сервера разработки
  server: {
    port: 3000,
    proxy: {
      // Проксируем API запросы на PHP сервер
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true
      },
      '/cart': {
        target: 'http://localhost:8000',
        changeOrigin: true
      },
      '/specification': {
        target: 'http://localhost:8000',
        changeOrigin: true
      }
    }
  },
  
  // Оптимизации
  optimizeDeps: {
    include: ['jquery'] // Предварительно бандлим jQuery
  }
});