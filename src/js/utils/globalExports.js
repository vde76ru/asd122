// utils/globalExports.js
export function exportToWindow(exports) {
    Object.entries(exports).forEach(([name, func]) => {
        window[name] = func;
    });
}