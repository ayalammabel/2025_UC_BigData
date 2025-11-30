// static/js/main.js

document.addEventListener('DOMContentLoaded', function () {
    // 1. Ocultar alerts automáticamente
    const alerts = document.querySelectorAll('.alert-auto-dismiss');
    if (alerts.length > 0) {
        setTimeout(() => {
            alerts.forEach(alert => {
                alert.style.transition = 'opacity 0.4s ease';
                alert.style.opacity = '0';
                setTimeout(() => alert.remove(), 400);
            });
        }, 5000); // 5 segundos
    }

    // 2. Loader al enviar formularios pesados
    const loader = document.querySelector('.loader-overlay');
    const forms = document.querySelectorAll('form.js-show-loader');

    forms.forEach(form => {
        form.addEventListener('submit', () => {
            if (loader) {
                loader.style.display = 'flex';
            }
        });
    });
});
