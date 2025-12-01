# app.py
from flask import (
    Flask, render_template, request,
    redirect, url_for, session, flash
)
from dotenv import load_dotenv
from functools import wraps
import os

# Importar solo lo que SÍ vamos a usar por ahora
from elastic import ElasticSearch
from functions import funciones

# ================== CARGAR VARIABLES DE ENTORNO ==================
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'clave_super_secreta_12345')

# ================== CONFIGURACIÓN ELASTICSEARCH CLOUD ==================
ELASTIC_CLOUD_URL = os.getenv('ELASTIC_CLOUD_URL')
ELASTIC_API_KEY   = os.getenv('ELASTIC_API_KEY')

# ================== METADATOS DE LA APLICACIÓN ==================
VERSION_APP = "1.0.0"
CREATOR_APP = "MabelAyala"

# ================== INICIALIZAR CONEXIONES ==================
elastic = ElasticSearch(ELASTIC_CLOUD_URL, ELASTIC_API_KEY)
utils = funciones()   # instancia de la clase funciones


# ================== DECORADOR PARA RUTAS PROTEGIDAS ==================
def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if 'usuario' not in session:
            flash("Debes iniciar sesión para acceder a esta página.", "warning")
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return wrapper


# ================== RUTAS ==================

# ================== RUTAS ==================

# Landing en "/"
@app.route('/', endpoint='landing')
def landing():
    """Landing page pública"""
    return render_template(
        'landing.html',
        version=VERSION_APP,
        creador=CREATOR_APP
    )

# Buscador en "/buscador"
@app.route('/buscador')
def buscador():
    """
    Página pública del buscador de términos.
    Más adelante aquí conectamos con ElasticSearch.
    """
    return render_template(
        'buscador.html',
        version=VERSION_APP,
        creador=CREATOR_APP
    )



@app.route('/login', methods=['GET', 'POST'])
def login():
    """
    Página de login.
    Por ahora NO valida contra MongoDB, solo simula el login.
    """
    error_message = None

    if request.method == 'POST':
        usuario = request.form.get('usuario') or "invitado"

        # Simulamos login exitoso sin Mongo
        session['usuario'] = usuario
        session['rol'] = 'Usuario'

        flash('Inicio de sesión simulado (sin MongoDB).', 'success')
        return redirect(url_for('admin'))

    return render_template(
        'login.html',
        error_message=error_message,
        version=VERSION_APP,
        creador=CREATOR_APP
    )


@app.route('/logout')
def logout():
    """Cerrar sesión y volver a la landing."""
    session.clear()
    flash("Sesión cerrada correctamente.", "info")
    return redirect(url_for('landing'))


@app.route('/admin')
@login_required
def admin():
    """
    Panel admin principal (placeholder).
    Más adelante aquí pondremos:
    - Admin usuarios
    - Admin Elastic
    - Carga de archivos / índice PLN
    """
    return render_template(
        'admin.html',
        version=VERSION_APP,
        creador=CREATOR_APP,
        usuario=session.get('usuario'),
        rol=session.get('rol')
    )


# =============== RUTAS EXTRA OPCIONALES (NAVBAR) ===============

@app.route('/about')
def about():
    return render_template(
        'about.html',
        version=VERSION_APP,
        creador=CREATOR_APP
    )


@app.route('/contacto')
def contacto():
    return render_template(
        'contacto.html',
        version=VERSION_APP,
        creador=CREATOR_APP
    )


# ================== MAIN (solo cuando corres localmente) ==================
if __name__ == '__main__':
    # Crear carpetas necesarias (p.e. para uploads)
    utils.crear_carpeta('static/uploads')

    # Verificar conexión a Elastic
    print("\n" + "=" * 50)
    print("VERIFICANDO CONEXIÓN A ELASTICSEARCH")

    if elastic.test_connection():
        print("✅ ElasticSearch Cloud: Conectado")
    else:
        print("❌ ElasticSearch Cloud: Error de conexión")

    print("=" * 50 + "\n")

    # Levantar la app en local
    app.run(debug=True)
