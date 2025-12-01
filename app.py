# app.py
from mongoDB import MongoDB
from elastic import ElasticSearch
from functions import funciones

from flask import (
    Flask, render_template, request,
    redirect, url_for, session, flash
)
from dotenv import load_dotenv
import os

# Importar helpers (según tu estructura)
# from Helpers.mongoDB import MongoDB
# from Helpers.elastic import ElasticSearch
# from Helpers.funciones import funciones

# ================== CARGAR VARIABLES DE ENTORNO ==================
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'clave_super_secreta_12345')

# ================== CONFIGURACIÓN MONGODB ==================
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB = os.getenv('MONGO_DB')
MONGO_COLECCION = os.getenv('MONGO_COLECCION', 'usuario_roles')

# ================== CONFIGURACIÓN ELASTICSEARCH CLOUD ==================
ELASTIC_CLOUD_URL = os.getenv('ELASTIC_CLOUD_URL')
ELASTIC_API_KEY = os.getenv('ELASTIC_API_KEY')

# ================== METADATOS DE LA APLICACIÓN ==================
VERSION_APP = "1.0.0"
CREATOR_APP = "MabelAyala"

# ================== INICIALIZAR CONEXIONES ==================
mongo = MongoDB(MONGO_URI, MONGO_DB, MONGO_COLECCION)
elastic = ElasticSearch(ELASTIC_CLOUD_URL, ELASTIC_API_KEY)
utils = funciones() 

# ================== DECORADOR PARA RUTAS PROTEGIDAS ==================
from functools import wraps

def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if 'usuario' not in session:
            flash("Debes iniciar sesión para acceder a esta página.", "warning")
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return wrapper


# ================== RUTAS ==================

@app.route('/')
def landing():
    """Landing page pública"""
    return render_template(
        'landing.html',
        version=VERSION_APP,
        creador=CREATOR_APP
    )


@app.route('/login', methods=['GET', 'POST'])
def login():
    """
    Página de login.
    GET  -> muestra el formulario
    POST -> valida credenciales contra MongoDB
    """
    error_message = None

    if request.method == 'POST':
        usuario = request.form.get('usuario')
        password = request.form.get('password')

        # Aquí usamos un método de tu helper de Mongo.
        # Ajusta el nombre según cómo lo implementes en Helpers/mongoDB.py
        user_doc = mongo.validar_usuario(usuario, password)

        if user_doc:
            # Guardar datos mínimos en sesión
            session['usuario'] = usuario
            session['rol'] = user_doc.get('rol', 'Usuario')

            flash('Inicio de sesión exitoso.', 'success')
            return redirect(url_for('admin'))
        else:
            error_message = "Usuario o contraseña incorrectos."

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
    Panel admin principal.
    Más adelante aquí pondremos enlaces a:
    - Admin usuarios (Mongo)
    - Admin Elastic
    - Carga de archivos / índice PLN
    """
    return render_template(
        'admin.html',   # cuando lo tengamos creado
        version=VERSION_APP,
        creador=CREATOR_APP,
        usuario=session.get('usuario'),
        rol=session.get('rol')
    )


# =============== RUTAS EXTRA OPCIONALES (NAVBAR) ===============

@app.route('/about')
def about():
    # Por ahora algo sencillo. Luego puedes crear about.html.
    return render_template(
        'about.html',
        version=VERSION_APP,
        creador=CREATOR_APP
    )


@app.route('/contacto')
def contacto():
    # Igual, más adelante puedes hacer contacto.html bonito.
    return render_template(
        'contacto.html',
        version=VERSION_APP,
        creador=CREATOR_APP
    )


# ================== MAIN ==================
if __name__ == '__main__':
    # Crear carpetas necesarias (p.e. para uploads)
    Funciones.crear_carpeta('static/uploads')

    # Verificar conexiones
    print("\n" + "=" * 50)
    print("VERIFICANDO CONEXIONES")

    if mongo.test_connection():
        print("✅ MongoDB Atlas: Conectado")
    else:
        print("❌ MongoDB Atlas: Error de conexión")

    if elastic.test_connection():
        print("✅ ElasticSearch Cloud: Conectado")
    else:
        print("❌ ElasticSearch Cloud: Error de conexión")

    print("=" * 50 + "\n")

    # Levantar la app
    app.run(debug=True)
