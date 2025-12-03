# app.py
from flask import (
    Flask, render_template, request,
    redirect, url_for, session, flash, jsonify
)
from dotenv import load_dotenv
from functools import wraps
import os

# Importar solo lo que SÍ vamos a usar por ahora
from elastic import ElasticSearch
from functions import funciones
import mongo

# ================== CARGAR VARIABLES DE ENTORNO ==================
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'clave_super_secreta_12345')

# ================== CONFIGURACIÓN MONGO ==================
# ================== CONFIGURACIÓN MONGO ==================
MONGO_URI = "mongodb+srv://mayala:mayala123@mayala.y4cqo9f.mongodb.net/?retryWrites=true&w=majority&appName=mayala"
MONGO_DB = "proyecto_bigData"
MONGO_COLECCION = "usuario_roles"

print("DEBUG MONGO CONFIG:", MONGO_URI, MONGO_DB, MONGO_COLECCION)


print("DEBUG MONGO CONFIG:", MONGO_URI, MONGO_DB, MONGO_COLECCION)
if not MONGO_URI:
    print("⚠️ ATENCIÓN: MONGO_URI no está definida. Revisa el .env.")

# ================== CONFIGURACIÓN ELASTICSEARCH CLOUD ==================
ELASTIC_CLOUD_URL = os.getenv(
    'ELASTIC_URL',
    "https://fe68aba5b3194046b86205bc65ddcf71.us-central1.gcp.cloud.es.io:443"
)
ELASTIC_API_KEY = os.getenv(
    'ELASTIC_API_KEY',
    'ZXphVTBwb0JBS1JFTTg4bzc4clY6UWlMaXQ5ZHVyZEVzTEh5amJtOWpEZw=='
)

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

# Landing en "/"
@app.route('/', endpoint='landing')
def landing():
    """Landing page pública"""
    return render_template(
        'landing.html',
        version=VERSION_APP,
        creador=CREATOR_APP
    )


# Buscador en "/buscador" (vista)
@app.route('/buscador')
def buscador():
    """
    Página pública del buscador de términos.
    """
    return render_template(
        'buscador.html',
        version=VERSION_APP,
        creador=CREATOR_APP
    )


# ====== API del buscador (conexión real a Elastic) ======
@app.route('/api/buscar', methods=['GET'])
def api_buscar():
    """
    Endpoint que consulta ElasticSearch.
    Ejemplo: /api/buscar?q=palabra&index=mi_indice
    """
    query = request.args.get('q', '').strip()
    index_name = request.args.get('index', 'lenguaje_controlado')
    size = int(request.args.get('size', 10))

    if not query:
        return jsonify({"error": "Parámetro 'q' es obligatorio"}), 400

    try:
        resultados = elastic.buscar_texto(
            index_name=index_name,
            query=query,
            size=size
        )
        return jsonify(resultados)
    except Exception as e:
        print("Error al buscar en Elastic:", e)
        return jsonify({"error": "Error al conectar con ElasticSearch"}), 500


# ====== LOGIN CON MONGO ======
@app.route('/login', methods=['GET', 'POST'])
def login():
    """
    Página de login con validación REAL en MongoDB.
    """
    error_message = None

    if request.method == 'POST':
        usuario = request.form.get('usuario', '').strip()
        password = request.form.get('password', '').strip()

        if not usuario or not password:
            error_message = 'Por favor ingresa usuario y contraseña.'
        else:
            print("\n=== DEBUG LOGIN ===")
            print("Usuario enviado:", usuario)

            try:
                user_data = mongo.validar_usuario(
                    usuario,
                    password,
                    MONGO_URI,
                    MONGO_DB,
                    MONGO_COLECCION
                )
                print("user_data devuelto por Mongo:", user_data)
            except Exception as e:
                print("ERROR VALIDANDO USUARIO EN MONGO:", repr(e))
                error_message = 'Error al conectar con la base de datos.'
                user_data = None

            if user_data:
                # Guardar sesión
                session['usuario'] = user_data.get('usuario', usuario)
                session['rol'] = user_data.get('rol', 'Usuario')
                session['permisos'] = user_data.get('permisos', {})
                session['logged_in'] = True

                flash('Inicio de sesión exitoso.', 'success')
                return redirect(url_for('admin'))
            else:
                if not error_message:
                    error_message = 'Usuario o contraseña incorrectos.'

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
    """
    permisos = session.get('permisos', {})
    return render_template(
        'admin.html',
        version=VERSION_APP,
        creador=CREATOR_APP,
        usuario=session.get('usuario'),
        permisos=permisos
    )

@app.route('/listar-usuarios', methods=['GET'])
@login_required
def listar_usuarios_route():
    """
    Devuelve en JSON los usuarios registrados en MongoDB.
    """
    # Opcional: control por permisos
    permisos = session.get('permisos', {})
    if not permisos.get('admin_usuarios', True):  # pon True mientras pruebas
        return jsonify({"error": "No autorizado"}), 403

    try:
        usuarios = mongo.listar_usuarios(MONGO_URI, MONGO_DB, MONGO_COLECCION)
        print("USUARIOS ENCONTRADOS:", usuarios)
        return jsonify(usuarios)
    except Exception as e:
        print("ERROR LISTANDO USUARIOS:", repr(e))
        return jsonify({"error": str(e)}), 500

# =============== RUTAS EXTRA OPCIONALES (NAVBAR) ===============

@app.route('/about')
def about():
    return render_template(
        'about.html',
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

    if elastic.ping():
        print("✅ ElasticSearch Cloud: Conectado")
    else:
        print("❌ ElasticSearch Cloud: Error de conexión")

    print("=" * 50 + "\n")

    # Levantar la app en local
    app.run(debug=True)

