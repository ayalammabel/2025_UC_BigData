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
def buscar():
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
@app.route('/admin/usuarios')
@login_required
def admin_usuarios():
    """
    Sección de gestión de usuarios.
    Aquí luego podemos:
    - listar usuarios (reutilizando /listar-usuarios),
    - crear / editar / eliminar usuarios, etc.
    """
    permisos = session.get('permisos', {})
    return render_template(
        'admin_usuarios.html',
        version=VERSION_APP,
        creador=CREATOR_APP,
        usuario=session.get('usuario'),
        permisos=permisos
    )


@app.route('/admin/elastic')
@login_required
def admin_elastic():
    """
    Sección de gestión de Elastic.
    Aquí luego podremos:
    - ver estado del índice,
    - hacer pruebas de búsqueda,
    - ver conteos, etc.
    """
    permisos = session.get('permisos', {})
    return render_template(
        'admin_elastic.html',
        version=VERSION_APP,
        creador=CREATOR_APP,
        usuario=session.get('usuario'),
        permisos=permisos
    )


@app.route('/admin/carga-archivos', methods=['GET', 'POST'])
@login_required
def admin_carga_archivos():
    """
    Página para cargar archivos que luego se indexarán en ElasticSearch.
    Por ahora solo guarda los archivos en disco y muestra mensajes.
    """
    # Permiso especial (opcional: si no quieres restricción, comenta este bloque)
    permisos = session.get('permisos', {})
    if not permisos.get('admin_data_elastic', True):
        flash('No tienes permisos para cargar archivos en ElasticSearch.', 'danger')
        return redirect(url_for('admin'))

    if request.method == 'POST':
        metodo = request.form.get('metodo')
        indice_destino = request.form.get('indice_destino', 'lenguaje_controlado')

        # Carpeta donde guardaremos temporalmente los archivos
        upload_dir = os.path.join('static', 'uploads', 'admin_carga_archivos')
        os.makedirs(upload_dir, exist_ok=True)

        if metodo == 'zip_json':
            # Archivos ZIP / JSON subidos
            files = request.files.getlist('archivos_zipjson')
            guardados = []

            for f in files:
                if f and f.filename:
                    filename = secure_filename(f.filename)
                    path = os.path.join(upload_dir, filename)
                    f.save(path)
                    guardados.append(path)

            flash(f'Se cargaron {len(guardados)} archivo(s) para el índice "{indice_destino}". '
                  'Falta implementar la indexación en Elastic.', 'success')

        elif metodo == 'json_suelto':
            files = request.files.getlist('archivos_json')
            guardados = []

            for f in files:
                if f and f.filename:
                    filename = secure_filename(f.filename)
                    path = os.path.join(upload_dir, filename)
                    f.save(path)
                    guardados.append(path)

            flash(f'Se cargaron {len(guardados)} JSON sueltos para el índice "{indice_destino}". '
                  'Falta implementar la indexación.', 'success')

        elif metodo == 'web_scraping':
            url_scraping = request.form.get('url_scraping', '').strip()
            extensiones = request.form.get('extensiones', '').strip()
            tipos_archivos = request.form.get('tipos_archivos', '').strip()

            # Aquí en el futuro llamarías a una función de web scraping
            # utils.ejecutar_scraping(url_scraping, extensiones, tipos_archivos, indice_destino)
            flash('La configuración de web scraping se recibió correctamente. '
                  'Falta implementar el proceso de scraping e indexación.', 'info')

        else:
            flash('Método de carga no reconocido.', 'danger')

        return redirect(url_for('admin_carga_archivos'))

    # GET: solo renderizamos la página
    return render_template(
        'admin_carga_archivos.html',
        version=VERSION_APP,
        creador=CREATOR_APP
    )

@app.route('/api/usuarios', methods=['GET', 'POST'])
@login_required
def api_usuarios():
    """
    API de administración de usuarios.
    GET  -> lista usuarios
    POST -> crea usuario nuevo
    """
    permisos = session.get('permisos', {})
    # Si quieres controlar por permiso:
    if not permisos.get('admin_usuarios', True):
        return jsonify({"error": "No autorizado"}), 403

    if request.method == 'GET':
        try:
            usuarios = mongo.listar_usuarios_tabla(
                MONGO_URI,
                MONGO_DB,
                MONGO_COLECCION
            )
            return jsonify(usuarios)
        except Exception as e:
            print("ERROR listando usuarios:", repr(e))
            return jsonify({"error": "Error al listar usuarios"}), 500

    if request.method == 'POST':
        try:
            data = request.get_json(force=True)
            mongo.crear_usuario(
                MONGO_URI,
                MONGO_DB,
                MONGO_COLECCION,
                data
            )
            return jsonify({"ok": True}), 201
        except ValueError as ve:
            return jsonify({"error": str(ve)}), 400
        except Exception as e:
            print("ERROR creando usuario:", repr(e))
            return jsonify({"error": "Error al crear usuario"}), 500
@app.route('/api/usuarios/<usuario>', methods=['PUT', 'DELETE'])
@login_required
def api_usuario_detalle(usuario):
    """
    API para actualizar o eliminar un usuario concreto.
    """
    permisos = session.get('permisos', {})
    if not permisos.get('admin_usuarios', True):
        return jsonify({"error": "No autorizado"}), 403

    if request.method == 'PUT':
        try:
            data = request.get_json(force=True)
            mongo.actualizar_usuario(
                MONGO_URI,
                MONGO_DB,
                MONGO_COLECCION,
                usuario_original=usuario,
                data=data
            )
            return jsonify({"ok": True})
        except ValueError as ve:
            return jsonify({"error": str(ve)}), 400
        except Exception as e:
            print("ERROR actualizando usuario:", repr(e))
            return jsonify({"error": "Error al actualizar usuario"}), 500

    if request.method == 'DELETE':
        try:
            mongo.eliminar_usuario(
                MONGO_URI,
                MONGO_DB,
                MONGO_COLECCION,
                usuario=usuario
            )
            return jsonify({"ok": True})
        except ValueError as ve:
            return jsonify({"error": str(ve)}), 400
        except Exception as e:
            print("ERROR eliminando usuario:", repr(e))
            return jsonify({"error": "Error al eliminar usuario"}), 500

@app.route('/gestor_elastic')
@login_required
def gestor_elastic():
    """
    Página de gestión de ElasticSearch (requiere permiso admin_elastic).
    """
    permisos = session.get('permisos', {})

    if not permisos.get('admin_elastic'):
        flash('No tienes permisos para gestionar ElasticSearch.', 'danger')
        return redirect(url_for('admin'))

    # Opcional: podríamos precargar índices aquí, pero yo lo haría por AJAX
    return render_template(
        'admin_elastic.html',
        version=VERSION_APP,
        creador=CREATOR_APP,
        usuario=session.get('usuario'),
        permisos=permisos
    )


@app.route('/api/elastic/indices')
def api_elastic_indices():
    try:
        if not session.get("logged_in"):
            return jsonify({"error": "No autorizado"}), 401

        permisos = session.get("permisos", {})
        if not permisos.get("admin_elastic"):
            return jsonify({"error": "No tiene permisos para gestionar ElasticSearch"}), 403

        indices = elastic.listar_indices()
        return jsonify({"indices": indices})

    except Exception as e:
        # Log completo en consola
        print("Error al listar índices de Elastic:", repr(e))
        # y mandamos el detalle al front TEMPORALMENTE
        return jsonify({"error": f"Error en listar_indices: {str(e)}"}), 500


@app.route('/api/elastic/ejecutar', methods=['POST'])
@login_required
def api_elastic_ejecutar():
    permisos = session.get('permisos', {})
    if not permisos.get('admin_elastic'):
        return jsonify({"error": "No autorizado (PERMISOS_APP"}), 403

    data = request.get_json(force=True, silent=True) or {}
    modo = data.get("modo")  # "query" o "dml"

    try:
        if modo == "query":
            index_name = data.get("index", "lenguaje_controlado")
            body = data.get("body", {})
            resp = elastic.ejecutar_query(index_name, body)
        elif modo == "dml":
            comando = data.get("comando", {})
            resp = elastic.ejecutar_dml(comando)
        else:
            return jsonify({"error": "Modo inválido"}), 400

        return jsonify({"resultado": resp})
    except Exception as e:
        print("Error al ejecutar comando Elastic:", e)
        return jsonify({"error": "Error al ejecutar en ElasticSearch"}), 500


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

