from pymongo import MongoClient
from typing import Optional, Dict

def validar_usuario(
    usuario: str,
    password: str,
    uri: str,
    db_name: str,
    collection_name: str
) -> Optional[Dict]:
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        # probar conexión
        client.admin.command("ping")

        db = client[db_name]
        coleccion = db[collection_name]

        user = coleccion.find_one({"usuario": usuario})
        if not user:
            return None

        if user.get("password") != password:
            return None

        return {
            "usuario": user.get("usuario"),
            "rol": user.get("rol", "Usuario"),
            "permisos": user.get("permisos", {})
        }
    except Exception as e:
        print(">>> ERROR EN validar_usuario():", repr(e))
        raise

def _get_client(uri: str) -> MongoClient:
    return MongoClient(uri, serverSelectionTimeoutMS=5000)

def listar_usuarios(uri: str, db_name: str, collection_name: str):
    """
    Retorna una lista de usuarios con campos básicos para la tabla:
    usuario, rol y fechaCreacion (si existe).
    """
    client = _get_client(uri)
    db = client[db_name]
    coleccion = db[collection_name]

    usuarios = []
    for doc in coleccion.find({}, {"_id": 0, "usuario": 1, "rol": 1, "fecha_creacion": 1}):
        usuarios.append({
            "usuario": doc.get("usuario"),
            "rol": doc.get("rol", "Usuario"),
        })

    return usuarios

def listar_usuarios_tabla(uri: str, db_name: str, collection_name: str):
    """
    Lista de usuarios con los campos necesarios para la tabla de administración.
    """
    client = _get_client(uri)
    db = client[db_name]
    coleccion = db[collection_name]

    usuarios = []
    for doc in coleccion.find(
        {},
        {
            "_id": 0,
            "usuario": 1,
            "password": 1,
            "permisos": 1
        }
    ):
        permisos = doc.get("permisos", {})
        usuarios.append({
            "usuario": doc.get("usuario"),
            "password": doc.get("password"),
            "login": bool(permisos.get("login", True)),
            "admin_usuarios": bool(permisos.get("admin_usuarios", False)),
            "admin_elastic": bool(permisos.get("admin_elastic", False)),
            "admin_data_elastic": bool(permisos.get("admin_data_elastic", False)),
        })

    return usuarios


def crear_usuario(uri: str, db_name: str, collection_name: str, data: dict):
    """
    Crea un nuevo usuario en la colección.
    OJO: para el proyecto real deberíamos encriptar password,
    aquí se deja plano solo por fines académicos.
    """
    client = _get_client(uri)
    db = client[db_name]
    coleccion = db[collection_name]

    usuario = data.get("usuario")
    if not usuario:
        raise ValueError("El campo 'usuario' es obligatorio")

    # Verificar si ya existe
    ya_existe = coleccion.find_one({"usuario": usuario})
    if ya_existe:
        raise ValueError("El usuario ya existe")

    doc = {
        "usuario": usuario,
        "password": data.get("password", ""),
        "rol": data.get("rol", "Usuario"),
        "permisos": {
            "login": bool(data.get("login", True)),
            "admin_usuarios": bool(data.get("admin_usuarios", False)),
            "admin_elastic": bool(data.get("admin_elastic", False)),
            "admin_data_elastic": bool(data.get("admin_data_elastic", False)),
        }
    }

    coleccion.insert_one(doc)
    return True

def actualizar_usuario(uri: str, db_name: str, collection_name: str,
                       usuario_original: str, data: dict):
    """
    Actualiza un usuario identificado por 'usuario_original'.
    Permite cambiar nombre, password y permisos.
    """
    client = _get_client(uri)
    db = client[db_name]
    coleccion = db[collection_name]

    if not usuario_original:
        raise ValueError("Usuario original no especificado")

    nuevo_usuario = data.get("usuario", usuario_original)

    # Si cambia el nombre, validar que no exista otro igual
    if nuevo_usuario != usuario_original:
        ya_existe = coleccion.find_one({"usuario": nuevo_usuario})
        if ya_existe:
            raise ValueError("Ya existe otro usuario con ese nombre")

    update_doc = {
        "usuario": nuevo_usuario,
        "password": data.get("password", ""),
        "rol": data.get("rol", "Usuario"),
        "permisos": {
            "login": bool(data.get("login", True)),
            "admin_usuarios": bool(data.get("admin_usuarios", False)),
            "admin_elastic": bool(data.get("admin_elastic", False)),
            "admin_data_elastic": bool(data.get("admin_data_elastic", False)),
        }
    }

    result = coleccion.update_one(
        {"usuario": usuario_original},
        {"$set": update_doc}
    )
    if result.matched_count == 0:
        raise ValueError("El usuario no existe")

    return True


def eliminar_usuario(uri: str, db_name: str, collection_name: str,
                     usuario: str):
    """
    Elimina un usuario por su nombre.
    """
    client = _get_client(uri)
    db = client[db_name]
    coleccion = db[collection_name]

    result = coleccion.delete_one({"usuario": usuario})
    if result.deleted_count == 0:
        raise ValueError("El usuario no existe")
    return True



