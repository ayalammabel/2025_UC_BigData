# mongo.py
from pymongo import MongoClient
from typing import Optional, Dict


def get_collection(uri: str, db_name: str, collection_name: str):
    client = MongoClient(uri)
    db = client[db_name]
    return db[collection_name]


def validar_usuario(
    usuario: str,
    password: str,
    uri: str,
    db_name: str,
    collection_name: str
) -> Optional[Dict]:
    """
    Valida usuario y contraseña en MongoDB.
    Retorna un dict con datos del usuario si es válido,
    o None si no existe / contraseña incorrecta.
    """
    coleccion = get_collection(uri, db_name, collection_name)

    user = coleccion.find_one({"usuario": usuario})

    if not user:
        return None

    # Para algo simple: plaintext (como el profe suele mostrar en clase)
    # Si usas hash, acá habría que usar bcrypt.check_password_hash
    if user.get("password") != password:
        return None

    # Normalizar info que se devuelve a app.py
    return {
        "usuario": user.get("usuario"),
        "rol": user.get("rol", "Usuario"),
        "permisos": user.get("permisos", {})
    }
