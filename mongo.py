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
