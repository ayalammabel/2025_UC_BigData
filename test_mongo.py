from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, OperationFailure

MONGO_URI = "mongodb+srv://mayala:mayala123@mayala.y4cqo9f.mongodb.net/?retryWrites=true&w=majority&appName=mayala"
MONGO_DB = "proyecto_bigData"
MONGO_COLECCION = "usuario_roles"

print("🔎 Probando conexión a MongoDB Atlas...")
print("URI:", MONGO_URI)

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    # Probar ping al servidor
    client.admin.command("ping")
    print("✅ Conexión correcta (ping OK)")

    db = client[MONGO_DB]
    col = db[MONGO_COLECCION]

    print("Documentos en usuario_roles:")
    for doc in col.find():
        print(doc)

except ServerSelectionTimeoutError as e:
    print("❌ Error de red / acceso (ServerSelectionTimeoutError):")
    print(repr(e))

except OperationFailure as e:
    print("❌ Error de autenticación u operación (OperationFailure):")
    print(repr(e))

except Exception as e:
    print("❌ Otro tipo de error:")
    print(repr(e))
