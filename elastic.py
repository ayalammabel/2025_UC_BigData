from elasticsearch import Elasticsearch, helpers
from typing import Dict, List, Optional, Any


class ElasticSearch:
    """
    Cliente sencillo para conectarse a Elastic Cloud usando API Key.
    """
   
    def __init__(self, base_url: str, api_key: str):
        """
        Inicializa conexión a ElasticSearch usando una URL normal.

        base_url: por ejemplo
        https://fe68aba5b3194046b86205bc65ddcf71.us-central1.gcp.cloud.es.io:443
        """
        if not base_url or not api_key:
            print("⚠️ ElasticSearch: falta ELASTIC_URL o ELASTIC_API_KEY.")
            self.client = None
            return

        self.client = Elasticsearch(
            base_url,
            api_key=api_key,
            verify_certs=True,
        )

    def test_connection(self) -> bool:
        """Prueba la conexión a ElasticSearch"""
        if self.client is None:
            print("⚠️ ElasticSearch: cliente no inicializado.")
            return False
        try:
            info = self.client.info()
            print(f"✅ Conectado a Elastic: {info['version']['number']}")
            return True
        except Exception as e:
            print(f"❌ Error al conectar con Elastic: {e}")
            return False

    # ------------------------------------------------------------------
    # ÍNDICES
    # ------------------------------------------------------------------

    def crear_index(
        self,
        nombre_index: str,
        mappings: Dict = None,
        settings: Dict = None,
    ) -> bool:
        """
        Crea un nuevo índice
        """
        if not self.client:
            print("❌ Cliente Elastic no inicializado.")
            return False

        try:
            body: Dict[str, Any] = {}
            if mappings:
                body["mappings"] = mappings
            if settings:
                body["settings"] = settings

            self.client.indices.create(index=nombre_index, body=body)
            return True
        except Exception as e:
            print(f"Error al crear índice: {e}")
            return False

    def eliminar_index(self, nombre_index: str) -> bool:
        """Elimina un índice"""
        if not self.client:
            print("❌ Cliente Elastic no inicializado.")
            return False

        try:
            self.client.indices.delete(index=nombre_index)
            return True
        except Exception as e:
            print(f"Error al eliminar índice: {e}")
            return False

    def listar_indices(self) -> List[Dict]:
        """Lista todos los índices"""
        if not self.client:
            print("❌ Cliente Elastic no inicializado.")
            return []

        try:
            indices = self.client.cat.indices(format="json")
            return indices
        except Exception as e:
            print(f"Error al listar índices: {e}")
            return []

    # ------------------------------------------------------------------
    # INDEXACIÓN
    # ------------------------------------------------------------------

    def indexar_documento(
        self,
        index: str,
        documento: Dict,
        doc_id: str = None,
    ) -> bool:
        """
        Indexa un documento en ElasticSearch
        """
        if not self.client:
            print("❌ Cliente Elastic no inicializado.")
            return False

        try:
            if doc_id:
                self.client.index(index=index, id=doc_id, document=documento)
            else:
                self.client.index(index=index, document=documento)
            return True
        except Exception as e:
            print(f"Error al indexar documento: {e}")
            return False

    def indexar_bulks(self, index: str, documentos: List[Dict]) -> bool:
        """
        Indexa múltiples documentos usando la API bulk.
        """
        if not self.client:
            print("❌ Cliente Elastic no inicializado.")
            return False

        try:
            actions = [
                {
                    "_index": index,
                    # Si el documento ya trae un id, lo usamos. Si no, que Elastic lo genere.
                    "_id": doc.get("id") or doc.get("_id"),
                    "_source": doc,
                }
                for doc in documentos
            ]

            helpers.bulk(self.client, actions)
            return True
        except Exception as e:
            print(f"Error al indexar documentos (bulk): {e}")
            return False

    # ------------------------------------------------------------------
    # BÚSQUEDAS GENERALES
    # ------------------------------------------------------------------

    def buscar(self, index: str, query: Dict, size: int = 10) -> Dict:
        """
        Realiza una búsqueda en ElasticSearch (query en formato DSL).
        """
        if not self.client:
            return {"success": False, "error": "Cliente Elastic no inicializado."}

        try:
            # En cliente 9.x se usa el parámetro 'query='
            q = query.get("query", query)
            response = self.client.search(index=index, query=q, size=size)
            return {
                "success": True,
                "total": response["hits"]["total"]["value"],
                "resultados": response["hits"]["hits"],
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
            }

    def buscar_texto(
        self,
        index: str,
        texto: str,
        campos: List[str] = None,
        size: int = 10,
    ) -> Dict:
        """
        Búsqueda simple de texto en campos específicos.
        """
        if not self.client:
            return {"success": False, "error": "Cliente Elastic no inicializado."}

        try:
            if campos:
                # Búsqueda en campos específicos
                query = {
                    "multi_match": {
                        "query": texto,
                        "fields": campos,
                    }
                }
            else:
                # Búsqueda general en todos los campos indexados
                query = {
                    "query_string": {
                        "query": texto,
                    }
                }

            response = self.client.search(index=index, query=query, size=size)
            return {
                "success": True,
                "total": response["hits"]["total"]["value"],
                "resultados": response["hits"]["hits"],
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
            }

    # ------------------------------------------------------------------
    # BÚSQUEDA ESPECÍFICA PARA EL BUSCADOR WEB
    # ------------------------------------------------------------------

    def search_terms(self, text: str, size: int = 20) -> List[Dict]:
        """
        Búsqueda pensada para el buscador web.
        Usa el índice por defecto (self.index_name) y 
        devuelve una lista simplificada de resultados.
        """
        if not self.client:
            print("⚠️ search_terms llamado sin cliente Elastic.")
            return []

        if not text:
            return []

        query = {
            "multi_match": {
                "query": text,
                "fields": [
                    "term_child^3",
                    "term_parent^2",
                    "definition",
                    "pdf_text",
                ],
            }
        }

        try:
            resp = self.client.search(
                index=self.index_name,
                query=query,
                size=size,
            )
        except Exception as e:
            print("❌ Error al buscar en Elastic:", e)
            return []

        resultados: List[Dict] = []
        for hit in resp.get("hits", {}).get("hits", []):
            src = hit.get("_source", {})
            resultados.append(
                {
                    "score": hit.get("_score"),
                    "term_parent": src.get("term_parent"),
                    "term_child": src.get("term_child"),
                    "definition": src.get("definition"),
                    "source_url": src.get("source_url"),
                    "file_name": src.get("file_name"),
                }
            )

        return resultados

    # ------------------------------------------------------------------
    # CRUD DOCUMENTOS
    # ------------------------------------------------------------------

    def obtener_documento(self, index: str, doc_id: str) -> Optional[Dict]:
        """Obtiene un documento por su ID"""
        if not self.client:
            print("❌ Cliente Elastic no inicializado.")
            return None

        try:
            response = self.client.get(index=index, id=doc_id)
            return response["_source"]
        except Exception as e:
            print(f"Error al obtener documento: {e}")
            return None

    def actualizar_documento(
        self,
        index: str,
        doc_id: str,
        datos: Dict,
    ) -> bool:
        """Actualiza un documento existente"""
        if not self.client:
            print("❌ Cliente Elastic no inicializado.")
            return False

        try:
            self.client.update(index=index, id=doc_id, doc=datos)
            return True
        except Exception as e:
            print(f"Error al actualizar documento: {e}")
            return False

    def eliminar_documento(self, index: str, doc_id: str) -> bool:
        """Elimina un documento"""
        if not self.client:
            print("❌ Cliente Elastic no inicializado.")
            return False

        try:
            self.client.delete(index=index, id=doc_id)
            return True
        except Exception as e:
            print(f"Error al eliminar documento: {e}")
            return False

    # ------------------------------------------------------------------
    # CIERRE
    # ------------------------------------------------------------------

    def close(self):
        """Cierra la conexión"""
        if self.client:
            self.client.close()

