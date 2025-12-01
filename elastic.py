from elasticsearch import Elasticsearch, helpers
from typing import Dict, List, Optional, Any
import json


class ElasticSearch:
    def __init__(self, cloud_url: str, api_key: str):
        """
        Inicializa conexión a ElasticSearch Cloud

        Args:
            cloud_url: URL del cluster de Elastic Cloud
            api_key: API Key para autenticación
        """
        self.client = Elasticsearch(
            cloud_url,
            api_key=api_key,
            verify_certs=True,
        )

    def test_connection(self) -> bool:
        """Prueba la conexión a ElasticSearch"""
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

        Args:
            nombre_index: Nombre del índice
            mappings: Definición de campos (opcional)
            settings: Configuración del índice (opcional)
        """
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
        try:
            self.client.indices.delete(index=nombre_index)
            return True
        except Exception as e:
            print(f"Error al eliminar índice: {e}")
            return False

    def listar_indices(self) -> List[Dict]:
        """Lista todos los índices"""
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

        Args:
            index: Nombre del índice
            documento: Documento a indexar
            doc_id: ID del documento (opcional)
        """
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

        Args:
            index: Nombre del índice
            documentos: Lista de documentos a indexar
        """
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
    # BÚSQUEDAS
    # ------------------------------------------------------------------

    def buscar(self, index: str, query: Dict, size: int = 10) -> Dict:
        """
        Realiza una búsqueda en ElasticSearch

        Args:
            index: Nombre del índice
            query: Query de búsqueda (DSL de Elastic)
            size: Número de resultados
        """
        try:
            response = self.client.search(index=index, body=query, size=size)
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

        Args:
            index: Nombre del índice
            texto: Texto a buscar
            campos: Lista de campos donde buscar (si es None, busca en todos)
            size: Número de resultados
        """
        try:
            if campos:
                # Búsqueda en campos específicos
                query = {
                    "query": {
                        "multi_match": {
                            "query": texto,
                            "fields": campos,
                        }
                    }
                }
            else:
                # Búsqueda general en todos los campos indexados
                query = {
                    "query": {
                        "query_string": {
                            "query": texto,
                        }
                    }
                }

            response = self.client.search(index=index, body=query, size=size)
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
    # CRUD DOCUMENTOS
    # ------------------------------------------------------------------

    def obtener_documento(self, index: str, doc_id: str) -> Optional[Dict]:
        """Obtiene un documento por su ID"""
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
        try:
            self.client.update(index=index, id=doc_id, doc=datos)
            return True
        except Exception as e:
            print(f"Error al actualizar documento: {e}")
            return False

    def eliminar_documento(self, index: str, doc_id: str) -> bool:
        """Elimina un documento"""
        try:
            self.client.delete(index=index, id=doc_id)
            return True
        except Exception as e:
            print(f"Error al eliminar documento: {e}")
            return False

    # ------------------------------------------------------------------
    # CONEXIÓN
    # ------------------------------------------------------------------

    def close(self):
        """Cierra la conexión"""
        self.client.close()
