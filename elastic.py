import os
import json
import logging
from typing import List, Optional, Dict

import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ElasticSearch:
    """
    Cliente sencillo para conectarse a Elastic Cloud usando API Key.
    """

    def __init__(self, base_url: str | None = None, api_key: str | None = None):
        # Si no se pasan por parámetro, los toma de variables de entorno
        self.base_url = base_url or os.getenv("ELASTIC_BASE_URL", "")
        self.api_key = api_key or os.getenv("ELASTIC_API_KEY", "")

        if not self.base_url or not self.api_key:
            raise ValueError(
                "ELASTIC_BASE_URL y/o ELASTIC_API_KEY no están configurados "
                "ni por parámetro ni como variables de entorno."
            )

        # Normalizar base_url para que no termine en '/'
        self.base_url = self.base_url.rstrip("/")

        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"ApiKey {self.api_key}",
        }

    def _url(self, path: str) -> str:
        """
        Construye la URL completa a partir de la ruta interna.
        """
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}{path}"

    def ping(self) -> bool:
        """
        Verifica si Elastic está respondiendo.
        """
        try:
            resp = requests.get(self._url("/"), headers=self.headers, timeout=10)
            logger.info("Ping Elastic status: %s", resp.status_code)
            return resp.ok
        except Exception as e:
            logger.error("Error haciendo ping a Elastic: %s", e)
            return False

    def crear_indice(self, index_name: str, mappings: Optional[dict] = None) -> dict:
        """
        Crea un índice si no existe. Si ya existe, devuelve la respuesta tal cual.
        """
        url = self._url(f"/{index_name}")
        body = mappings or {}

        resp = requests.put(url, headers=self.headers, data=json.dumps(body))
        try:
            return resp.json()
        except Exception:
            return {"status_code": resp.status_code, "text": resp.text}

    def indexar_bulks(self, index_name: str, documentos: List[dict]) -> dict:
        """
        Indexa una lista de documentos usando la API _bulk de Elastic.

        Cada documento de la lista debe ser un dict con el contenido a indexar.
        """
        if not documentos:
            return {"error": "La lista de documentos está vacía"}

        bulk_lines = []
        for doc in documentos:
            action = {"index": {"_index": index_name}}
            bulk_lines.append(json.dumps(action))
            bulk_lines.append(json.dumps(doc))

        body = "\n".join(bulk_lines) + "\n"

        url = self._url("/_bulk")
        headers = self.headers.copy()
        headers["Content-Type"] = "application/x-ndjson"

        resp = requests.post(url, headers=headers, data=body)

        try:
            data = resp.json()
        except Exception:
            data = {"status_code": resp.status_code, "text": resp.text}

        if resp.status_code >= 400:
            logger.error("Error en bulk indexing: %s", data)

        return data

    def buscar_texto(
        self,
        index_name: str,
        query: str,
        campos: Optional[List[str]] = None,
        size: int = 10,
    ) -> dict:
        """
        Busca texto en uno o varios campos de un índice.
        Si no se pasan campos, busca en todos los campos con 'query_string'.
        """
        if campos:
            q = {
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": campos,
                    }
                },
                "size": size,
            }
        else:
            q = {
                "query": {
                    "query_string": {
                        "query": query,
                    }
                },
                "size": size,
            }

        url = self._url(f"/{index_name}/_search")
        resp = requests.get(url, headers=self.headers, data=json.dumps(q))

        try:
            return resp.json()
        except Exception:
            return {"status_code": resp.status_code, "text": resp.text}

    # ===================== MÉTODOS ADMIN ELASTIC ======================

    def listar_indices(self) -> List[Dict]:
        """
        Devuelve información del índice principal de lenguaje controlado.
        NO usa _cat/indices para evitar problemas de permisos de clúster.
        """
        try:
            index_name = getattr(self, "index_por_defecto", "lenguaje_controlado")

            # 1) Contar documentos con una búsqueda size=0
            url = self._url(f"/{index_name}/_search")
            body = {
                "size": 0,
                "query": {"match_all": {}}
            }
            resp = requests.get(url, headers=self.headers, data=json.dumps(body))

            try:
                data = resp.json()
            except Exception:
                logger.error("Error parseando JSON en listar_indices: %s", resp.text)
                raise

            docs_total = (
                data.get("hits", {})
                    .get("total", {})
                    .get("value", 0)
            )

            # No pedimos info de cluster para no chocar con permisos:
            status = "open"

            return [{
                "nombre": index_name,
                "docs": docs_total,
                "tamano": "N/A",
                "salud": "N/A",
                "status": status
            }]

        except Exception as e:
            logger.error("Error en listar_indices(): %s", e)
            raise

    def ejecutar_query(self, index_name: str, query_body: Dict) -> Dict:
        """
        Ejecuta un _search con el body que envíe el usuario.
        """
        try:
            url = self._url(f"/{index_name}/_search")
            resp = requests.get(url, headers=self.headers, data=json.dumps(query_body))

            try:
                return resp.json()
            except Exception:
                return {"status_code": resp.status_code, "text": resp.text}
        except Exception as e:
            logger.error("Error en ejecutar_query(): %s", e)
            raise

    def ejecutar_dml(self, comando: Dict) -> Dict:
        """
        Ejecuta operaciones sencillas de DML:
        - operacion: index | update | delete
        - index: nombre del índice
        - id: id del documento
        - documento: dict con el contenido (para index/update)
        """
        try:
            operacion = comando.get("operacion")
            index_name = comando.get("index")
            doc_id = comando.get("id")
            documento = comando.get("documento", {})

            if not index_name:
                raise ValueError("Falta 'index' en el comando DML")

            if operacion == "index":
                # PUT /{index}/_doc/{id}
                url = self._url(f"/{index_name}/_doc/{doc_id}")
                resp = requests.put(url, headers=self.headers, data=json.dumps(documento))

            elif operacion == "update":
                # POST /{index}/_update/{id}
                url = self._url(f"/{index_name}/_update/{doc_id}")
                body = {"doc": documento}
                resp = requests.post(url, headers=self.headers, data=json.dumps(body))

            elif operacion == "delete":
                # DELETE /{index}/_doc/{id}
                url = self._url(f"/{index_name}/_doc/{doc_id}")
                resp = requests.delete(url, headers=self.headers)

            else:
                raise ValueError(f"Operación DML no soportada: {operacion}")

            try:
                return resp.json()
            except Exception:
                return {"status_code": resp.status_code, "text": resp.text}

        except Exception as e:
            logger.error("Error en ejecutar_dml(): %s", e)
            raise




