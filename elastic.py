import os
import json
import logging
from typing import List, Optional

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


