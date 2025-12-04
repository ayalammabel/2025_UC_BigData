# webscraping_helper.py
import os
import tempfile
from typing import List

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from PyPDF2 import PdfReader


def descargar_pdfs_desde_url(
    url_inicial: str,
    tipos_archivos: str = "pdf",
    max_pdfs: int = 5,
    max_paginas_por_pdf: int = 5,
) -> List[dict]:
    """
    Desde una URL inicial:
      - Busca enlaces que apunten a PDFs (u otras extensiones indicadas).
      - Descarga hasta `max_pdfs` archivos.
      - Extrae texto (hasta `max_paginas_por_pdf` páginas) con PyPDF2.
      - Devuelve una lista de documentos listos para indexar en Elastic.

    Cada documento tiene campos:
      - source
      - url_pdf
      - titulo
      - contenido
    """
    docs: List[dict] = []

    try:
        resp = requests.get(url_inicial, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"[WEB] Error al acceder a la URL inicial: {e}")
        return docs

    soup = BeautifulSoup(resp.text, "lxml")

    # Normalizar tipos de archivo (pdf, docx, etc.)
    tipos = [t.strip().lower() for t in tipos_archivos.split(",") if t.strip()]
    if not tipos:
        tipos = ["pdf"]

    # Buscar enlaces
    links_encontrados = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        href_lower = href.lower()
        if any(href_lower.endswith("." + ext) for ext in tipos):
            full_url = urljoin(url_inicial, href)
            if full_url not in links_encontrados:
                links_encontrados.append(full_url)

    if not links_encontrados:
        print("[WEB] No se encontraron enlaces a archivos con las extensiones indicadas.")
        return docs

    # Limitar cantidad de PDFs a descargar
    links_encontrados = links_encontrados[:max_pdfs]

    # Carpeta temporal para los PDFs
    tmp_dir = tempfile.mkdtemp(prefix="webpdf_", dir="/tmp")

    try:
        for url_pdf in links_encontrados:
            try:
                print(f"[WEB] Descargando {url_pdf}")
                pdf_resp = requests.get(url_pdf, timeout=40)
                pdf_resp.raise_for_status()
            except Exception as e:
                print(f"[WEB] Error descargando PDF {url_pdf}: {e}")
                continue

            nombre_pdf = url_pdf.split("/")[-1] or "documento.pdf"
            ruta_pdf = os.path.join(tmp_dir, nombre_pdf)

            try:
                with open(ruta_pdf, "wb") as f:
                    f.write(pdf_resp.content)
            except Exception as e:
                print(f"[WEB] Error guardando PDF {nombre_pdf}: {e}")
                continue

            # Extraer texto con PyPDF2
            texto = ""
            try:
                with open(ruta_pdf, "rb") as f:
                    reader = PdfReader(f)
                    num_pages = min(len(reader.pages), max_paginas_por_pdf)
                    for i in range(num_pages):
                        try:
                            texto += reader.pages[i].extract_text() or ""
                            texto += "\n"
                        except Exception:
                            continue
            except Exception as e:
                print(f"[WEB] Error leyendo PDF {nombre_pdf}: {e}")
                continue

            texto = texto.strip()
            if not texto:
                print(f"[WEB] PDF sin texto legible: {nombre_pdf}")
                continue

            doc = {
                "source": "web_scraping",
                "url_pdf": url_pdf,
                "titulo": nombre_pdf,
                "contenido": texto,
            }
            docs.append(doc)

    finally:
        # Limpiar carpeta temporal
        try:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass

    print(f"[WEB] Se generaron {len(docs)} documentos desde {url_inicial}")
    return docs
