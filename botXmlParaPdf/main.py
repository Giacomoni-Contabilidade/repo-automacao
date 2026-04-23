"""
API que recebe um ou mais arquivos XML e converte cada arquivo em PDF.

Resposta:
- 1 XML: retorna o PDF diretamente
- 2+ XMLs: retorna um arquivo ZIP com todos os PDFs
"""

from __future__ import annotations

import io
import os
import re
import zipfile
from datetime import datetime
from xml.dom import minidom

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import Response, StreamingResponse
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas


app = FastAPI(title="Bot XML para PDF")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=app.title,
        version="0.1.0",
        routes=app.routes,
        description=app.description,
    )

    body_schema = openapi_schema.get("components", {}).get("schemas", {}).get(
        "Body_converter_converter_post", {}
    )
    arquivos_schema = body_schema.get("properties", {}).get("arquivos", {})
    item_schema = arquivos_schema.get("items", {})

    if item_schema.get("type") == "string":
        item_schema.pop("contentMediaType", None)
        item_schema["format"] = "binary"

    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi


def _safe_filename(filename: str) -> str:
    base = os.path.basename(filename or "arquivo.xml")
    # Evita apenas caracteres de controle/cabeçalho HTTP e mantém o nome original.
    base = re.sub(r"[\r\n\t\"]+", "_", base).strip()
    return base or "arquivo.xml"


def _pretty_xml(xml_bytes: bytes) -> str:
    try:
        parsed = minidom.parseString(xml_bytes)
        return parsed.toprettyxml(indent="  ")
    except Exception as exc:
        raise ValueError(f"XML invalido: {exc}") from exc


def _xml_text_to_pdf_bytes(filename: str, xml_text: str) -> bytes:
    buf = io.BytesIO()
    pdf = canvas.Canvas(buf, pagesize=A4)
    _, height = A4

    left = 40
    top = height - 40
    line_height = 12
    max_chars = 95
    y = top

    timestamp = datetime.now().strftime("%d/%m/%Y, %H:%M")

    lines: list[str] = [
        timestamp,
        "",
        filename,
        "",
        "This XML file does not appear to have any style information associated with it. The document tree is shown below.",
    ]

    for preface_line in lines:
        if y < 40:
            pdf.showPage()
            y = top
        pdf.setFont("Helvetica", 9)
        pdf.drawString(left, y, preface_line)
        y -= 14

    y -= 4
    pdf.setFont("Courier", 8)

    body_lines: list[str] = []
    for raw_line in xml_text.splitlines():
        text_line = raw_line.replace("\t", "    ")
        if len(text_line) <= max_chars:
            body_lines.append(text_line)
            continue

        while text_line:
            body_lines.append(text_line[:max_chars])
            text_line = text_line[max_chars:]

    if not body_lines:
        body_lines = ["(arquivo XML vazio)"]

    for line in body_lines:
        if y < 40:
            pdf.showPage()
            y = top
            pdf.setFont("Courier", 8)
        pdf.drawString(left, y, line)
        y -= line_height

    pdf.save()
    return buf.getvalue()


@app.get("/")
async def root():
    return {
        "message": "Bot XML para PDF",
        "endpoints": {
            "POST /converter": "Recebe 1..N arquivos XML e retorna PDF (ou ZIP de PDFs)",
            "GET /health": "Health check",
        },
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.post("/converter")
async def converter(arquivos: list[UploadFile] = File(...)):
    if not arquivos:
        raise HTTPException(status_code=400, detail="Envie pelo menos um arquivo XML.")

    pdfs: list[tuple[str, bytes]] = []

    for arquivo in arquivos:
        if not arquivo.filename:
            raise HTTPException(status_code=400, detail="Arquivo sem nome enviado.")

        if not arquivo.filename.lower().endswith(".xml"):
            raise HTTPException(
                status_code=400,
                detail=f"O arquivo '{arquivo.filename}' nao e XML.",
            )

        raw = await arquivo.read()
        if not raw:
            raise HTTPException(
                status_code=400,
                detail=f"O arquivo '{arquivo.filename}' esta vazio.",
            )

        try:
            pretty = _pretty_xml(raw)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"{arquivo.filename}: {exc}") from exc

        safe_original_name = _safe_filename(arquivo.filename)
        pdf_name = f"{safe_original_name}.pdf"
        pdf_content = _xml_text_to_pdf_bytes(safe_original_name, pretty)
        pdfs.append((pdf_name, pdf_content))

    if len(pdfs) == 1:
        pdf_name, pdf_content = pdfs[0]
        return Response(
            content=pdf_content,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{pdf_name}"'},
        )

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for pdf_name, pdf_content in pdfs:
            zf.writestr(pdf_name, pdf_content)

    zip_buf.seek(0)
    return StreamingResponse(
        zip_buf,
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="xml-para-pdf.zip"'},
    )
