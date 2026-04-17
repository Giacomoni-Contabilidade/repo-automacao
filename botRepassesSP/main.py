"""
API que recebe arquivos OFX ou PDF de repasses para diretorios municipais de SP
e cola o CSV resultante numa planilha Google.
"""

import csv
import io

import httpx
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse

from repasses_parser import processar_bytes_arquivo


app = FastAPI(title="Bot Repasses SP - OFX/PDF to Google Sheets")

APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbzriEVGMb23KfoqDpYNX8vqUZTFJzRXF2FXiIk2sVCqiTUhmxAz5X1INHwsc1BZEAT3xw/exec"

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

    body_schema = openapi_schema.get("components", {}).get("schemas", {}).get("Body_converter_converter_post", {})
    arquivos_schema = body_schema.get("properties", {}).get("arquivos", {})
    item_schema = arquivos_schema.get("items", {})

    if item_schema.get("type") == "string":
        item_schema.pop("contentMediaType", None)
        item_schema["format"] = "binary"

    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi


@app.post("/converter")
async def converter(arquivos: list[UploadFile] = File(...)):
    todos_registros = []

    try:
        for arquivo in arquivos:
            raw = await arquivo.read()
            todos_registros.extend(processar_bytes_arquivo(arquivo.filename, raw))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if not todos_registros:
        raise HTTPException(status_code=400, detail="Nenhum repasse encontrado nos arquivos OFX/PDF.")

    todos_registros.sort(
        key=lambda registro: (
            registro["dtDoacao"],
            registro.get("municipio", ""),
            registro["nrExtratoBancario"],
        )
    )

    for indice, registro in enumerate(todos_registros, start=1):
        registro["nrLancamento"] = str(indice)

    colunas = list(todos_registros[0].keys())

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=colunas)
    writer.writeheader()
    writer.writerows(todos_registros)
    csv_string = buf.getvalue()

    async with httpx.AsyncClient(follow_redirects=True, timeout=60) as client:
        resp = await client.post(
            APPS_SCRIPT_URL,
            data={
                "csv": csv_string,
                "sheetName": "Preencher Dados",
            },
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Erro ao enviar para Google Sheets: {resp.text}")

    result = resp.json()
    return JSONResponse(
        {
            "status": "success",
            "sheets_url": result["url"],
            "total_lancamentos": result.get("rows", 0) - 1,
        }
    )
