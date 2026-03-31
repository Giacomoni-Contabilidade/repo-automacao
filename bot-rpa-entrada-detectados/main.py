"""
Bot RPA Entrada Detectados - extrai todos os campos detectados do PDF
e envia para a aba "Entrada" de outra planilha Google via Apps Script.

Campos enviados:
  A: codigo_empresa                 (7 digitos)
  B: codigo_contribuinte            (10 digitos)
  C: competencia                    (AAAAMM)
  D: coluna_d_em_branco            (em branco)
  E: numero_rpa                     (em branco)
  F: rendimento_bruto               (inteiro, so digitos)
  G: valor_iss                      (inteiro, so digitos)
  H: nome
  I: base_iss                       (inteiro, so digitos)
  J: valor_inss                     (inteiro, so digitos)
  K: data_pagamento                 (em branco)
  L: base_irrf                      (inteiro, so digitos)
  M: quantidade_dependentes_ir      (3 digitos)
  N: valor_ir                       (inteiro, so digitos)
"""

import csv
import io
import os
import re
from datetime import date

import httpx
import pdfplumber
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI(
    title="Bot RPA Entrada Detectados - PDF to Google Sheets",
    description=(
        "Extrai todos os campos detectados do extrato mensal de RPA "
        "e envia para a aba Entrada da planilha via Apps Script"
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

APPS_SCRIPT_URL = (
    "https://script.google.com/macros/s/"
    "AKfycbycChGyhycfdMgv39GOYelW_A8IW2yLuuuimfiHe-1ZyNIH4C0b6Cob7FUTahDmQbQ/exec"
)
SHEET_NAME = "Entrada"
START_CELL = "A3"

COLUNAS_CSV = [
    "codigo_empresa",
    "codigo_contribuinte",
    "competencia",
    "coluna_d_em_branco",
    "numero_rpa",
    "rendimento_bruto",
    "valor_iss",
    "nome",
    "base_iss",
    "valor_inss",
    "data_pagamento",
    "base_irrf",
    "quantidade_dependentes_ir",
    "valor_ir",
]


def somente_digitos(texto: str) -> str:
    return re.sub(r"\D", "", texto or "")


def moeda_para_inteiro(texto: str, padrao: str = "0") -> str:
    digitos = somente_digitos(texto)
    if not digitos:
        return padrao
    return str(int(digitos))


def limpar_espacos(texto: str) -> str:
    return re.sub(r"\s+", " ", (texto or "").strip())


def extrair_texto_pdf(pdf_bytes: bytes) -> str:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        return "\n".join(
            page.extract_text() for page in pdf.pages if page.extract_text()
        )


def extrair_metadados(texto: str) -> dict:
    empresa_match = re.search(r"Empresa:\s*(\d+)", texto)
    competencia_match = re.search(r"Compet[êe]ncia:\s*(\d{2})/(\d{4})", texto)

    codigo_empresa = empresa_match.group(1).zfill(7) if empresa_match else "0000000"

    competencia_pdf = None
    if competencia_match:
        competencia_pdf = (
            f"{competencia_match.group(2)}{competencia_match.group(1)}"
        )

    return {
        "codigo_empresa": codigo_empresa,
        "competencia_pdf": competencia_pdf,
    }


def extrair_campo(bloco: str, padrao: str, grupo: int = 1, padrao_vazio: str = "0") -> str:
    match = re.search(padrao, bloco, re.IGNORECASE)
    if not match:
        return padrao_vazio
    return match.group(grupo)


def extrair_nome(bloco: str) -> str:
    match = re.search(r"Contr:\s*\d+\s*(.+?)\s*(?:Situa|CPF:)", bloco, re.IGNORECASE)
    if not match:
        return ""
    nome = limpar_espacos(match.group(1))
    nome = re.sub(r"\s*Trabalhando.*", "", nome, flags=re.IGNORECASE)
    nome = re.sub(r"[^A-Za-zÀ-ÿ\s]+$", "", nome).strip()
    return nome


def extrair_registros_do_texto(texto: str, competencia_padrao: str | None = None) -> list[dict]:
    metadados = extrair_metadados(texto)
    competencia = competencia_padrao or metadados["competencia_pdf"]
    if not competencia:
        hoje = date.today()
        competencia = hoje.strftime("%Y%m")

    blocos = re.split(r"(?=Contr:\s*\d+)", texto)
    registros = []

    for bloco in blocos:
        if "Contr:" not in bloco or "Proventos:" not in bloco:
            continue

        codigo_contribuinte = extrair_campo(
            bloco,
            r"Contr:\s*(\d+)",
        ).zfill(10)

        nome = extrair_nome(bloco)

        rendimento_bruto = moeda_para_inteiro(
            extrair_campo(bloco, r"Proventos:\s*([\d.,]+)")
        )
        valor_iss = moeda_para_inteiro(
            extrair_campo(bloco, r"Valor ISS:\s*([\d.,]+)")
        )
        base_iss = moeda_para_inteiro(
            extrair_campo(bloco, r"Base ISS:\s*([\d.,]+)")
        )
        valor_inss = moeda_para_inteiro(
            extrair_campo(bloco, r"INSS AUTONOMO\s+[\d.,]+\s+([\d.,]+)D")
        )
        base_irrf = moeda_para_inteiro(
            extrair_campo(bloco, r"Base IRRF:\s*([\d.,]+)")
        )
        quantidade_dependentes_ir = extrair_campo(
            bloco,
            r"ND:\s*(\d+)",
        ).zfill(3)
        valor_ir = moeda_para_inteiro(
            extrair_campo(bloco, r"IRRF AUTONOMO\s+[\d.,]+\s+([\d.,]+)D")
        )

        registros.append(
            {
                "codigo_empresa": metadados["codigo_empresa"],
                "codigo_contribuinte": codigo_contribuinte,
                "competencia": competencia,
                "coluna_d_em_branco": "",
                "numero_rpa": "",
                "rendimento_bruto": rendimento_bruto,
                "valor_iss": valor_iss,
                "nome": nome,
                "base_iss": base_iss,
                "valor_inss": valor_inss,
                "data_pagamento": "",
                "base_irrf": base_irrf,
                "quantidade_dependentes_ir": quantidade_dependentes_ir,
                "valor_ir": valor_ir,
            }
        )

    return registros


def montar_csv(registros: list[dict]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=COLUNAS_CSV)
    writer.writeheader()
    writer.writerows(registros)
    return buf.getvalue()


@app.get("/")
async def root():
    return {
        "message": "Bot RPA Entrada Detectados - PDF to Google Sheets",
        "version": "1.0.0",
        "sheet_name": SHEET_NAME,
        "start_cell": START_CELL,
        "endpoints": {
            "POST /converter": "Extrai campos detectados do PDF e envia para a planilha",
            "POST /extrair/json": "Extrai campos detectados do PDF e retorna JSON",
            "GET /health": "Health check",
        },
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.post("/converter")
async def converter_pdf_e_enviar(
    file: UploadFile = File(...),
    competencia: str = Form(default=None),
):
    if not competencia:
        competencia = None

    try:
        if not file.filename.endswith(".pdf"):
            raise HTTPException(status_code=400, detail="O arquivo deve ser um PDF")

        pdf_bytes = await file.read()
        texto = extrair_texto_pdf(pdf_bytes)
        registros = extrair_registros_do_texto(texto, competencia)

        if not registros:
            raise HTTPException(status_code=422, detail="Nenhum registro encontrado no PDF")

        csv_string = montar_csv(registros)

        async with httpx.AsyncClient(follow_redirects=True, timeout=60) as client:
            resp = await client.post(
                APPS_SCRIPT_URL,
                data={
                    "csv": csv_string,
                    "sheetName": SHEET_NAME,
                    "startCell": START_CELL,
                    "clearFrom": START_CELL,
                    "clearBeforeWrite": "true",
                },
            )

        if resp.status_code != 200:
            raise HTTPException(
                status_code=502,
                detail=f"Erro ao enviar para Google Sheets: {resp.text}",
            )

        try:
            result = resp.json()
        except Exception:
            result = {"raw_response": resp.text}

        return JSONResponse(
            {
                "status": "success",
                "sheet_name": SHEET_NAME,
                "start_cell": START_CELL,
                "total_registros": len(registros),
                "competencia": registros[0]["competencia"],
                "sheets_response": result,
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@app.post("/extrair/json")
async def extrair_json(
    file: UploadFile = File(...),
    competencia: str = Form(default=None),
):
    if not competencia:
        competencia = None

    try:
        if not file.filename.endswith(".pdf"):
            raise HTTPException(status_code=400, detail="O arquivo deve ser um PDF")

        pdf_bytes = await file.read()
        texto = extrair_texto_pdf(pdf_bytes)
        registros = extrair_registros_do_texto(texto, competencia)

        if not registros:
            raise HTTPException(status_code=422, detail="Nenhum registro encontrado no PDF")

        return JSONResponse(
            {
                "arquivo": file.filename,
                "sheet_name": SHEET_NAME,
                "start_cell": START_CELL,
                "total_registros": len(registros),
                "competencia": registros[0]["competencia"],
                "colunas": COLUNAS_CSV,
                "registros": registros,
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
