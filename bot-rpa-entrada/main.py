"""
Bot RPA Entrada - Extrai RPAs do PDF e cola na planilha Google via Apps Script.

Recebe PDF de Extrato Mensal RPA do Domínio, extrai os dados e envia
para a aba "Entrada" da planilha via Google Apps Script webapp.

Formato da Entrada (colunas A-H, coluna D preservada para fórmulas):
  A: empresa       (ex: "0000205")
  B: código        (10 dígitos, zero-padded)
  C: competência   (ex: "202602")
  D: (vazio)       (fórmula PROCX na planilha)
  E: nº RPA        (6 dígitos, zero-padded)
  F: data pgto     (YYYYMMDD)
  G: valor líquido (inteiro, só dígitos)
  H: nome          (uppercase, sem acentos)
"""

import io
import os
import re
import csv
import unicodedata
from datetime import date, timedelta

import httpx
import pdfplumber
from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Bot RPA Entrada - PDF to Google Sheets",
    description="Extrai RPAs do PDF e cola na aba Entrada da planilha via Apps Script",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbwO9LKIVVdLNKtWO60I5QEuEViZ3GlnXr1xc0gutTO9U5t0qWJkKg0sCMXosSzoRcvR1g/exec"


# ------------------------------------
# FUNÇÕES AUXILIARES
# ------------------------------------

def remover_acentos(texto: str) -> str:
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    texto = texto.replace("ç", "c").replace("Ç", "C")
    return texto


def normalizar_nome(texto: str) -> str:
    texto = texto.upper().strip()
    texto = remover_acentos(texto)
    texto = re.sub(r"[^A-Z\s]", "", texto)
    return re.sub(r"\s+", " ", texto).strip()


def quinto_dia_util(competencia: str) -> str:
    ano = int(competencia[:4])
    mes = int(competencia[4:])
    mes += 1
    if mes == 13:
        mes = 1
        ano += 1
    dia = date(ano, mes, 1)
    dias_uteis = 0
    while dias_uteis < 5:
        if dia.weekday() < 5:
            dias_uteis += 1
        if dias_uteis < 5:
            dia += timedelta(days=1)
    return dia.strftime("%Y%m%d")


# ------------------------------------
# EXTRAÇÃO DO PDF
# ------------------------------------

def extrair_rpas_do_pdf(pdf_bytes: bytes) -> list[dict]:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        text = "\n".join(
            page.extract_text() for page in pdf.pages if page.extract_text()
        )

    blocks = re.split(r"(?=Contr:\s*\d+)", text)
    rpas = []

    for block in blocks:
        if "Proventos:" not in block or "Contr:" not in block:
            continue

        # Código e nome do contribuinte
        header = re.search(r"Contr:\s*(\d+)\s*(.+?)Situa", block)
        if not header:
            header = re.search(r"Contr:\s*(\d+)\s*(.+?)\s*CPF:", block)

        codigo = header.group(1).zfill(10) if header else "0000000000"
        nome_raw = header.group(2).strip() if header else ""
        nome_raw = re.sub(r"\s*Trabalhando.*", "", nome_raw).strip()
        nome_raw = re.sub(r"[^A-Za-zÀ-ÿ\s]+$", "", nome_raw).strip()
        nome = normalizar_nome(nome_raw) if nome_raw else ""

        # Valor líquido
        liquido_match = re.search(r"L.quido:\s*([\d.,]+)", block)
        valor_str = (
            re.sub(r"[^\d]", "", liquido_match.group(1)) if liquido_match else "0"
        )

        rpas.append({
            "codigo": codigo,
            "nome": nome,
            "valor_liquido": valor_str,
        })

    return rpas


def montar_linhas_entrada(
    rpas: list[dict],
    empresa: str,
    competencia: str,
    data_pagamento: str,
) -> list[dict]:
    """Monta as linhas no formato da aba Entrada (A-H, D vazio)."""
    linhas = []
    for i, rpa in enumerate(rpas, start=1):
        linhas.append({
            "empresa": empresa,
            "codigo": rpa["codigo"],
            "competencia": competencia,
            "funcao": "",  # coluna D — fórmula PROCX na planilha
            "rpa_num": str(i).zfill(6),
            "data_pagamento": data_pagamento,
            "valor_liquido": int(rpa["valor_liquido"]) if rpa["valor_liquido"] else 0,
            "nome": rpa["nome"],
        })
    return linhas


# ============================================================
# ENDPOINTS
# ============================================================

@app.get("/")
async def root():
    return {
        "message": "Bot RPA Entrada - PDF to Google Sheets",
        "version": "1.0.0",
        "endpoints": {
            "POST /converter": "Extrai RPAs do PDF e cola na aba Entrada via Apps Script",
            "POST /extrair/json": "Extrai RPAs do PDF e retorna JSON (sem enviar)",
            "GET /health": "Health check",
        },
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.post("/converter")
async def converter_pdf_e_enviar(
    file: UploadFile = File(...),
    empresa: str = Form(default="0000205"),
    competencia: str = Form(default=None),
    data_pagamento: str = Form(default=None),
):
    """
    Extrai RPAs do PDF e envia para a aba Entrada da planilha via Apps Script.

    - empresa: Código da empresa (padrão: 0000205)
    - competencia: YYYYMM (padrão: mês atual)
    - data_pagamento: YYYYMMDD (padrão: 5º dia útil do mês seguinte)
    """
    if not competencia:
        hoje = date.today()
        competencia = hoje.strftime("%Y%m")
    if not data_pagamento:
        data_pagamento = quinto_dia_util(competencia)

    try:
        if not file.filename.endswith(".pdf"):
            raise HTTPException(status_code=400, detail="O arquivo deve ser um PDF")

        pdf_bytes = await file.read()
        rpas = extrair_rpas_do_pdf(pdf_bytes)

        if not rpas:
            raise HTTPException(
                status_code=422, detail="Nenhum RPA encontrado no PDF"
            )

        linhas = montar_linhas_entrada(rpas, empresa, competencia, data_pagamento)

        # Monta CSV para enviar ao Apps Script
        colunas = [
            "empresa", "codigo", "competencia", "funcao",
            "rpa_num", "data_pagamento", "valor_liquido", "nome",
        ]
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=colunas)
        writer.writeheader()
        writer.writerows(linhas)
        csv_string = buf.getvalue()

        # Envia para Google Sheets via Apps Script
        async with httpx.AsyncClient(follow_redirects=True, timeout=60) as client:
            resp = await client.post(
                APPS_SCRIPT_URL,
                data={
                    "csv": csv_string,
                    "sheetName": "Entrada",
                },
            )

        if resp.status_code != 200:
            raise HTTPException(
                status_code=502,
                detail=f"Erro ao enviar para Google Sheets: {resp.text}",
            )

        result = resp.json()

        return JSONResponse({
            "status": "success",
            "sheets_url": result.get("url", ""),
            "total_rpas": len(rpas),
            "total_linhas_enviadas": result.get("rows", 0) - 1,
            "empresa": empresa,
            "competencia": competencia,
            "data_pagamento": data_pagamento,
        })

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@app.post("/extrair/json")
async def extrair_json(
    file: UploadFile = File(...),
    empresa: str = Form(default="0000205"),
    competencia: str = Form(default=None),
    data_pagamento: str = Form(default=None),
):
    """Extrai RPAs do PDF e retorna JSON para preview (sem enviar para Sheets)."""
    if not competencia:
        hoje = date.today()
        competencia = hoje.strftime("%Y%m")
    if not data_pagamento:
        data_pagamento = quinto_dia_util(competencia)

    try:
        if not file.filename.endswith(".pdf"):
            raise HTTPException(status_code=400, detail="O arquivo deve ser um PDF")

        pdf_bytes = await file.read()
        rpas = extrair_rpas_do_pdf(pdf_bytes)

        if not rpas:
            raise HTTPException(
                status_code=422, detail="Nenhum RPA encontrado no PDF"
            )

        linhas = montar_linhas_entrada(rpas, empresa, competencia, data_pagamento)

        return JSONResponse({
            "arquivo": file.filename,
            "empresa": empresa,
            "competencia": competencia,
            "data_pagamento": data_pagamento,
            "total_rpas": len(rpas),
            "registros": linhas,
        })

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
