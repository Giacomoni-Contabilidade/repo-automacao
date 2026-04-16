"""
Bot RPA Dados - extrai dados do extrato mensal de RPA do Domínio
e cola na aba "Dados" de uma planilha Google via Apps Script.
"""

import csv
import io
import os
import re

import httpx
import pdfplumber
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI(
    title="Bot RPA Dados - PDF to Google Sheets",
    description=(
        "Extrai os campos do extrato mensal de RPA "
        "e envia para a aba Dados da planilha via Apps Script"
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
    "AKfycbzHcx7gx0jq916uSryf2DSI58Lj0ec6f4mjRB47N0X7l5YB30N8xweklPmZYWpbMKkL6A/exec"
)
SHEET_NAME = "Dados"
START_CELL = "A3"

COLUNAS_CSV = [
    "nrCnpjPrestador",
    "anoExercicio",
    "nrCpf",
    "nmPessoa",
    "descricao",
    "dataEmissaoContratacao",
    "vrTotalDocumento",
    "cdDescricaoGasto",
    "vrGasto",
    "descricaoResumida",
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


def somente_cnpj(texto: str) -> str:
    return re.sub(r"\D", "", texto or "")


def extrair_texto_pdf(pdf_bytes: bytes) -> str:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        return "\n".join(
            page.extract_text() for page in pdf.pages if page.extract_text()
        )


def extrair_metadados(texto: str) -> dict:
    cnpj_match = re.search(r"CNPJ:\s*([\d./-]+)", texto)
    competencia_match = re.search(r"Compet[êe]ncia:\s*(\d{2})/(\d{4})", texto)
    emissao_match = re.search(r"Emiss[ãa]o:\s*(\d{2}/\d{2}/\d{4})", texto)

    nr_cnpj_prestador = somente_cnpj(cnpj_match.group(1)) if cnpj_match else ""
    ano_exercicio = competencia_match.group(2) if competencia_match else ""
    if not ano_exercicio and emissao_match:
        ano_exercicio = emissao_match.group(1)[-4:]

    return {
        "nrCnpjPrestador": nr_cnpj_prestador,
        "anoExercicio": ano_exercicio,
        "dataEmissaoContratacao": emissao_match.group(1) if emissao_match else "",
    }


def extrair_campo(bloco: str, padrao: str, grupo: int = 1, padrao_vazio: str = "0") -> str:
    match = re.search(padrao, bloco, re.IGNORECASE)
    if not match:
        return padrao_vazio
    return match.group(grupo)


def extrair_nome(bloco: str) -> str:
    match = re.search(
        r"Contr:\s*\d+\s*(?P<nome>.+?)(?:\s+Situa\w*:|\s+CPF:|$)",
        bloco,
        re.IGNORECASE,
    )
    if not match:
        return ""
    nome = limpar_espacos(match.group("nome"))
    nome = re.sub(r"\s*Trabalhando.*", "", nome, flags=re.IGNORECASE)
    nome = re.sub(r"[^A-Za-zÀ-ÿ\s]+$", "", nome).strip()
    return nome


def extrair_cpf(bloco: str) -> str:
    match = re.search(r"CPF:\s*([\d.-]+)", bloco, re.IGNORECASE)
    return somente_cnpj(match.group(1)) if match else ""


def extrair_descricao_principal(bloco: str) -> tuple[str, str, str, str]:
    linhas = [linha.strip() for linha in bloco.splitlines() if linha.strip()]

    for linha in linhas:
        match = re.match(
            r"^(?P<cd>\d{3})(?P<desc>.*?)(?P<vr>[\d.]+,\d{2})\s+[\d.]+,\d{2}P\b",
            linha,
        )
        if not match:
            continue

        descricao = limpar_espacos(match.group("desc"))
        descricao = re.sub(r"\s*-\s*$", "", descricao).strip()
        descricao = descricao.rstrip(" .")

        descricao_resumida = descricao
        if "-" in descricao:
            descricao_resumida = limpar_espacos(descricao.split("-", 1)[-1])
        descricao_resumida = descricao_resumida.rstrip(" .")

        return (
            match.group("cd"),
            descricao,
            match.group("vr"),
            descricao_resumida,
        )

    return "", "", "", ""


def extrair_registros_do_texto(texto: str, competencia_padrao: str | None = None) -> list[dict]:
    metadados = extrair_metadados(texto)
    blocos = re.split(r"(?=Contr:\s*\d+)", texto)
    registros = []

    for bloco in blocos:
        if "Contr:" not in bloco or "Proventos:" not in bloco:
            continue

        nome = extrair_nome(bloco)
        nr_cpf = extrair_cpf(bloco)
        cd_descricao_gasto, descricao, vr_gasto, descricao_resumida = extrair_descricao_principal(bloco)
        vr_total_documento = extrair_campo(bloco, r"L[íi]quido:\s*([\d.,]+)")
        if not vr_total_documento:
            vr_total_documento = extrair_campo(bloco, r"L.quido:\s*([\d.,]+)")


        if "MULHERES" in descricao_resumida:
            cd_descricao_gasto = "220"
        else:
            cd_descricao_gasto = "221"


        registros.append(
            {
                "nrCnpjPrestador": metadados["nrCnpjPrestador"],
                "anoExercicio": metadados["anoExercicio"],
                "nrCpf": nr_cpf,
                "nmPessoa": nome,
                "descricao": descricao,
                "dataEmissaoContratacao": metadados["dataEmissaoContratacao"],
                "vrTotalDocumento": vr_total_documento,
                "cdDescricaoGasto": cd_descricao_gasto,
                "vrGasto": vr_gasto,
                "descricaoResumida": f"PAGAMENTO CONFORME RPA {descricao_resumida}",
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
        "message": "Bot RPA Entrada Dados - PDF to Google Sheets",
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
                "ano_exercicio": registros[0]["anoExercicio"],
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
                "ano_exercicio": registros[0]["anoExercicio"],
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
