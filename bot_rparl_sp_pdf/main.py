"""
Bot RPaRL PDF - Extrai creditos de extrato PDF e cola na planilha Google.

Mantem o mesmo layout de planilha do bot_rparl original:
  nmPessoa, cpfPessoa, nRecibo, banco, conta, dv_conta,
  tipo, data, valor, checknum, memo, agencia, dv_agencia
"""

import csv
import io
import os
import re
import unicodedata
from datetime import datetime

import httpx
import pdfplumber
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI(
    title="Bot RPaRL PDF - PDF to Google Sheets",
    description="Extrai creditos de extrato PDF e cola na planilha do RPaRL",
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
    "AKfycbzHON9qlcXdTYIQW3hLw0qNBTNw4PjHh6XJqQ4nONsIPDwTNw_rjwZA3VO-5NQktEmAKg/exec"
)
SHEET_NAME = "Preencher Dados"

# Dados de conta definidos no fluxo anterior.
BANCO = "001"
CONTA = "505052"
DV_CONTA = "9"
AGENCIA = "300"
DV_AGENCIA = "0"
TIPO = ""

COLUNAS_CSV = [
    "nmPessoa",
    "cpfPessoa",
    "nRecibo",
    "banco",
    "conta",
    "dv_conta",
    "tipo",
    "data",
    "valor",
    "checknum",
    "memo",
    "agencia",
    "dv_agencia",
]


def remover_acentos(texto: str) -> str:
    texto = unicodedata.normalize("NFD", texto)
    return "".join(c for c in texto if unicodedata.category(c) != "Mn")


def normalizar_chave(texto: str) -> str:
    texto = remover_acentos(texto or "").upper().strip()
    return re.sub(r"\s+", " ", texto)


def normalizar_espacos(texto: str) -> str:
    return re.sub(r"\s+", " ", (texto or "")).strip()


def linha_eh_data(texto: str) -> bool:
    return bool(re.fullmatch(r"\d{2}/\d{2}/\d{4}", texto.strip()))


def formatar_data(data_br: str) -> str:
    return datetime.strptime(data_br, "%d/%m/%Y").strftime("%Y-%m-%d")


def formatar_valor(valor_br: str) -> str:
    valor = valor_br.replace(".", "").replace(",", ".").strip()
    return f"{float(valor):.2f}"


def extrair_texto_pdf(pdf_bytes: bytes) -> str:
    paginas = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            texto = ""
            try:
                texto = page.extract_text(layout=True) or ""
            except TypeError:
                texto = page.extract_text() or ""

            if not texto:
                texto = page.extract_text() or ""

            if texto:
                paginas.append(texto)

    return "\n".join(paginas)


def detectar_evento_credito(texto: str) -> bool:
    chave = normalizar_chave(texto)
    return (
        "TED-CREDITO EM CONTA" in chave
        or "TRANSFERENCIA RECEBIDA" in chave
        or "PIX" in chave
    )


def extrair_info_linha_valor_layout_legado(line: str) -> dict | None:
    valor_match = re.search(r"(?P<valor>[\d.]+,\d{2})\s*\((?P<sinal>[+-])\)\s*$", line)
    if not valor_match:
        return None

    antes_valor = line[:valor_match.start()].rstrip()
    linha_match = re.match(
        r"\s*(?P<lote>\d+)"
        r"(?:\s+(?P<documento>[\d.]+))?"
        r"(?:\s+(?P<historico>.*?))?\s*$",
        antes_valor,
    )
    if not linha_match:
        return None

    return {
        "layout": "legado",
        "data": "",
        "descricao": "",
        "lote": (linha_match.group("lote") or "").strip(),
        "documento": (linha_match.group("documento") or "").strip(),
        "historico": (linha_match.group("historico") or "").strip(),
        "valor": valor_match.group("valor"),
        "sinal": valor_match.group("sinal"),
    }


def extrair_info_linha_valor_layout_bb(line: str) -> dict | None:
    valor_match = re.match(
        r"\s*(?P<data>\d{2}/\d{2}/\d{4})"
        r"\s+(?P<ag_origem>\d+)"
        r"\s+(?P<lote>\d+)"
        r"\s+(?P<resto>.+?)"
        r"\s+(?P<valor>[\d.]+,\d{2})"
        r"\s+(?P<sinal>[CD])"
        r"(?:\s+[\d.]+,\d{2}\s+[CD])?\s*$",
        line,
    )
    if not valor_match:
        return None

    resto = normalizar_espacos(valor_match.group("resto"))
    documento = ""
    descricao = resto

    doc_match = re.match(r"^(?P<descricao>.+?)\s+(?P<documento>[\d.]+)$", resto)
    if doc_match:
        descricao = normalizar_espacos(doc_match.group("descricao"))
        documento = doc_match.group("documento")

    return {
        "layout": "bb_inline",
        "data": valor_match.group("data"),
        "descricao": descricao,
        "lote": valor_match.group("lote"),
        "documento": documento,
        "historico": "",
        "valor": valor_match.group("valor"),
        "sinal": "+" if valor_match.group("sinal") == "C" else "-",
    }


def extrair_info_linha_valor(line: str) -> dict | None:
    return (
        extrair_info_linha_valor_layout_legado(line)
        or extrair_info_linha_valor_layout_bb(line)
    )


def texto_eh_resumo(texto: str) -> bool:
    chave = normalizar_chave(texto)
    chave_compacta = chave.replace(" ", "")
    return chave.startswith("SALDO") or chave_compacta.startswith("SALDO")


def coletar_complementos(lines: list[str], start_index: int) -> list[str]:
    extras = []
    for idx in range(start_index + 1, len(lines)):
        stripped = lines[idx].strip()
        if not stripped:
            break
        if stripped == "00/00/0000" or linha_eh_data(stripped):
            break
        if extrair_info_linha_valor(lines[idx]):
            break

        chave = normalizar_chave(stripped)
        if chave in {
            "SALDO",
            "SALDO DO DIA",
            "SALDO ANTERIOR",
            "TOTAL APLICACOES FINANCEIRAS",
        }:
            break
        if detectar_evento_credito(stripped):
            break

        extras.append(stripped)

    return extras


def extrair_nome_cpf(historico: str) -> tuple[str, str]:
    historico = normalizar_espacos(historico)

    ted_match = re.match(
        r"^\d{3}\s+\d{3,5}\s+(?P<cpf>\d{11})\s+(?P<nome>.+)$",
        historico,
    )
    if ted_match:
        return normalizar_espacos(ted_match.group("nome")), ""

    transf_match = re.match(
        r"^\d{2}/\d{2}\s+\d{2}:\d{2}\s+(?P<nome>.+?)\s+(?P<identificador>\d+)$",
        historico,
    )
    if transf_match:
        return normalizar_espacos(transf_match.group("nome")), ""

    return "", ""


def extrair_creditos_do_texto(texto: str) -> list[dict]:
    lines = [line.rstrip() for line in texto.splitlines()]
    registros = []

    for idx, line in enumerate(lines):
        info_linha = extrair_info_linha_valor(line)
        if not info_linha or info_linha["sinal"] != "+":
            continue
        texto_principal = info_linha.get("descricao") or info_linha["historico"]
        if texto_eh_resumo(texto_principal):
            continue

        data_lancamento = info_linha.get("data", "")
        descricao_lancamento = info_linha.get("descricao", "")

        for prev_idx in range(idx - 1, max(-1, idx - 8), -1):
            if data_lancamento and descricao_lancamento:
                break

            stripped = lines[prev_idx].strip()
            if not stripped:
                continue
            if stripped == "00/00/0000":
                break
            if data_lancamento and extrair_info_linha_valor(lines[prev_idx]):
                break

            if not data_lancamento and linha_eh_data(stripped) and stripped != "00/00/0000":
                data_lancamento = stripped
                continue

            if not descricao_lancamento and detectar_evento_credito(stripped):
                descricao_lancamento = stripped
                continue

        if not data_lancamento or not descricao_lancamento:
            continue

        complementos = coletar_complementos(lines, idx)
        detalhes = [parte for parte in [info_linha["historico"], *complementos] if parte]
        memo_partes = [descricao_lancamento, *detalhes]
        memo = " | ".join(parte for parte in memo_partes if parte)
        base_nome_cpf = info_linha["historico"] or (detalhes[0] if detalhes else "")
        nome, cpf = extrair_nome_cpf(base_nome_cpf)

        registros.append({
            "nmPessoa": nome,
            "cpfPessoa": cpf,
            "nRecibo": "",
            "banco": BANCO,
            "conta": CONTA,
            "dv_conta": DV_CONTA,
            "tipo": TIPO,
            "data": formatar_data(data_lancamento),
            "valor": formatar_valor(info_linha["valor"]),
            "checknum": info_linha["documento"],
            "memo": memo,
            "agencia": AGENCIA,
            "dv_agencia": DV_AGENCIA,
        })

    return registros


def processar_pdf(pdf_bytes: bytes) -> list[dict]:
    texto = extrair_texto_pdf(pdf_bytes)
    if not texto.strip():
        return []
    return extrair_creditos_do_texto(texto)


async def processar_arquivos(arquivos: list[UploadFile]) -> list[dict]:
    todos_lancamentos = []

    for arquivo in arquivos:
        nome = arquivo.filename or "arquivo.pdf"
        if not nome.lower().endswith(".pdf"):
            raise HTTPException(status_code=400, detail=f"O arquivo {nome} nao e um PDF.")

        pdf_bytes = await arquivo.read()
        todos_lancamentos.extend(processar_pdf(pdf_bytes))

    if not todos_lancamentos:
        raise HTTPException(
            status_code=422,
            detail="Nenhum credito recebido foi encontrado nos PDFs enviados.",
        )

    todos_lancamentos.sort(key=lambda reg: (reg["data"], reg["checknum"]))
    return todos_lancamentos


async def processar_arquivo_unico(arquivo: UploadFile) -> list[dict]:
    return await processar_arquivos([arquivo])


@app.get("/")
async def root():
    return {
        "message": "Bot RPaRL PDF - PDF to Google Sheets",
        "version": "1.0.0",
        "endpoints": {
            "POST /converter": "Extrai creditos do PDF e cola na planilha do RPaRL",
            "POST /converter/simples": "Mesmo fluxo, mas com upload de um unico arquivo no Swagger",
            "POST /extrair/json": "Extrai os creditos do PDF e retorna preview em JSON",
            "POST /extrair/json/simples": "Mesmo preview, mas com upload de um unico arquivo no Swagger",
            "GET /health": "Health check",
        },
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.post("/extrair/json")
async def extrair_json(arquivos: list[UploadFile] = File(...)):
    try:
        lancamentos = await processar_arquivos(arquivos)
        return JSONResponse({
            "status": "success",
            "total_lancamentos": len(lancamentos),
            "registros": lancamentos,
        })
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(exc)}")


@app.post("/extrair/json/simples")
async def extrair_json_simples(arquivo: UploadFile = File(...)):
    try:
        lancamentos = await processar_arquivo_unico(arquivo)
        return JSONResponse({
            "status": "success",
            "total_lancamentos": len(lancamentos),
            "registros": lancamentos,
        })
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(exc)}")


@app.post("/converter")
async def converter(arquivos: list[UploadFile] = File(...)):
    try:
        lancamentos = await processar_arquivos(arquivos)

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=COLUNAS_CSV)
        writer.writeheader()
        writer.writerows(lancamentos)
        csv_string = buf.getvalue()

        async with httpx.AsyncClient(follow_redirects=True, timeout=60) as client:
            resp = await client.post(
                APPS_SCRIPT_URL,
                data={
                    "csv": csv_string,
                    "sheetName": SHEET_NAME,
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
            "total_lancamentos": result.get("rows", 0) - 1,
        })
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(exc)}")


@app.post("/converter/simples")
async def converter_simples(arquivo: UploadFile = File(...)):
    try:
        lancamentos = await processar_arquivo_unico(arquivo)

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=COLUNAS_CSV)
        writer.writeheader()
        writer.writerows(lancamentos)
        csv_string = buf.getvalue()

        async with httpx.AsyncClient(follow_redirects=True, timeout=60) as client:
            resp = await client.post(
                APPS_SCRIPT_URL,
                data={
                    "csv": csv_string,
                    "sheetName": SHEET_NAME,
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
            "total_lancamentos": result.get("rows", 0) - 1,
        })
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(exc)}")


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
