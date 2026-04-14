"""
API que recebe arquivos OFX e cola os recebimentos (creditos) numa planilha Google.
"""

import io
import re
import csv

import httpx
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Bot RPaRL - OFX to Google Sheets")

# ✅ URL CORRETA (sem o /a/macros/giacomoni.com.br/)
APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbzHON9qlcXdTYIQW3hLw0qNBTNw4PjHh6XJqQ4nONsIPDwTNw_rjwZA3VO-5NQktEmAKg/exec"

# Configuração CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def extrair_recebimentos(conteudo: str) -> list[dict]:
    banco_match = re.search(r'<BANKID>(\S+)', conteudo)
    banco = banco_match.group(1) if banco_match else ''

    acctid_match = re.search(r'<ACCTID>(\S+)', conteudo)
    acctid_raw = acctid_match.group(1).strip() if acctid_match else ''
    if '-' in acctid_raw:
        conta, dv_conta = acctid_raw.split('-', 1)
    else:
        conta, dv_conta = acctid_raw, ''

    branch_match = re.search(r'<BRANCHID>(\S+)', conteudo)
    branch_raw = branch_match.group(1).strip() if branch_match else ''
    if '-' in branch_raw:
        agencia, dv_agencia = branch_raw.split('-', 1)
    else:
        agencia, dv_agencia = branch_raw, ''

    lancamentos = []
    blocos = re.findall(r'<STMTTRN>(.*?)</STMTTRN>', conteudo, re.DOTALL)

    for bloco in blocos:
        trntype = re.search(r'<TRNTYPE>(\S+)', bloco)
        tipo = trntype.group(1).strip() if trntype else ''

        if tipo == 'DEBIT':
            continue

        dtposted = re.search(r'<DTPOSTED>(\d{8})', bloco)
        trnamt = re.search(r'<TRNAMT>([-\d.]+)', bloco)
        checknum = re.search(r'<CHECKNUM>(\S+)', bloco)
        memo = re.search(r'<MEMO>(.+)', bloco)

        data_raw = dtposted.group(1) if dtposted else ''
        data_fmt = f"{data_raw[:4]}-{data_raw[4:6]}-{data_raw[6:8]}" if len(data_raw) == 8 else data_raw

        lancamentos.append({
            'nRecibo': '',
            'nmPessoa': '',
            'cpfPessoa': '',
            'banco': banco,
            'conta': conta,
            'dv_conta': dv_conta,
            'tipo': tipo,
            'data': data_fmt,
            'valor': trnamt.group(1).strip() if trnamt else '',
            'checknum': checknum.group(1).strip() if checknum else '',
            'memo': memo.group(1).strip() if memo else '',
            'agencia': agencia,
            'dv_agencia': dv_agencia,
        })

    return lancamentos


@app.post("/converter")
async def converter(arquivos: list[UploadFile] = File(...)):
    todos_lancamentos = []

    for arq in arquivos:
        raw = await arq.read()
        conteudo = raw.decode('cp1252', errors='replace')
        todos_lancamentos.extend(extrair_recebimentos(conteudo))

    todos_lancamentos.sort(key=lambda x: x['data'])

    colunas = ['nmPessoa', 'cpfPessoa', 'nRecibo', 'banco', 'conta', 'dv_conta',
               'tipo', 'data', 'valor', 'checknum', 'memo', 'agencia', 'dv_agencia']

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=colunas)
    writer.writeheader()
    writer.writerows(todos_lancamentos)

    csv_string = buf.getvalue()

    # ✅ Envia corretamente com data={}
    async with httpx.AsyncClient(follow_redirects=True, timeout=60) as client:
        resp = await client.post(
            APPS_SCRIPT_URL,
            data={
                "csv": csv_string,
                "sheetName": "Preencher Dados"
            }
        )

    if resp.status_code != 200:
        raise HTTPException(
            status_code=502, 
            detail=f"Erro ao enviar para Google Sheets: {resp.text}"
        )

    result = resp.json()
    
    return JSONResponse({
        "status": "success",
        "sheets_url": result["url"],
        "total_lancamentos": result.get("rows", 0) - 1
    })