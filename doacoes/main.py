"""
API que recebe arquivos OFX de doações financeiras (SPCA Cadastro)
e cola o CSV resultante numa planilha Google.
"""

import io
import re
import csv

import httpx
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Bot Doações - OFX to Google Sheets")

APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbzriEVGMb23KfoqDpYNX8vqUZTFJzRXF2FXiIk2sVCqiTUhmxAz5X1INHwsc1BZEAT3xw/exec"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# CONFIGURAÇÕES FIXAS - Classificação 376
# =============================================================================

CNPJ_PRESTADOR = "06954942000195"
TIPO = "PP"
NR_PARTIDO = "50"
ESFERA_PARTIDARIA = "ESTADUAL"
FONTE_RECURSO = "FP"
CLASSIFICACAO_DOACAO = "376"
TP_FUNDO_PARTIDARIO = "2"

# =============================================================================
# DADOS BANCÁRIOS POR UF (destino)
# =============================================================================

DADOS_UF = {
    "AC": {"cnpj": "07808778000170", "agencia": "2358", "dv_agencia": "2", "conta": "52074",    "dv_conta": "8"},
    "AM": {"cnpj": "09205985000166", "agencia": "3286", "dv_agencia": "7", "conta": "46601",    "dv_conta": "8"},
    "AP": {"cnpj": "08083782000181", "agencia": "4544", "dv_agencia": "6", "conta": "10087",    "dv_conta": "0"},
    "BA": {"cnpj": "08109726000179", "agencia": "4279", "dv_agencia": "x", "conta": "37574",    "dv_conta": "8"},
    "CE": {"cnpj": "08769121000104", "agencia": "3653", "dv_agencia": "6", "conta": "27644",    "dv_conta": "8"},
    "DF": {"cnpj": "08878505000111", "agencia": "1230", "dv_agencia": "0", "conta": "505050",   "dv_conta": "2"},
    "GO": {"cnpj": "08077359000179", "agencia": "3607", "dv_agencia": "2", "conta": "53558",    "dv_conta": "3"},
    "MG": {"cnpj": "27178691000174", "agencia": "3857", "dv_agencia": "1", "conta": "33057",    "dv_conta": "4"},
    "MS": {"cnpj": "08852944000107", "agencia": "2916", "dv_agencia": "5", "conta": "505050",   "dv_conta": "2"},
    "MT": {"cnpj": "08103641000183", "agencia": "46",   "dv_agencia": "9", "conta": "25102",    "dv_conta": "x"},
    "PB": {"cnpj": "08767001000178", "agencia": "1234", "dv_agencia": "3", "conta": "138260",   "dv_conta": "8"},
    "PE": {"cnpj": "10737784000199", "agencia": "5740", "dv_agencia": "1", "conta": "138496",   "dv_conta": "1"},
    "PR": {"cnpj": "11393227000160", "agencia": "3273", "dv_agencia": "5", "conta": "106937",   "dv_conta": "3"},
    "RJ": {"cnpj": "08049726000120", "agencia": "392",  "dv_agencia": "1", "conta": "37067",    "dv_conta": "3"},
    "RN": {"cnpj": "08039227000152", "agencia": "1668", "dv_agencia": "3", "conta": "90393",    "dv_conta": "0"},
    "RO": {"cnpj": "08071859000100", "agencia": "3231", "dv_agencia": "x", "conta": "28730",    "dv_conta": "x"},
    "RR": {"cnpj": "08077215000112", "agencia": "250",  "dv_agencia": "x", "conta": "77977",    "dv_conta": "x"},
    "RS": {"cnpj": "11664141000190", "agencia": "3240", "dv_agencia": "x", "conta": "50051",    "dv_conta": "8"},
    "SC": {"cnpj": "08133541000108", "agencia": "5201", "dv_agencia": "9", "conta": "1647680",  "dv_conta": "8"},
    "TO": {"cnpj": "09245968000163", "agencia": "5921", "dv_agencia": "8", "conta": "12719",    "dv_conta": "1"},
}

DOCUMENTOS_UF = {
    "552358000052074": "AC",
    "553286000046601": "AM",
    "554544000010087": "AP",
    "554279000037574": "BA",
    "553653000027644": "CE",
    "551230000505650": "DF",
    "553607000008558": "GO",
    "553857000033057": "MG",
    "552916000505060": "MS",
    "550046000026102": "MT",
    "551234000138260": "PB",
    "555740000138496": "PE",
    "553273000106937": "PR",
    "550392000037067": "RJ",
    "551668000090383": "RN",
    "553231000028730": "RO",
    "550250000077977": "RR",
    "553240000050060": "RS",
    "555201001847680": "SC",
    "555921000012719": "TO",
}


# =============================================================================
# PARSER OFX
# =============================================================================

def extrair_valor_tag(bloco, tag):
    padrao_fechado = re.compile(rf"<{tag}>(.*?)</{tag}>", re.DOTALL | re.IGNORECASE)
    m = padrao_fechado.search(bloco)
    if m:
        return m.group(1).strip()
    padrao_aberto = re.compile(rf"<{tag}>\s*(.+?)(?:\s*(?:<|\Z))", re.IGNORECASE)
    m = padrao_aberto.search(bloco)
    if m:
        return m.group(1).strip()
    return ""


def detectar_forma_e_operacao(memo):
    memo_upper = memo.upper()
    if "PIX" in memo_upper:
        return "TEL", "PIX"
    elif "TED" in memo_upper:
        return "TED", "TED"
    elif "TRANSFERÊNCIA" in memo_upper or "TRANSFERENCIA" in memo_upper:
        return "TEL", "TEL"
    else:
        return "", ""


def formatar_data(dtposted):
    dt = dtposted.strip()[:8]
    if len(dt) == 8:
        return f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}"
    return dt


def formatar_valor(trnamt):
    try:
        valor = abs(float(trnamt.strip()))
        return f"{valor:.2f}"
    except (ValueError, AttributeError):
        return trnamt


def processar_ofx(conteudo: str) -> list[dict]:
    banco = extrair_valor_tag(conteudo, "BANKID")
    conta_id = extrair_valor_tag(conteudo, "ACCTID")

    conta_origem = conta_id
    dv_conta_origem = ""
    if "-" in conta_id:
        partes = conta_id.split("-")
        conta_origem = partes[0].strip()
        dv_conta_origem = partes[1].strip() if len(partes) > 1 else ""

    registros = []
    blocos = re.findall(r"<STMTTRN>(.*?)</STMTTRN>", conteudo, re.DOTALL | re.IGNORECASE)

    for bloco in blocos:
        checknum = extrair_valor_tag(bloco, "CHECKNUM").strip()
        if checknum not in DOCUMENTOS_UF:
            continue

        uf = DOCUMENTOS_UF[checknum]
        dados_dest = DADOS_UF.get(uf, {})
        dtposted = extrair_valor_tag(bloco, "DTPOSTED")
        trnamt = extrair_valor_tag(bloco, "TRNAMT")
        memo = extrair_valor_tag(bloco, "MEMO")
        fitid = extrair_valor_tag(bloco, "FITID")
        forma_doacao, operacao_financeira = detectar_forma_e_operacao(memo)

        registros.append({
            "nrDocumento": "",
            "nrReciboDoacao": "",
            "nrCnpjPrestador": CNPJ_PRESTADOR,
            "anoExercicio": dtposted.strip()[:4],
            "tipo": TIPO,
            "nrCnpj": dados_dest.get("cnpj", ""),
            "esferaPartidaria": ESFERA_PARTIDARIA,
            "partido": NR_PARTIDO,
            "uf": uf,
            "dtDoacao": formatar_data(dtposted),
            "fonteRecurso": FONTE_RECURSO,
            "classificacaoDoacao": CLASSIFICACAO_DOACAO,
            "valorDoacao": formatar_valor(trnamt),
            "formaDoacao": forma_doacao,
            "operacaoFinanceira": operacao_financeira,
            "nrExtratoBancario": checknum,
            "nrBancoOrigem": banco,
            "agenciaOrigem": "1230",
            "dvAgenciaOrigem": "0",
            "contaCorrenteOrigem": conta_origem,
            "dvContaCorrenteOrigem": dv_conta_origem,
            "tpFundoPartidario": TP_FUNDO_PARTIDARIO,
            "nrLancamento": "",
            "nrBancoDestino": "001",
            "agenciaDestino": dados_dest.get("agencia", ""),
            "dvAgenciaDestino": dados_dest.get("dv_agencia", ""),
            "contaCorrenteDestino": dados_dest.get("conta", ""),
            "dvContaCorrenteDestino": dados_dest.get("dv_conta", ""),
            "memo_ofx": memo,
            "fitid_ofx": fitid,
        })

    return registros


# =============================================================================
# ENDPOINT
# =============================================================================

@app.post("/converter")
async def converter(arquivos: list[UploadFile] = File(...)):
    todos_registros = []

    for arq in arquivos:
        raw = await arq.read()
        conteudo = raw.decode("cp1252", errors="replace")
        todos_registros.extend(processar_ofx(conteudo))

    if not todos_registros:
        raise HTTPException(status_code=400, detail="Nenhuma doação encontrada nos arquivos OFX.")

    todos_registros.sort(key=lambda r: (r["dtDoacao"], r["uf"]))

    for i, reg in enumerate(todos_registros, start=1):
        reg["nrLancamento"] = str(i)

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

    return JSONResponse({
        "status": "success",
        "sheets_url": result["url"],
        "total_lancamentos": result.get("rows", 0) - 1,
    })
