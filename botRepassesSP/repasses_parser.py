"""
Parser compartilhado para extratos OFX e PDF do bot de repasses municipais de SP.
"""

from __future__ import annotations

import io
import re
import subprocess
import unicodedata
from pathlib import Path
from shutil import which

try:
    import pdfplumber
except ImportError:  # pragma: no cover - dependencia opcional no ambiente
    pdfplumber = None


CNPJ_PRESTADOR = "08745772000164"
TIPO = "PP"
NR_PARTIDO = "50"
ESFERA_PARTIDARIA = "MUNICIPAL"
UF_DESTINO = "SP"
FONTE_RECURSO = "FP"
CLASSIFICACAO_DOACAO = "378"
TP_FUNDO_PARTIDARIO = "2"

AGENCIA_ORIGEM_OFX = "1230"
DV_AGENCIA_ORIGEM_OFX = "0"

DESTINOS_POR_EXTRATO = {
    "610179000085870": {
        "municipio": "Araçatuba",
        "cnpj": "16119795000191",
        "banco": "001",
        "agencia": "0179",
        "dv_agencia": "1",
        "conta": "85870",
        "dv_conta": "6",
    },
    "610079000071664": {
        "municipio": "Botucatu",
        "cnpj": "15795106000104",
        "banco": "001",
        "agencia": "0079",
        "dv_agencia": "5",
        "conta": "71664",
        "dv_conta": "2",
    },
    "613015000041107": {
        "municipio": "Bauru",
        "cnpj": "25499754000169",
        "banco": "001",
        "agencia": "3015",
        "dv_agencia": "5",
        "conta": "41107",
        "dv_conta": "8",
    },
    "610053000098515": {
        "municipio": "Franca",
        "cnpj": "16517409000110",
        "banco": "001",
        "agencia": "0053",
        "dv_agencia": "1",
        "conta": "98515",
        "dv_conta": "5",
    },
    "610175000107510": {
        "municipio": "São José dos Campos",
        "cnpj": "16540224000126",
        "banco": "001",
        "agencia": "0175",
        "dv_agencia": "9",
        "conta": "107510",
        "dv_conta": "1",
    },
    "610295000095451": {
        "municipio": "São Carlos",
        "cnpj": "10239560000157",
        "banco": "001",
        "agencia": "295",
        "dv_agencia": "x",
        "conta": "95451",
        "dv_conta": "9",
    },
    "610636000100078": {
        "municipio": "Guarulhos",
        "cnpj": "09674183000103",
        "banco": "001",
        "agencia": "0636",
        "dv_agencia": "x",
        "conta": "100078",
        "dv_conta": "0",
    },
    "611451000035998": {
        "municipio": "Guaratinguetá",
        "cnpj": "38732569000114",
        "banco": "001",
        "agencia": "1451",
        "dv_agencia": "6",
        "conta": "35998",
        "dv_conta": "x",
    },
    "616710000044657": {
        "municipio": "Suzano",
        "cnpj": "15687476000110",
        "banco": "001",
        "agencia": "6710",
        "dv_agencia": "5",
        "conta": "44657",
        "dv_conta": "2",
    },
    "617010000020738": {
        "municipio": "Cotia",
        "cnpj": "10174922000179",
        "banco": "001",
        "agencia": "7010",
        "dv_agencia": "6",
        "conta": "20738",
        "dv_conta": "1",
    },
    "611529000068798": {
        "municipio": "Barueri",
        "cnpj": "15867242000154",
        "banco": "001",
        "agencia": "1529",
        "dv_agencia": "6",
        "conta": "68798",
        "dv_conta": "7",
    },
    "613018000033663": {
        "municipio": "São José do Rio Preto",
        "cnpj": "09647887000189",
        "banco": "001",
        "agencia": "3018",
        "dv_agencia": "x",
        "conta": "36663",
        "dv_conta": "7",
    },
    "616535000043403": {
        "municipio": "Mogi das Cruzes",
        "cnpj": "09631731000100",
        "banco": "001",
        "agencia": "6534",
        "dv_agencia": "8",
        "conta": "43403",
        "dv_conta": "5",
    },
    "613304000032299": {
        "municipio": "Santo André",
        "cnpj": "12183601000166",
        "banco": "001",
        "agencia": "3304",
        "dv_agencia": "9",
        "conta": "32299",
        "dv_conta": "7",
    },
    "616511000066669": {
        "municipio": "Sorocaba",
        "cnpj": "09619773000125",
        "banco": "001",
        "agencia": "6511",
        "dv_agencia": "0",
        "conta": "66669",
        "dv_conta": "6",
    },
    "612466000047364": {
        "municipio": "Poá",
        "cnpj": "09685361000193",
        "banco": "001",
        "agencia": "2466",
        "dv_agencia": "x",
        "conta": "47364",
        "dv_conta": "2",
    },
    "610172000089569": {
        "municipio": "Rio Claro",
        "cnpj": "23845461000160",
        "banco": "001",
        "agencia": "0172",
        "dv_agencia": "4",
        "conta": "89569",
        "dv_conta": "5",
    },
    "613145000029359": {
        "municipio": "Santos",
        "cnpj": "10735023000106",
        "banco": "001",
        "agencia": "3145",
        "dv_agencia": "3",
        "conta": "29359",
        "dv_conta": "8",
    },
}


def remover_acentos(texto: str) -> str:
    texto = unicodedata.normalize("NFD", texto or "")
    return "".join(char for char in texto if unicodedata.category(char) != "Mn")


def normalizar_espacos(texto: str) -> str:
    return re.sub(r"\s+", " ", (texto or "")).strip()


def somente_digitos(texto: str) -> str:
    return re.sub(r"\D", "", texto or "")


def carregar_destinos() -> dict[str, dict]:
    return DESTINOS_POR_EXTRATO


def extrair_valor_tag(bloco: str, tag: str) -> str:
    padrao_fechado = re.compile(rf"<{tag}>(.*?)</{tag}>", re.DOTALL | re.IGNORECASE)
    match = padrao_fechado.search(bloco)
    if match:
        return match.group(1).strip()

    padrao_aberto = re.compile(rf"<{tag}>\s*(.+?)(?:\s*(?:<|\Z))", re.IGNORECASE)
    match = padrao_aberto.search(bloco)
    if match:
        return match.group(1).strip()

    return ""


def detectar_forma_e_operacao(memo: str) -> tuple[str, str]:
    memo_upper = remover_acentos(memo).upper()
    if "PIX" in memo_upper:
        return "TEL", "PIX"
    if "TED" in memo_upper:
        return "TED", "TED"
    if "TRANSFER" in memo_upper:
        return "TEL", "TEL"
    return "", ""


def formatar_data_ofx(dtposted: str) -> str:
    dt = (dtposted or "").strip()[:8]
    if len(dt) == 8:
        return f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}"
    return dt


def formatar_data_br(data_br: str) -> str:
    match = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", (data_br or "").strip())
    if not match:
        return (data_br or "").strip()
    dia, mes, ano = match.groups()
    return f"{ano}-{mes}-{dia}"


def formatar_valor_numerico(valor: str) -> str:
    texto = (valor or "").strip()
    if not texto:
        return ""

    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")

    try:
        return f"{abs(float(texto)):.2f}"
    except (TypeError, ValueError):
        return valor


def montar_registro(
    *,
    destino: dict,
    dt_doacao: str,
    valor_doacao: str,
    memo: str,
    nr_extrato_bancario: str,
    nr_banco_origem: str,
    agencia_origem: str,
    dv_agencia_origem: str,
    conta_origem: str,
    dv_conta_origem: str,
    fitid: str = "",
) -> dict:
    forma_doacao, operacao_financeira = detectar_forma_e_operacao(memo)

    return {
        "nrDocumento": "",
        "nrReciboDoacao": "",
        "nrCnpjPrestador": CNPJ_PRESTADOR,
        "anoExercicio": dt_doacao[:4],
        "tipo": TIPO,
        "nrCnpj": destino["cnpj"],
        "esferaPartidaria": ESFERA_PARTIDARIA,
        "partido": NR_PARTIDO,
        "uf": UF_DESTINO,
        "municipio": destino["municipio"],
        "dtDoacao": dt_doacao,
        "fonteRecurso": FONTE_RECURSO,
        "classificacaoDoacao": CLASSIFICACAO_DOACAO,
        "valorDoacao": valor_doacao,
        "formaDoacao": forma_doacao,
        "operacaoFinanceira": operacao_financeira,
        "nrExtratoBancario": nr_extrato_bancario,
        "nrBancoOrigem": nr_banco_origem,
        "agenciaOrigem": agencia_origem,
        "dvAgenciaOrigem": dv_agencia_origem,
        "contaCorrenteOrigem": conta_origem,
        "dvContaCorrenteOrigem": dv_conta_origem,
        "tpFundoPartidario": TP_FUNDO_PARTIDARIO,
        "nrLancamento": "",
        "nrBancoDestino": destino["banco"],
        "agenciaDestino": destino["agencia"],
        "dvAgenciaDestino": destino["dv_agencia"],
        "contaCorrenteDestino": destino["conta"],
        "dvContaCorrenteDestino": destino["dv_conta"],
        "memo_ofx": memo,
        "fitid_ofx": fitid,
    }


def processar_ofx(conteudo: str) -> list[dict]:
    destinos = carregar_destinos()
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
        checknum = somente_digitos(extrair_valor_tag(bloco, "CHECKNUM").strip())
        destino = destinos.get(checknum)
        if not destino:
            continue

        dtposted = extrair_valor_tag(bloco, "DTPOSTED")
        trnamt = extrair_valor_tag(bloco, "TRNAMT")
        memo = extrair_valor_tag(bloco, "MEMO")
        fitid = extrair_valor_tag(bloco, "FITID")

        registros.append(
            montar_registro(
                destino=destino,
                dt_doacao=formatar_data_ofx(dtposted),
                valor_doacao=formatar_valor_numerico(trnamt),
                memo=memo,
                nr_extrato_bancario=checknum,
                nr_banco_origem=banco,
                agencia_origem=AGENCIA_ORIGEM_OFX,
                dv_agencia_origem=DV_AGENCIA_ORIGEM_OFX,
                conta_origem=conta_origem,
                dv_conta_origem=dv_conta_origem,
                fitid=fitid,
            )
        )

    return registros


def extrair_texto_pdf(pdf_bytes: bytes) -> str:
    if pdfplumber is None:
        texto = extrair_texto_pdf_via_pdftotext(pdf_bytes)
        if texto.strip():
            return texto
        raise RuntimeError("Suporte a PDF indisponivel: instale pdfplumber.")

    paginas = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            texto = page.extract_text() or ""
            if not texto:
                try:
                    texto = page.extract_text(layout=True) or ""
                except TypeError:
                    texto = ""
            if texto:
                paginas.append(texto)

    texto = "\n".join(paginas)
    if texto.strip():
        return texto

    return extrair_texto_pdf_via_pdftotext(pdf_bytes)


def extrair_texto_pdf_via_pdftotext(pdf_bytes: bytes) -> str:
    executavel = which("pdftotext")
    if not executavel:
        return ""

    try:
        resultado = subprocess.run(
            [executavel, "-layout", "-", "-"],
            input=pdf_bytes,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
    except (OSError, subprocess.SubprocessError):
        return ""

    return resultado.stdout.decode("utf-8", errors="replace")


def extrair_dados_conta_pdf(texto: str) -> tuple[str, str, str, str]:
    agencia_match = re.search(r"Ag[êe]ncia(?::)?\s*(\d+)-([\dxX])", texto, re.IGNORECASE)
    conta_match = re.search(
        r"Conta(?:\s+corrente)?(?::)?\s*(\d+)-([\dxX])",
        texto,
        re.IGNORECASE,
    )
    if not agencia_match or not conta_match:
        return "", "", "", ""

    agencia, dv_agencia = agencia_match.groups()
    conta, dv_conta = conta_match.groups()
    return agencia, dv_agencia.lower(), conta, dv_conta.lower()


def extrair_inicio_bloco_pdf(texto: str) -> tuple[str, str] | None:
    match = re.fullmatch(r"(\d{2}/\d{2}/\d{4})(?:\s+(.*))?", (texto or "").strip())
    if not match:
        return None
    return match.group(1), normalizar_espacos(match.group(2) or "")


def linha_pdf_cabecalho_ou_rodape(texto: str) -> bool:
    chave = remover_acentos(normalizar_espacos(texto)).upper()
    return chave in {
        "",
        "CONSULTAS - EXTRATO DE CONTA CORRENTE",
        "EXTRATO DE CONTA CORRENTE",
        "CLIENTE - CONTA ATUAL",
        "CLIENTE P-SOL FUNDO PARTIDARIO",
        "LANCAMENTOS",
        "DT. BALANCETE DT. MOVIMENTO AG. ORIGEM LOTE HISTORICO",
        "DIA LOTE DOCUMENTO HISTORICO VALOR",
        "DOCUMENTO",
        "VALOR R$",
        "SALDO",
        "TOTAL APLICACOES FINANCEIRAS 0,00",
        "* SALDOS POR DIA BASE",
        "SUJEITOS A CONFIRMACAO NO MOMENTO DA CONTRATACAO",
    } or chave.startswith("AGENCIA:") or chave.startswith("AGENCIA ") or chave.startswith("CONTA CORRENTE ")


def extrair_linha_principal_pdf(texto: str) -> dict | None:
    texto_normalizado = normalizar_espacos(texto)

    match = re.match(
        r"^(?P<lote>\d+)"
        r"(?:\s+(?P<documento>\d{2,20}))?"
        r"(?:\s+(?P<historico>.*?))?"
        r"\s+(?P<valor>[\d.]+,\d{2})\s+\((?P<sinal>[+-])\)$",
        texto_normalizado,
    )
    if match:
        return {
            "data": "",
            "lote": match.group("lote") or "",
            "documento": somente_digitos(match.group("documento") or ""),
            "historico": normalizar_espacos(match.group("historico") or ""),
            "valor": match.group("valor") or "",
            "sinal": match.group("sinal") or "",
        }

    match = re.match(
        r"^(?:(?P<data>\d{2}/\d{2}/\d{4})\s+)?"
        r"(?:(?P<agencia>\d{4})\s+)?"
        r"(?P<lote>\d{5}(?:\s*\d{3})?)"
        r"\s+(?P<historico>.*?)"
        r"\s+(?P<documento>\d[\d.]*)"
        r"\s+(?P<valor>[\d.]+,\d{2})\s+(?P<sinal>[DC])"
        r"(?:\s*[\d.]+,\d{2}\s+[DC])?$",
        texto_normalizado,
    )
    if not match:
        return None

    return {
        "data": match.group("data") or "",
        "lote": somente_digitos(match.group("lote") or ""),
        "documento": somente_digitos(match.group("documento") or ""),
        "historico": normalizar_espacos(match.group("historico") or ""),
        "valor": match.group("valor") or "",
        "sinal": "-" if (match.group("sinal") or "").upper() == "D" else "+",
    }


def bloco_pdf_eh_resumo(data: str, descricoes: list[str], principal: dict | None) -> bool:
    if data == "00/00/0000":
        return True

    textos = descricoes[:]
    if principal:
        textos.append(principal.get("historico", ""))

    chave = normalizar_espacos(" ".join(textos)).upper().replace(" ", "")
    return chave.startswith("SALDO") or chave.startswith("SALDODODIA")


def linha_pdf_eh_descricao_transacao(texto: str) -> bool:
    chave = remover_acentos(normalizar_espacos(texto)).upper()
    return any(
        trecho in chave
        for trecho in (
            "TRANSFERENCIA",
            "TED TRANSF",
            "PIX - ENVIADO",
            "PIX - RECEBIDO",
            "PAGAMENTO DE BOLETO",
            "PAGTO CONTA",
            "PAGTO VIA AUTO-ATEND.BB",
            "TAR DOC/TED",
            "TARIFA PIX ENVIADO",
            "TARIFA PACOTE DE SERVICOS",
        )
    )


def extrair_contexto_pdf(linhas: list[str], indice_linha_principal: int) -> tuple[str, list[str]]:
    data = ""
    descricoes: list[str] = []

    for idx in range(indice_linha_principal - 1, max(-1, indice_linha_principal - 8), -1):
        linha = normalizar_espacos(linhas[idx])
        if not linha or linha_pdf_cabecalho_ou_rodape(linha):
            continue
        if extrair_linha_principal_pdf(linha):
            break

        inicio = extrair_inicio_bloco_pdf(linha)
        if not inicio:
            continue

        data, descricao_inline = inicio
        if descricao_inline:
            descricoes.insert(0, descricao_inline)
            break

        for idx_desc in range(idx - 1, max(-1, idx - 3), -1):
            linha_desc = normalizar_espacos(linhas[idx_desc])
            if not linha_desc or linha_pdf_cabecalho_ou_rodape(linha_desc):
                continue
            if extrair_linha_principal_pdf(linha_desc) or extrair_inicio_bloco_pdf(linha_desc):
                break
            descricoes.insert(0, linha_desc)
            break

        break

    return data, descricoes


def coletar_continuacoes_pdf(linhas: list[str], indice_linha_principal: int) -> list[str]:
    continuacoes = []

    for idx in range(indice_linha_principal + 1, len(linhas)):
        linha = normalizar_espacos(linhas[idx])
        if not linha:
            continue
        if linha_pdf_cabecalho_ou_rodape(linha):
            continue
        if extrair_linha_principal_pdf(linha) or extrair_inicio_bloco_pdf(linha):
            break

        proxima = ""
        for prox_idx in range(idx + 1, len(linhas)):
            proxima = normalizar_espacos(linhas[prox_idx])
            if proxima:
                break

        if linha_pdf_eh_descricao_transacao(linha) and extrair_inicio_bloco_pdf(proxima):
            break

        continuacoes.append(linha)

    return continuacoes


def processar_pdf(pdf_bytes: bytes) -> list[dict]:
    destinos = carregar_destinos()
    texto = extrair_texto_pdf(pdf_bytes)
    if not texto.strip():
        return []

    conta_origem = extrair_dados_conta_pdf(texto)
    linhas = [linha.rstrip() for linha in texto.splitlines()]
    registros = []
    agencia_origem, dv_agencia_origem, conta, dv_conta = conta_origem

    for idx, linha_bruta in enumerate(linhas):
        principal = extrair_linha_principal_pdf(linha_bruta)
        if not principal or principal["sinal"] != "-":
            continue

        destino = destinos.get(principal["documento"])
        if not destino:
            continue

        data, descricoes = extrair_contexto_pdf(linhas, idx)
        data = principal.get("data") or data
        if not data or data == "00/00/0000":
            continue
        if bloco_pdf_eh_resumo(data, descricoes, principal):
            continue

        continuacoes = coletar_continuacoes_pdf(linhas, idx)
        memo_partes = [parte for parte in [*descricoes, principal["historico"], *continuacoes] if parte]
        memo = " | ".join(memo_partes)

        registros.append(
            montar_registro(
                destino=destino,
                dt_doacao=formatar_data_br(data),
                valor_doacao=formatar_valor_numerico(principal["valor"]),
                memo=memo,
                nr_extrato_bancario=principal["documento"],
                nr_banco_origem="001",
                agencia_origem=agencia_origem,
                dv_agencia_origem=dv_agencia_origem,
                conta_origem=conta,
                dv_conta_origem=dv_conta,
                fitid="",
            )
        )

    return registros


def inferir_tipo_arquivo(nome_arquivo: str | None, conteudo: bytes) -> str:
    nome = (nome_arquivo or "").lower()
    if nome.endswith(".pdf") or conteudo.startswith(b"%PDF"):
        return "pdf"

    amostra = conteudo[:4096].upper()
    if nome.endswith(".ofx") or b"<OFX>" in amostra or b"OFXHEADER:" in amostra:
        return "ofx"

    raise ValueError(f"Formato nao suportado para o arquivo: {nome_arquivo or 'sem nome'}")


def processar_bytes_arquivo(nome_arquivo: str | None, conteudo: bytes) -> list[dict]:
    tipo = inferir_tipo_arquivo(nome_arquivo, conteudo)
    if tipo == "pdf":
        return processar_pdf(conteudo)

    texto = conteudo.decode("cp1252", errors="replace")
    return processar_ofx(texto)


def processar_caminho_arquivo(caminho: str | Path) -> list[dict]:
    path = Path(caminho)
    conteudo = path.read_bytes()
    return processar_bytes_arquivo(path.name, conteudo)
