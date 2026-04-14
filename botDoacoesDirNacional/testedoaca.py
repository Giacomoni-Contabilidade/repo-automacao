#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para leitura de arquivos OFX e geração de CSV com dados
para importação de Doações Financeiras a Partidos e Candidatos (SPCA Cadastro).

Tipo de lançamento:
  TRANSFERÊNCIAS FINANCEIRAS EFETUADAS - FUNDO PARTIDÁRIO - DIREÇÃO ESTADUAL - ORDINÁRIAS
  Classificação: 376

Uso:
    python testedoaca.py arquivo1.ofx [arquivo2.ofx ...] [-o saida.csv]

O script:
  1. Lê um ou mais arquivos OFX
  2. Filtra apenas lançamentos cujo número do documento (CHECKNUM) conste na lista de UFs
  3. Detecta a forma de doação e operação financeira pelo campo MEMO
  4. Gera um CSV com TODOS os campos do XML de importação do SPCA Cadastro
     - Campos fixos já preenchidos para classificação 376
     - Campos extraídos do OFX preenchidos automaticamente
     - Campos faltantes deixados em branco para preenchimento posterior

Campos do CSV (conforme XSD do SPCA):
  nrCnpjPrestador, anoExercicio, tipo, nrCnpj, esferaPartidaria, partido,
  uf, municipio, dtDoacao, fonteRecurso, classificacaoDoacao, valorDoacao,
  formaDoacao, operacaoFinanceira, nrExtratoBancario,
  nrBancoOrigem, agenciaOrigem, dvAgenciaOrigem,
  contaCorrenteOrigem, dvContaCorrenteOrigem, tpFundoPartidario,
  nrDocumento, nrLancamento, nrReciboDoacao,
  nrBancoDestino, agenciaDestino, dvAgenciaDestino,
  contaCorrenteDestino, dvContaCorrenteDestino,
  memo_ofx, fitid_ofx
"""

import sys
import os
import csv
import re
import io
import argparse

import httpx

# =============================================================================
# CONFIGURAÇÕES FIXAS - Classificação 376
# Transf. Financeiras Efetuadas - Fundo Partidário - Dir. Estadual - Ordinárias
# =============================================================================

APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbzriEVGMb23KfoqDpYNX8vqUZTFJzRXF2FXiIk2sVCqiTUhmxAz5X1INHwsc1BZEAT3xw/exec"

CNPJ_PRESTADOR = "06954942000195"
TIPO = "PP"                   # PP = Partido Político
NR_PARTIDO = "50"             # Número de legenda do partido
ESFERA_PARTIDARIA = "ESTADUAL"
FONTE_RECURSO = "FP"          # FP = Fundo Partidário
CLASSIFICACAO_DOACAO = "376"  # 376 = Transf. Fin. Efet. - FP - Dir. Estadual - Ordinárias
TP_FUNDO_PARTIDARIO = "2"    # 0=Fundação, 1=Mulher, 2=Ordinário

# =============================================================================
# LISTA DE DOCUMENTOS POR UF
# Chave: número do documento (CHECKNUM no OFX, sem pontos)
# Valor: sigla da UF
#
# ATENÇÃO: Verifique se os números estão corretos e ajuste se necessário.
# A entrada marcada como "??" precisa ser confirmada (pode ser SC, SE ou SP).
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
# PARSER OFX (formato SGML v1)
# =============================================================================

def ler_arquivo_ofx(caminho):
    """Lê um arquivo OFX e retorna o conteúdo como string."""
    # Tenta diferentes encodings comuns em OFX brasileiro
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            with open(caminho, "r", encoding=enc) as f:
                return f.read()
        except (UnicodeDecodeError, UnicodeError):
            continue
    raise ValueError(f"Não foi possível decodificar o arquivo: {caminho}")


def extrair_valor_tag(bloco, tag):
    """Extrai o valor de uma tag SGML/OFX dentro de um bloco de texto."""
    # Tenta formato com tag de fechamento: <TAG>valor</TAG>
    padrao_fechado = re.compile(
        rf"<{tag}>(.*?)</{tag}>", re.DOTALL | re.IGNORECASE
    )
    m = padrao_fechado.search(bloco)
    if m:
        return m.group(1).strip()

    # Formato sem tag de fechamento: <TAG>valor\n
    padrao_aberto = re.compile(
        rf"<{tag}>\s*(.+?)(?:\s*(?:<|\Z))", re.IGNORECASE
    )
    m = padrao_aberto.search(bloco)
    if m:
        return m.group(1).strip()

    return ""


def extrair_transacoes(conteudo_ofx):
    """Extrai todas as transações (STMTTRN) de um conteúdo OFX."""
    transacoes = []

    # Extrai dados da conta (banco e agência/conta)
    banco = extrair_valor_tag(conteudo_ofx, "BANKID")
    conta_id = extrair_valor_tag(conteudo_ofx, "ACCTID")

    # Separa conta e dígito verificador se houver hífen
    conta_origem = conta_id
    dv_conta_origem = ""
    if "-" in conta_id:
        partes = conta_id.split("-")
        conta_origem = partes[0].strip()
        dv_conta_origem = partes[1].strip() if len(partes) > 1 else ""

    # Encontra todos os blocos STMTTRN
    blocos = re.findall(
        r"<STMTTRN>(.*?)</STMTTRN>", conteudo_ofx, re.DOTALL | re.IGNORECASE
    )

    for bloco in blocos:
        trntype = extrair_valor_tag(bloco, "TRNTYPE")
        dtposted = extrair_valor_tag(bloco, "DTPOSTED")
        trnamt = extrair_valor_tag(bloco, "TRNAMT")
        fitid = extrair_valor_tag(bloco, "FITID")
        checknum = extrair_valor_tag(bloco, "CHECKNUM")
        memo = extrair_valor_tag(bloco, "MEMO")

        transacoes.append({
            "trntype": trntype,
            "dtposted": dtposted,
            "trnamt": trnamt,
            "fitid": fitid,
            "checknum": checknum,
            "memo": memo,
            "banco_origem": banco,
            "conta_origem": conta_origem,
            "dv_conta_origem": dv_conta_origem,
        })

    return transacoes


# =============================================================================
# DETECÇÃO DE FORMA DE DOAÇÃO E OPERAÇÃO FINANCEIRA
# =============================================================================

def detectar_forma_e_operacao(memo):
    """
    Detecta formaDoacao e operacaoFinanceira com base no campo MEMO do OFX.

    Conforme XSD do SPCA:
      formaDoacao:          CH | TEL | TED | TEB
      operacaoFinanceira:   PIX | TED | TEB | TEL | EP

    Mapeamento do extrato BB:
      "Pix - Enviado"            → formaDoacao=TEL,  operacaoFinanceira=PIX
      "TED Transf.Eletr..."      → formaDoacao=TED,  operacaoFinanceira=TED
      "Transferência enviada"    → formaDoacao=TEL,  operacaoFinanceira=TEL
    """
    memo_upper = memo.upper()

    if "PIX" in memo_upper:
        return "TEL", "PIX"
    elif "TED" in memo_upper:
        return "TED", "TED"
    elif "TRANSFERÊNCIA" in memo_upper or "TRANSFERENCIA" in memo_upper:
        return "TEL", "TEL"
    else:
        return "", ""


# =============================================================================
# FORMATAÇÃO DE DADOS
# =============================================================================

def formatar_data(dtposted):
    """Converte data OFX (YYYYMMDD) para formato XML do SPCA (YYYY-MM-DD)."""
    dt = dtposted.strip()[:8]  # Pega apenas YYYYMMDD
    if len(dt) == 8:
        return f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}"
    return dt


def formatar_valor(trnamt):
    """Converte valor OFX para formato decimal positivo."""
    try:
        valor = abs(float(trnamt.strip()))
        return f"{valor:.2f}"
    except (ValueError, AttributeError):
        return trnamt


# =============================================================================
# PROCESSAMENTO PRINCIPAL
# =============================================================================

def enviar_para_sheets(csv_string, sheet_name="Preencher Dados"):
    """Envia o CSV para o Google Sheets via Apps Script."""
    print(f"\nEnviando para Google Sheets (aba: {sheet_name})...")
    with httpx.Client(follow_redirects=True, timeout=60) as client:
        resp = client.post(
            APPS_SCRIPT_URL,
            data={
                "csv": csv_string,
                "sheetName": sheet_name,
            },
        )

    if resp.status_code != 200:
        print(f"ERRO ao enviar para Google Sheets: {resp.status_code} - {resp.text}")
        return None

    result = resp.json()
    print(f"  Planilha: {result.get('url', 'N/A')}")
    print(f"  Linhas inseridas: {result.get('rows', 0) - 1}")
    return result


def processar_ofx(caminhos_ofx, caminho_csv, sheet_name="Preencher Dados"):
    """Processa os arquivos OFX e gera o CSV."""

    registros = []
    total_transacoes = 0
    total_ignoradas = 0
    nr_lancamento = 0

    for caminho in caminhos_ofx:
        print(f"\nProcessando: {caminho}")
        conteudo = ler_arquivo_ofx(caminho)
        transacoes = extrair_transacoes(conteudo)
        print(f"  Transações encontradas: {len(transacoes)}")

        for trn in transacoes:
            total_transacoes += 1
            checknum = trn["checknum"].strip()

            # Filtra: só considera se o CHECKNUM está na lista
            if checknum not in DOCUMENTOS_UF:
                total_ignoradas += 1
                continue

            uf = DOCUMENTOS_UF[checknum]
            dados_dest = DADOS_UF.get(uf, {})
            nr_lancamento += 1
            forma_doacao, operacao_financeira = detectar_forma_e_operacao(trn["memo"])

            registro = {
                # --- CAMPOS PARA PREENCHIMENTO MANUAL ---
                "nrDocumento": "",
                "nrReciboDoacao": "",

                # --- CABEÇALHO (XML: CABECALHO) ---
                "nrCnpjPrestador": CNPJ_PRESTADOR,
                "anoExercicio": trn["dtposted"].strip()[:4],

                # --- BENEFICIÁRIO (XML: beneficiario > partido) ---
                "tipo": TIPO,
                "nrCnpj": dados_dest.get("cnpj", ""),
                "esferaPartidaria": ESFERA_PARTIDARIA,
                "partido": NR_PARTIDO,
                "uf": uf,

                # --- DADOS DA DOAÇÃO ---
                "dtDoacao": formatar_data(trn["dtposted"]),
                "fonteRecurso": FONTE_RECURSO,
                "classificacaoDoacao": CLASSIFICACAO_DOACAO,
                "valorDoacao": formatar_valor(trn["trnamt"]),
                "formaDoacao": forma_doacao,
                "operacaoFinanceira": operacao_financeira,
                "nrExtratoBancario": checknum,

                # --- CONTA BANCÁRIA DE ORIGEM (XML: contaBancariaOrigem) ---
                "nrBancoOrigem": trn["banco_origem"],
                "agenciaOrigem": "1230",
                "dvAgenciaOrigem": "0",
                "contaCorrenteOrigem": trn["conta_origem"],
                "dvContaCorrenteOrigem": trn["dv_conta_origem"],
                "tpFundoPartidario": TP_FUNDO_PARTIDARIO,

                # --- DOCUMENTO E LANÇAMENTO ---
                "nrLancamento": str(nr_lancamento),

                # --- CONTA BANCÁRIA DE DESTINO (XML: contaBancariaDestino) ---
                "nrBancoDestino": "001",
                "agenciaDestino": dados_dest.get("agencia", ""),
                "dvAgenciaDestino": dados_dest.get("dv_agencia", ""),
                "contaCorrenteDestino": dados_dest.get("conta", ""),
                "dvContaCorrenteDestino": dados_dest.get("dv_conta", ""),

                # --- REFERÊNCIA OFX (auxiliar, não vai para importação) ---
                "memo_ofx": trn["memo"],
                "fitid_ofx": trn["fitid"],
            }

            registros.append(registro)

    # Ordena por data e UF
    registros.sort(key=lambda r: (r["dtDoacao"], r["uf"]))

    # Gera CSV (arquivo local + string para envio)
    csv_string = ""
    if registros:
        campos = list(registros[0].keys())

        # Gera CSV em memória
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=campos, delimiter=",")
        writer.writeheader()
        writer.writerows(registros)
        csv_string = buf.getvalue()

        # Salva arquivo local
        with open(caminho_csv, "w", newline="", encoding="utf-8-sig") as f:
            f.write(csv_string)

    # Resumo
    total_aceitas = len(registros)
    print(f"\n{'='*60}")
    print(f"RESUMO")
    print(f"{'='*60}")
    print(f"  Total de transações lidas:    {total_transacoes}")
    print(f"  Ignoradas (fora da lista):    {total_ignoradas}")
    print(f"  Aceitas (na lista):           {total_aceitas}")
    print(f"  CSV gerado em:                {caminho_csv}")
    print(f"{'='*60}")

    if total_aceitas > 0:
        # Mostra resumo por UF
        resumo_uf = {}
        for r in registros:
            uf = r["uf"]
            val = float(r["valorDoacao"])
            if uf not in resumo_uf:
                resumo_uf[uf] = {"qtd": 0, "total": 0.0}
            resumo_uf[uf]["qtd"] += 1
            resumo_uf[uf]["total"] += val

        print(f"\n  {'UF':<6} {'Qtd':>5} {'Valor Total':>15}")
        print(f"  {'-'*28}")
        for uf in sorted(resumo_uf):
            dados = resumo_uf[uf]
            print(f"  {uf:<6} {dados['qtd']:>5} {dados['total']:>15,.2f}")

    # Envia para Google Sheets
    if csv_string:
        enviar_para_sheets(csv_string, sheet_name)

    return registros


# =============================================================================
# PONTO DE ENTRADA
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Converte OFX em CSV para importação SPCA - Doações Financeiras"
    )
    parser.add_argument(
        "arquivos",
        nargs="+",
        help="Caminho(s) do(s) arquivo(s) OFX"
    )
    parser.add_argument(
        "-o", "--output",
        default="doacoes_financeiras.csv",
        help="Caminho do arquivo CSV de saída (padrão: doacoes_financeiras.csv)"
    )
    parser.add_argument(
        "-s", "--sheet",
        default="Preencher Dados",
        help="Nome da aba na planilha Google (padrão: Preencher Dados)"
    )

    args = parser.parse_args()

    # Valida arquivos
    for arq in args.arquivos:
        if not os.path.isfile(arq):
            print(f"ERRO: Arquivo não encontrado: {arq}")
            sys.exit(1)

    processar_ofx(args.arquivos, args.output, args.sheet)


if __name__ == "__main__":
    main()