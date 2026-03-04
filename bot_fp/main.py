"""
API FastAPI para extrair recebimentos de Fundo Partidário de PDFs
e gerar XML formatado para prestação de contas (TSE)
"""

import io
import re
from decimal import Decimal
from datetime import datetime
from typing import List
import xml.etree.ElementTree as ET
from xml.dom import minidom

import pdfplumber
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="PDF to XML - Fundo Partidário",
    description="Extrai recebimentos de Fundo Partidário de extratos bancários e gera XML para TSE",
    version="1.0.0"
)

# PERMISSÃO PARA O LOVABLE ACESSAR
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def extrair_texto_pdf(arquivo_bytes: bytes) -> str:
    """Extrai todo o texto do PDF a partir de bytes."""
    texto = ""
    with pdfplumber.open(io.BytesIO(arquivo_bytes)) as pdf:
        for pagina in pdf.pages:
            texto += pagina.extract_text() + "\n"
    return texto


def parse_valor(valor_str: str) -> Decimal:
    """Converte string de valor para Decimal."""
    valor_str = valor_str.replace('.', '').replace(',', '.')
    return Decimal(valor_str)


def extrair_dados_conta(texto: str) -> tuple:
    """Extrai agência e conta com DVs separados do extrato."""
    agencia = ""
    agencia_dv = ""
    conta = ""
    conta_dv = ""

    agencia_match = re.search(r'Ag[êe]ncia\s+(\d+)-(\d)', texto)
    if agencia_match:
        agencia = agencia_match.group(1)
        agencia_dv = agencia_match.group(2)

    conta_match = re.search(r'Conta\s+corrente\s+(\d+)-(\d)', texto)
    if conta_match:
        conta = conta_match.group(1)
        conta_dv = conta_match.group(2)

    return agencia, agencia_dv, conta, conta_dv


def extrair_fundo_partidario(arquivo_bytes: bytes, nome_arquivo: str) -> List[dict]:
    """Extrai todos os recebimentos de Fundo Partidário do extrato bancário."""
    recebimentos = []

    texto = extrair_texto_pdf(arquivo_bytes)
    linhas = texto.split('\n')

    agencia, agencia_dv, conta, conta_dv = extrair_dados_conta(texto)

    i = 0
    while i < len(linhas):
        linha = linhas[i]
        linha_upper = linha.upper()

        if 'FUNDO PARTID' in linha_upper and 'TSE' in linha_upper:
            data_br = ""
            documento = ""
            valor_decimal = None

            for j in range(i - 1, max(0, i - 10), -1):
                linha_anterior = linhas[j]

                if 'Ordem Banc' in linha_anterior:
                    data_match = re.search(r'(\d{2}/\d{2}/\d{4})', linha_anterior)

                    if data_match:
                        data_br = data_match.group(1)
                        doc_match = re.search(r'(\d[\d.]+\.\d{3}\.\d{3})\s+[\d.,]+', linha_anterior)
                        if doc_match:
                            documento = doc_match.group(1).replace('.', '')
                    else:
                        if j + 1 < len(linhas):
                            linha_seguinte = linhas[j + 1]
                            data_match = re.search(r'(\d{2}/\d{2}/\d{4})', linha_seguinte)
                            if data_match:
                                data_br = data_match.group(1)

                            doc_match = re.search(r'(\d[\d.]+\.\d{3}\.\d{3})', linha_seguinte)
                            if doc_match:
                                documento = doc_match.group(1).replace('.', '')

                            valores_seguinte = re.findall(r'([\d.]+,\d{2})', linha_seguinte)
                            for v in valores_seguinte:
                                val = parse_valor(v)
                                if val > 100000:
                                    valor_decimal = val
                                    break

                    if not valor_decimal:
                        valores = re.findall(r'([\d.]+,\d{2})', linha_anterior)
                        if len(valores) >= 2:
                            val = parse_valor(valores[0])
                            if val > 100000:
                                valor_decimal = val
                        elif len(valores) == 1:
                            val = parse_valor(valores[0])
                            if val > 100000:
                                valor_decimal = val

                    break

            if valor_decimal and data_br:
                try:
                    dt = datetime.strptime(data_br, '%d/%m/%Y')
                    data_iso = dt.strftime('%Y-%m-%d')
                except:
                    data_iso = data_br

                recebimentos.append({
                    'arquivo': nome_arquivo,
                    'data': data_iso,
                    'valor': valor_decimal,
                    'agencia': agencia,
                    'agencia_dv': agencia_dv,
                    'conta': conta,
                    'conta_dv': conta_dv,
                    'documento': documento,
                    'especie': 'OB'
                })

        if 'FUNDO PARTI' in linha_upper and 'TSE' not in linha_upper:
            for j in range(i - 1, max(0, i - 5), -1):
                if 'Pix - Recebido' in linhas[j]:
                    linha_pix = linhas[j]

                    data_match = re.search(r'(\d{2}/\d{2}/\d{4})', linha_pix)
                    data_br = data_match.group(1) if data_match else ""

                    data_iso = ""
                    if data_br:
                        try:
                            dt = datetime.strptime(data_br, '%d/%m/%Y')
                            data_iso = dt.strftime('%Y-%m-%d')
                        except:
                            data_iso = data_br

                    valor_match = re.search(r'([\d.]+,\d{2})\s*C', linha_pix)
                    if valor_match:
                        valor = parse_valor(valor_match.group(1))

                        doc_match = re.search(
                            r'(\d{3}\.\d{3}\.\d{3}\.\d{3}\.\d{3}|\d{3}\.\d{3}\.\d{3}\.\d{3})',
                            linha_pix
                        )
                        documento = ""
                        if doc_match:
                            documento = doc_match.group(1).replace('.', '')

                        recebimentos.append({
                            'arquivo': nome_arquivo,
                            'data': data_iso,
                            'valor': valor,
                            'agencia': agencia,
                            'agencia_dv': agencia_dv,
                            'conta': conta,
                            'conta_dv': conta_dv,
                            'documento': documento,
                            'especie': 'PIX'
                        })
                    break

        i += 1

    # Remover duplicatas
    vistos = set()
    recebimentos_unicos = []
    for rec in recebimentos:
        chave = (rec['arquivo'], rec['data'], rec['valor'])
        if chave not in vistos:
            vistos.add(chave)
            recebimentos_unicos.append(rec)

    return recebimentos_unicos


def gerar_xml(recebimentos: List[dict]) -> bytes:
    """Gera XML formatado para prestação de contas TSE."""
    if not recebimentos:
        raise ValueError("Nenhum recebimento para exportar")

    ESPECIE_TAG = {
        "AC": "avisoCredito",
        "CC": "cartaoCredito",
        "CH": "depositoCheque",
        "EP": "depositoEspecie",
        "OB": "ordemBancaria",
        "OT": "outrosTitulosCredito",
        "PIX": "transferenciaEletronicaPIX",
        "TEL": "transferenciaEletronicaTEL",
        "TEB": "transferenciaEletronicaTEB",
        "TED": "transferenciaEletronicaTED",
    }

    ns = "http://www.tse.jus.br/2012/XMLSchema/origemRecurso.xsd"
    xsi = "http://www.w3.org/2001/XMLSchema-instance"

    root = ET.Element("spcaImportacaoArquivo")
    root.set("xmlns", ns)
    root.set("xmlns:xsi", xsi)
    root.set("xsi:schemaLocation", f"{ns} origemRecurso.xsd")

    cabecalho = ET.SubElement(root, "CABECALHO")
    ET.SubElement(cabecalho, "nrCnpjPrestador").text = "06954942000195"
    ET.SubElement(cabecalho, "anoExercicio").text = recebimentos[0]['data'][:4]

    corpo = ET.SubElement(root, "CORPO")
    origens = ET.SubElement(corpo, "origens")

    ET.SubElement(origens, "totalOrigem").text = str(len(recebimentos))

    for rec in recebimentos:
        origem = ET.SubElement(origens, "origem")

        ET.SubElement(origem, "dtEntrada").text = rec['data']
        ET.SubElement(origem, "vrOrigem").text = str(rec['valor'])
        ET.SubElement(origem, "fonteRecurso").text = "FP"
        ET.SubElement(origem, "naturezaRecurso").text = "0"

        origem_recurso = ET.SubElement(origem, "origemRecurso")
        cota_fp = ET.SubElement(origem_recurso, "cotaFundoPartidario")
        ET.SubElement(cota_fp, "tipo").text = "CF"

        ET.SubElement(origem, "classificacaoReceita").text = "313"

        especie_recurso = ET.SubElement(origem, "especieRecurso")
        tag_pai = ESPECIE_TAG.get(rec['especie'], "ordemBancaria")
        tipo_transferencia = ET.SubElement(especie_recurso, tag_pai)
        ET.SubElement(tipo_transferencia, "especieRecurso").text = rec['especie']

        if rec['especie'] == 'PIX':
            ET.SubElement(tipo_transferencia, "nrExtratoBancario").text = rec['documento']

        conta_bancaria = ET.SubElement(tipo_transferencia, "contaBancariaDestino")
        banco_destino = ET.SubElement(conta_bancaria, "bancoDestino")

        ET.SubElement(banco_destino, "nrBancoDestino").text = "001"
        ET.SubElement(banco_destino, "agenciaDestino").text = rec['agencia']
        ET.SubElement(banco_destino, "dvAgenciaDestino").text = rec['agencia_dv']
        ET.SubElement(banco_destino, "contaCorrente").text = rec['conta']
        ET.SubElement(banco_destino, "dvContaCorrente").text = rec['conta_dv']
        ET.SubElement(banco_destino, "tpFundoPartidario").text = "2"

    xml_str = ET.tostring(root, encoding='unicode')
    dom = minidom.parseString(xml_str)
    xml_formatado = dom.toprettyxml(indent="    ", encoding="UTF-8")

    return xml_formatado


@app.get("/")
async def root():
    """Endpoint de health check."""
    return {
        "status": "online",
        "service": "PDF to XML - Fundo Partidário",
        "version": "1.0.0"
    }


@app.post("/converter", response_class=Response)
async def converter_pdfs(arquivos: List[UploadFile] = File(...)):
    """
    Converte um ou mais PDFs de extratos bancários em um único XML.

    - **arquivos**: Lista de arquivos PDF para processar

    Retorna um arquivo XML no formato TSE para prestação de contas.
    """
    if not arquivos:
        raise HTTPException(status_code=400, detail="Nenhum arquivo enviado")

    todos_recebimentos = []

    for arquivo in arquivos:
        if not arquivo.filename.lower().endswith('.pdf'):
            raise HTTPException(
                status_code=400,
                detail=f"Arquivo '{arquivo.filename}' não é um PDF"
            )

        try:
            conteudo = await arquivo.read()
            recebimentos = extrair_fundo_partidario(conteudo, arquivo.filename)
            todos_recebimentos.extend(recebimentos)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Erro ao processar '{arquivo.filename}': {str(e)}"
            )

    if not todos_recebimentos:
        raise HTTPException(
            status_code=404,
            detail="Nenhum recebimento de Fundo Partidário encontrado nos PDFs"
        )

    # Ordenar por data
    def parse_data(rec):
        try:
            return datetime.strptime(rec['data'], '%Y-%m-%d')
        except:
            return datetime.min

    todos_recebimentos.sort(key=parse_data)

    try:
        xml_bytes = gerar_xml(todos_recebimentos)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao gerar XML: {str(e)}")

    return Response(
        content=xml_bytes,
        media_type="application/xml",
        headers={
            "Content-Disposition": "attachment; filename=fundoPartidario.xml"
        }
    )


@app.post("/info")
async def info_pdfs(arquivos: List[UploadFile] = File(...)):
    """
    Retorna informações sobre os recebimentos encontrados nos PDFs sem gerar o XML.

    - **arquivos**: Lista de arquivos PDF para analisar
    """
    if not arquivos:
        raise HTTPException(status_code=400, detail="Nenhum arquivo enviado")

    todos_recebimentos = []

    for arquivo in arquivos:
        if not arquivo.filename.lower().endswith('.pdf'):
            raise HTTPException(
                status_code=400,
                detail=f"Arquivo '{arquivo.filename}' não é um PDF"
            )

        try:
            conteudo = await arquivo.read()
            recebimentos = extrair_fundo_partidario(conteudo, arquivo.filename)
            todos_recebimentos.extend(recebimentos)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Erro ao processar '{arquivo.filename}': {str(e)}"
            )

    # Converter Decimal para float para serialização JSON
    for rec in todos_recebimentos:
        rec['valor'] = float(rec['valor'])

    total = sum(rec['valor'] for rec in todos_recebimentos)

    return {
        "quantidade": len(todos_recebimentos),
        "total": round(total, 2),
        "recebimentos": todos_recebimentos
    }
