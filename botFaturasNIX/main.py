import io
import os
import re
import csv
import pandas as pd
from typing import List
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pypdf import PdfReader

app = FastAPI(
    title="Extrator de Faturas de Viagem",
    description="API para extrair dados de faturas de viagem em PDF e gerar CSV",
    version="1.0.0"
)

# PERMISSÃO PARA O LOVABLE ACESSAR
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # No futuro, coloque aqui a URL do seu site Lovable
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Colunas do CSV
COLUNAS = [
    'Numero_Fatura',
    'Nº_Viag',
    'Passageiro',
    'Tipo',
    'Emissao',
    'Bilhete_NC_OS',
    'Num_OP',
    'Matricula',
    'Saida_Periodo',
    'Rota_Produto',
    'Nota_debito',
    'Reserva',
    'Vlr_Original',
    'Cambio',
    'Valor',
    'Extras',
    'Taxas',
    'Tx_Emb',
    'Taxas_DU',
    'Tx_Servico',
    'Out_Rec',
    'Desconto',
    'Total',
    'Nº_Pedido',
    'Fornecedor',
    'C_Custos',
    'Retirada',
    'Devolucao',
    'Nº_Confirm',
    'Solicitante',
    'Tipo_de_Pagamento',
    'Observacao'
]


def extrair_texto_pdf(pdf_stream) -> str:
    """Extrai texto de todas as páginas do PDF"""
    reader = PdfReader(pdf_stream)
    texto_completo = ""
    for page in reader.pages:
        texto_completo += page.extract_text() + "\n"
    return texto_completo


def extrair_numero_fatura(texto: str) -> str:
    """Extrai o número da fatura do texto"""
    match = re.search(r'(FT\d+)', texto)
    return match.group(1) if match else ""


def limpar_texto(texto: str) -> str:
    """Remove caracteres extras e normaliza espaços"""
    return ' '.join(texto.split()).strip()


def extrair_valor(texto: str) -> str:
    """Extrai valor monetário do texto"""
    texto = re.sub(r'\s*\[R\$\]', '', texto)
    return texto.strip()


def parse_bloco_registro(bloco: str, fornecedor_atual: str, solicitante_atual: str, numero_fatura: str) -> dict:
    """Parse de um bloco de texto para extrair dados de um registro"""
    registro = {col: '' for col in COLUNAS}
    registro['Numero_Fatura'] = numero_fatura
    registro['Fornecedor'] = fornecedor_atual
    registro['Solicitante'] = solicitante_atual

    linhas = bloco.split('\n')
    linhas = [l.strip() for l in linhas if l.strip()]

    # Detecta tipo de registro
    eh_passagem = any('ETICKET' in l for l in linhas)
    eh_veiculo = any('Veículo:' in l for l in linhas)

    idx = 0

    # Nº Viag (11 dígitos começando com 99)
    for i, linha in enumerate(linhas):
        if re.match(r'^\d{11}$', linha):
            registro['Nº_Viag'] = linha
            idx = i + 1
            break

    # Passageiro (SOBRENOME/NOME)
    if idx < len(linhas) and '/' in linhas[idx]:
        registro['Passageiro'] = linhas[idx]
        idx += 1

    # Tipo (ADT, CHD, INF) - só para passagens
    if eh_passagem and idx < len(linhas) and linhas[idx] in ['ADT', 'CHD', 'INF']:
        registro['Tipo'] = linhas[idx]
        idx += 1

    # Emissão (data)
    if idx < len(linhas) and re.match(r'^\d{2}/\d{2}/\d{4}$', linhas[idx]):
        registro['Emissao'] = linhas[idx]
        idx += 1

    # Bilhete/NC/OS ou VC
    if idx < len(linhas):
        linha = linhas[idx]
        if re.match(r'^[A-Z0-9]{6}$', linha) or re.match(r'^\d{10}$', linha) or linha.startswith('VC'):
            registro['Bilhete_NC_OS'] = linha
            idx += 1

    # Saída/Período
    for i in range(idx, min(idx + 5, len(linhas))):
        if re.match(r'^\d{2}/\d{2}/\d{4}$', linhas[i]):
            periodo = linhas[i]
            if i + 2 < len(linhas) and 'até' in linhas[i]:
                periodo = linhas[i] + ' ' + linhas[i+1]
            registro['Saida_Periodo'] = periodo
            break
        elif 'até' in linhas[i]:
            registro['Saida_Periodo'] = linhas[i].replace('até', '-').strip()
            if i + 1 < len(linhas) and re.match(r'^\d{2}/\d{2}/\d{4}$', linhas[i+1]):
                registro['Saida_Periodo'] += ' ' + linhas[i+1]
            break

    # Rota/Produto
    for linha in linhas:
        if 'ETICKET' in linha:
            rota = linha
            linha_idx = linhas.index(linha)
            if linha_idx + 1 < len(linhas) and 'Nacional' in linhas[linha_idx + 1]:
                rota += ' ' + linhas[linha_idx + 1]
            registro['Rota_Produto'] = limpar_texto(rota)
            break
        elif 'Hospedagem' in linha:
            registro['Rota_Produto'] = limpar_texto(linha)
            break
        elif 'Veículo:' in linha:
            registro['Rota_Produto'] = limpar_texto(linha)
            break

    # Nota débito (ND ou VC seguido de números)
    for linha in linhas:
        match = re.match(r'^(ND\d+|VC\d+)$', linha)
        if match:
            registro['Nota_debito'] = match.group(1)
            break

    # Reserva (8 dígitos)
    for linha in linhas:
        if re.match(r'^\d{8}$', linha):
            registro['Reserva'] = linha
            break

    # Extrai valores monetários em sequência
    valores = []
    inicio_valores = False
    contador_valores = 0

    for linha in linhas:
        if any(x in linha for x in ['Total Solicitante', 'Fornecedor:', 'Projeto:', 'Atividade', 'Classe do Voo', 'ROTINA']):
            if inicio_valores and contador_valores >= 9:
                break

        match = re.match(r'^([\d.,]+)\s*\[R\$\]$', linha)
        if match:
            valores.append(extrair_valor(match.group(1)))
            inicio_valores = True
            contador_valores = 1
            continue

        if inicio_valores and contador_valores < 11:
            if re.match(r'^\d,\d{6}$', linha):
                valores.append(linha)
                contador_valores += 1
                continue
            if re.match(r'^[\d.,]+$', linha) and ',' in linha:
                if 3 <= len(linha) <= 15:
                    valores.append(linha)
                    contador_valores += 1

    # Mapeia valores para colunas
    if len(valores) >= 10:
        registro['Vlr_Original'] = valores[0]
        registro['Cambio'] = valores[1]
        registro['Valor'] = valores[2]
        registro['Extras'] = valores[3]
        registro['Taxas'] = valores[4]
        registro['Tx_Emb'] = valores[5]
        registro['Taxas_DU'] = valores[6]
        registro['Tx_Servico'] = valores[7]

        if len(valores) == 10:
            registro['Out_Rec'] = valores[8]
            registro['Desconto'] = '0,00'
            registro['Total'] = valores[9]
        elif len(valores) >= 11:
            registro['Out_Rec'] = valores[8]
            registro['Desconto'] = valores[9]
            registro['Total'] = valores[10]

    # Nº Pedido
    for i, linha in enumerate(linhas):
        match = re.match(r'^(\d+-[A-Z0-9]+|[A-Z0-9]+-\d+)$', linha)
        if match:
            registro['Nº_Pedido'] = match.group(1)
            break

    # Centro de Custos
    if 'ROTINA' in bloco:
        registro['C_Custos'] = 'ROTINA'

    # Retirada e Devolução (para veículos)
    if eh_veiculo:
        for i, linha in enumerate(linhas):
            if linha in ['SDU', 'CGH', 'GIG', 'BSB', 'GRU', 'REC']:
                if not registro['Retirada']:
                    registro['Retirada'] = linha
                elif not registro['Devolucao']:
                    registro['Devolucao'] = linha

    # Nº Confirmação
    for linha in linhas:
        match = re.match(r'^(RES\d+-\d+|MV[A-Z0-9]+|\d{8})$', linha)
        if match and linha != registro['Reserva']:
            registro['Nº_Confirm'] = match.group(1)
            break

    # Tipo de Pagamento
    if 'FAT' in linhas:
        registro['Tipo_de_Pagamento'] = 'FAT'

    # Observações
    obs_parts = []
    for i, linha in enumerate(linhas):
        if linha.startswith('Projeto:') or linha == 'Projeto:':
            projeto = linha.replace('Projeto:', '').strip()
            if not projeto and i + 1 < len(linhas):
                projeto = linhas[i + 1].strip()
            if projeto:
                obs_parts.append(f"Projeto: {projeto}")
        elif linha.startswith('Atividade:') or linha.startswith('Atividade'):
            atividade = linha.replace('Atividade:', '').replace('Atividade', '').strip()
            if atividade.startswith(':'):
                atividade = atividade[1:].strip()
            if not atividade and i + 1 < len(linhas):
                atividade = linhas[i + 1].strip()
            if atividade:
                obs_parts.append(f"Atividade: {atividade}")

    if obs_parts:
        registro['Observacao'] = ' | '.join(obs_parts)

    return registro


def parse_registros(texto: str) -> list:
    """Parse do texto para extrair todos os registros"""
    registros = []
    numero_fatura = extrair_numero_fatura(texto)

    # Remove cabeçalhos repetidos das páginas
    texto = re.sub(r'Notas de Débito\nNº\s+Viag\nPassageiro\nTipo\nEmissão\n.*?Observação\n', '', texto, flags=re.DOTALL)

    # Remove rodapés das páginas
    texto = re.sub(r'CONTINUA NA PRÓXIMA PÁGINA\.\.\.\n.*?Inscr\. Est\.: ISENTA\n', '\n', texto, flags=re.DOTALL)
    texto = re.sub(r'FT\d+ - Página \d+ de \d+\n.*?Inscr\. Est\.: ISENTA\n', '\n', texto, flags=re.DOTALL)

    linhas = texto.split('\n')

    fornecedor_atual = ""
    solicitante_atual = ""

    # Encontra início de cada registro (número de viagem de 11 dígitos)
    indices_registros = []
    for i, linha in enumerate(linhas):
        if linha.startswith('Fornecedor:'):
            match = re.search(r'Fornecedor:\s*([^C]+)', linha)
            if match:
                fornecedor_atual = limpar_texto(match.group(1))

        if linha.startswith('Solicitante :') or linha.startswith('Solicitante:'):
            match = re.search(r'Solicitante\s*:\s*(.+)', linha)
            if match:
                sol = match.group(1).strip()
                if 'Total' not in sol:
                    solicitante_atual = sol

        if re.match(r'^\d{11}$', linha.strip()):
            indices_registros.append((i, fornecedor_atual, solicitante_atual))

    # Processa cada registro
    for j, (idx, fornecedor, solicitante) in enumerate(indices_registros):
        if j + 1 < len(indices_registros):
            fim = indices_registros[j + 1][0]
        else:
            fim = len(linhas)

        bloco = '\n'.join(linhas[idx:fim])

        if 'Total Solicitante' in bloco.split('\n')[0]:
            continue

        registro = parse_bloco_registro(bloco, fornecedor, solicitante, numero_fatura)

        if registro['Nº_Viag']:
            registros.append(registro)

    return registros


def calcular_total(registros: list) -> float:
    """Calcula o total geral dos registros"""
    total = 0
    for reg in registros:
        total_str = reg.get('Total', '0').replace('.', '').replace(',', '.')
        try:
            total += float(total_str) if total_str else 0
        except ValueError:
            pass
    return total


# ============================================================
# ENDPOINTS
# ============================================================

@app.get("/")
async def root():
    """Endpoint raiz com informações da API"""
    return {
        "message": "API Extrator de Faturas de Viagem",
        "version": "2.0.0",
        "endpoints": {
            "POST /converter": "Envia um ou mais PDFs e recebe um único CSV (endpoint principal para Lovable)",
            "POST /extrair": "Envia um ou mais PDFs e recebe um único CSV com os dados extraídos",
            "POST /extrair/json": "Envia um ou mais PDFs e recebe os dados em JSON",
            "GET /health": "Verifica se a API está funcionando"
        }
    }


@app.get("/health")
async def health_check():
    """Endpoint de health check"""
    return {"status": "healthy"}


@app.post("/converter")
async def converter_pdf_para_csv(files: List[UploadFile] = File(...)):
    """
    Endpoint principal para o Lovable.
    Recebe um ou mais arquivos PDF de fatura e retorna um único CSV para download.
    """
    todos_registros = []
    faturas_processadas = []

    try:
        for file in files:
            if not file.filename.endswith('.pdf'):
                continue  # Ignora arquivos que não são PDF

            # 1. Lê o PDF enviado
            conteudo_pdf = await file.read()
            pdf_stream = io.BytesIO(conteudo_pdf)

            # 2. Processa usando a lógica de extração
            texto = extrair_texto_pdf(pdf_stream)
            registros = parse_registros(texto)

            if registros:
                todos_registros.extend(registros)
                numero_fatura = extrair_numero_fatura(texto)
                if numero_fatura:
                    faturas_processadas.append(numero_fatura)

        if not todos_registros:
            raise HTTPException(status_code=422, detail="Nenhum dado encontrado nos PDFs enviados.")

        # 3. Gerar CSV em memória usando pandas
        df = pd.DataFrame(todos_registros)

        # Garante que as colunas estejam na ordem certa
        colunas_ordenadas = [c for c in COLUNAS if c in df.columns]
        df = df[colunas_ordenadas]

        stream = io.StringIO()
        df.to_csv(stream, index=False, sep=';', encoding='utf-8-sig')

        # 4. Retorna o arquivo para download
        if len(faturas_processadas) == 1:
            filename = f"{faturas_processadas[0]}_convertida.csv"
        else:
            filename = f"faturas_consolidadas_{len(faturas_processadas)}.csv"

        response = StreamingResponse(
            iter([stream.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        return response

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@app.post("/extrair")
async def extrair_fatura_csv(files: List[UploadFile] = File(...)):
    """
    Recebe um ou mais arquivos PDF de fatura e retorna um único CSV com os dados extraídos.
    """
    todos_registros = []
    faturas_processadas = []

    try:
        for file in files:
            if not file.filename.endswith('.pdf'):
                continue

            pdf_content = await file.read()
            pdf_file = io.BytesIO(pdf_content)

            texto = extrair_texto_pdf(pdf_file)
            registros = parse_registros(texto)

            if registros:
                todos_registros.extend(registros)
                numero_fatura = extrair_numero_fatura(texto)
                if numero_fatura:
                    faturas_processadas.append(numero_fatura)

        if not todos_registros:
            raise HTTPException(status_code=422, detail="Nenhum registro encontrado nos PDFs")

        # Gera CSV usando pandas
        df = pd.DataFrame(todos_registros)
        colunas_ordenadas = [c for c in COLUNAS if c in df.columns]
        df = df[colunas_ordenadas]

        stream = io.StringIO()
        df.to_csv(stream, index=False, sep=';', encoding='utf-8-sig')

        if len(faturas_processadas) == 1:
            filename = f"{faturas_processadas[0]}_gastos.csv"
        else:
            filename = f"faturas_consolidadas_{len(faturas_processadas)}.csv"

        return StreamingResponse(
            iter([stream.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar o PDF: {str(e)}")


@app.post("/extrair/json")
async def extrair_fatura_json(files: List[UploadFile] = File(...)):
    """
    Recebe um ou mais arquivos PDF de fatura e retorna os dados em JSON.
    """
    todos_registros = []
    faturas_processadas = []

    try:
        for file in files:
            if not file.filename.endswith('.pdf'):
                continue

            pdf_content = await file.read()
            pdf_file = io.BytesIO(pdf_content)

            texto = extrair_texto_pdf(pdf_file)
            registros = parse_registros(texto)

            if registros:
                todos_registros.extend(registros)
                numero_fatura = extrair_numero_fatura(texto)
                if numero_fatura:
                    faturas_processadas.append(numero_fatura)

        if not todos_registros:
            raise HTTPException(status_code=422, detail="Nenhum registro encontrado nos PDFs")

        total_geral = calcular_total(todos_registros)

        return JSONResponse({
            "faturas_processadas": faturas_processadas,
            "quantidade_faturas": len(faturas_processadas),
            "quantidade_registros": len(todos_registros),
            "total_geral": f"R$ {total_geral:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'),
            "registros": todos_registros
        })

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar o PDF: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
