import io
import os
import re
from pypdf import PdfReader
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Extrator de RPA - SPCA",
    description="API para extrair dados de RPAs em PDF e gerar CSV para importação no SPCA",
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

# Colunas do CSV para SPCA
COLUNAS = [
    'Tipo_Pessoa',
    'CPF',
    'Nome',
    'Tipo_Doc',
    'Num_Doc',
    'Data',
    'Valor',
    'Cod_Gasto',
    'Situacao',
    'Descricao'
]


def extrair_texto_pdf(pdf_stream) -> str:
    """Extrai texto de todas as páginas do PDF usando pypdf"""
    leitor = PdfReader(pdf_stream)
    texto = ""
    for pagina in leitor.pages:
        texto += pagina.extract_text() + "\n"
    return texto


def processar_texto_bloco(texto_completo: str, num_doc: str, data_doc: str) -> list:
    """Processa o texto do PDF e extrai os dados de cada pessoa"""
    dados = []

    # Encontrar onde começa cada pessoa
    # Padrão: Numero GRUDADO no Nome + espaço + "Contr:"
    # Ex: "162ALESSANDRA RODRIGUES FREITAS Contr:"
    iterador_pessoas = re.finditer(r'(\d{1,5})([A-ZÁÉÍÓÚÃÕÇ][A-ZÁÉÍÓÚÃÕÇ\s\.\~]+?)\s+Contr:', texto_completo)

    pessoas_encontradas = list(iterador_pessoas)

    for i, match in enumerate(pessoas_encontradas):
        # Define o início e fim do bloco de texto dessa pessoa
        inicio = match.start()
        # O fim é o início da próxima pessoa, ou o fim do arquivo
        fim = pessoas_encontradas[i+1].start() if i+1 < len(pessoas_encontradas) else len(texto_completo)

        bloco_texto = texto_completo[inicio:fim]

        # --- Extração de Dados do Bloco ---

        # Nome (Grupo 2 do regex principal)
        nome = match.group(2).strip()

        # CPF (Procura padrão de CPF em qualquer lugar do bloco)
        match_cpf = re.search(r'(\d{3}\.\d{3}\.\d{3}-\d{2})', bloco_texto)
        cpf = match_cpf.group(1).replace('.', '').replace('-', '') if match_cpf else ""

        # Valor (Procura "Proventos:" que é o mais confiável)
        match_valor = re.search(r'Proventos:\s*([\d\.]+,\d{2})', bloco_texto)
        valor = match_valor.group(1) if match_valor else "0,00"

        # Classificação do Gasto (Procura palavras chave no bloco)
        cod_spca = "221"
        sit = "1"
        desc = "DIRIGENTE PARTIDARIO - RPA"

        if "FAXINEIRA" in bloco_texto or "203 " in bloco_texto or "FAXINAS" in bloco_texto:
            cod_spca = "212"
            sit = "2"  # Situação 2 exige Qtd e Custo na planilha
            desc = "SERVICOS DE LIMPEZA - RPA"
        elif "MOTORISTA" in bloco_texto or "211 " in bloco_texto:
            cod_spca = "264"  # Transporte
            sit = "5"        # Situação 5 exige Placa
            desc = "SERVICOS MOTORISTA - RPA"
        elif "NEGRITUDE" in bloco_texto or "221 " in bloco_texto:
            desc = "DIRIGENTE PARTIDARIO - NEGRITUDE"
        elif "MULHERES" in bloco_texto or "217 " in bloco_texto:
            desc = "DIRIGENTE PARTIDARIO - MULHERES"

        # Monta o registro
        dados.append({
            'Tipo_Pessoa': "PF",
            'CPF': cpf,
            'Nome': nome,
            'Tipo_Doc': "recibo",
            'Num_Doc': num_doc,
            'Data': data_doc,
            'Valor': valor,
            'Cod_Gasto': cod_spca,
            'Situacao': sit,
            'Descricao': desc
        })

    return dados


def calcular_total(registros: list) -> str:
    """Calcula o total dos valores"""
    total = 0.0
    for reg in registros:
        valor_str = reg.get('Valor', '0,00')
        # Converte "1.234,56" para float
        valor_float = float(valor_str.replace('.', '').replace(',', '.'))
        total += valor_float
    return f"{total:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')


# ============================================================
# ENDPOINTS
# ============================================================

@app.get("/")
async def root():
    """Endpoint raiz com informações da API"""
    return {
        "message": "API Extrator de RPA - SPCA",
        "version": "1.0.0",
        "endpoints": {
            "POST /converter": "Envia um PDF de RPA e recebe um CSV para importação no SPCA",
            "POST /extrair/json": "Envia um PDF de RPA e recebe os dados em JSON",
            "GET /health": "Verifica se a API está funcionando"
        },
        "parametros": {
            "num_doc": "Número do documento (padrão: MM/AAAA do mês atual)",
            "data_doc": "Data do documento DD/MM/AAAA (padrão: último dia do mês atual)"
        }
    }


@app.get("/health")
async def health_check():
    """Endpoint de health check"""
    return {"status": "healthy"}


@app.post("/converter")
async def converter_pdf_para_csv(
    file: UploadFile = File(...),
    num_doc: str = Form(default=None),
    data_doc: str = Form(default=None)
):
    """
    Endpoint principal.
    Recebe um arquivo PDF de RPA e retorna um CSV para importação no SPCA.

    Parâmetros opcionais:
    - num_doc: Número do documento (padrão: MM/AAAA do mês atual)
    - data_doc: Data do documento DD/MM/AAAA (padrão: último dia do mês atual)
    """
    from datetime import date
    import calendar

    # Define num_doc padrão (mês/ano atual)
    if not num_doc:
        hoje = date.today()
        num_doc = hoje.strftime("%m/%Y")

    # Define data_doc padrão (último dia do mês atual)
    if not data_doc:
        hoje = date.today()
        ultimo_dia = calendar.monthrange(hoje.year, hoje.month)[1]
        data_doc = f"{ultimo_dia:02d}/{hoje.month:02d}/{hoje.year}"

    try:
        if not file.filename.endswith('.pdf'):
            raise HTTPException(status_code=400, detail="O arquivo deve ser um PDF")

        conteudo_pdf = await file.read()
        pdf_stream = io.BytesIO(conteudo_pdf)

        texto = extrair_texto_pdf(pdf_stream)
        registros = processar_texto_bloco(texto, num_doc, data_doc)

        if not registros:
            raise HTTPException(status_code=422, detail="Nenhum dado encontrado no PDF enviado.")

        # Gerar CSV em memória usando pandas
        df = pd.DataFrame(registros)
        df = df[COLUNAS]

        stream = io.StringIO()
        df.to_csv(stream, index=False, sep=';', encoding='utf-8-sig')

        # Nome do arquivo
        filename = f"importacao_rpa_{len(registros)}_registros.csv"

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


@app.post("/extrair/json")
async def extrair_rpa_json(
    file: UploadFile = File(...),
    num_doc: str = Form(default=None),
    data_doc: str = Form(default=None)
):
    """
    Recebe um arquivo PDF de RPA e retorna os dados em JSON.
    """
    from datetime import date
    import calendar

    if not num_doc:
        hoje = date.today()
        num_doc = hoje.strftime("%m/%Y")

    if not data_doc:
        hoje = date.today()
        ultimo_dia = calendar.monthrange(hoje.year, hoje.month)[1]
        data_doc = f"{ultimo_dia:02d}/{hoje.month:02d}/{hoje.year}"

    try:
        if not file.filename.endswith('.pdf'):
            raise HTTPException(status_code=400, detail="O arquivo deve ser um PDF")

        pdf_content = await file.read()
        pdf_file = io.BytesIO(pdf_content)

        texto = extrair_texto_pdf(pdf_file)
        registros = processar_texto_bloco(texto, num_doc, data_doc)

        if not registros:
            raise HTTPException(status_code=422, detail="Nenhum registro encontrado no PDF")

        total_geral = calcular_total(registros)

        return JSONResponse({
            "arquivo_processado": file.filename,
            "quantidade_registros": len(registros),
            "num_doc": num_doc,
            "data_doc": data_doc,
            "total_geral": total_geral,
            "registros": registros
        })

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar o PDF: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
