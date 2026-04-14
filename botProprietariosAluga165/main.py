"""
API FastAPI para gerar TXT contábil de proprietários a partir de PDF.
O plano de contas é enviado como JSON (vem do banco de dados do Lovable/Supabase).

Fluxo em 2 etapas:
  1. POST /analisar  → envia PDF + plano JSON → retorna nomes pendentes
  2. POST /gerar     → envia PDF + plano JSON + códigos novos → retorna TXT
"""

import io
import re
import csv
import json
import os

import pdfplumber
from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Gerador TXT Proprietários",
    description="Extrai dados de proprietários de PDF e gera TXT contábil para importação",
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


# ============================================================
# FUNÇÕES DE EXTRAÇÃO
# ============================================================

def extrair_dados_pdf(pdf_bytes: bytes) -> tuple:
    """
    Extrai do PDF:
    - data_final: string DD/MM/AAAA
    - registros: lista de dicts com nome, valores de débito e crédito
    """
    data_final = None
    registros = []

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for pagina in pdf.pages:
            texto = pagina.extract_text()
            if not texto:
                continue
            linhas = texto.split("\n")

            if data_final is None:
                for linha in linhas:
                    match = re.search(r"Data Final (\d{2}/\d{2}/\d{4})", linha)
                    if match:
                        data_final = match.group(1)

            capturando = False
            for linha in linhas:
                if "Código Nome Saldo Anterior Débitos Créditos Saldo" in linha:
                    capturando = True
                    continue
                if capturando and "Imobiliar" in linha:
                    capturando = False

                if capturando:
                    if "Totais:" in linha or "Registros:" in linha:
                        continue

                    partes = linha.split()
                    if len(partes) >= 6:
                        possiveis_valores = partes[-4:]
                        if not all("," in v or "." in v for v in possiveis_valores):
                            continue

                        nome = " ".join(partes[1:-4]).strip()
                        nome_upper = re.sub(r"\s+", " ", nome.upper())

                        debito_str = possiveis_valores[1]
                        credito_str = possiveis_valores[2]

                        try:
                            debito_val = float(debito_str.replace(".", "").replace(",", "."))
                            credito_val = float(credito_str.replace(".", "").replace(",", "."))
                        except ValueError:
                            continue

                        if debito_val == 0 and credito_val == 0:
                            continue

                        registros.append({
                            "nome": nome,
                            "nome_upper": nome_upper,
                            "debito_str": debito_str,
                            "credito_str": credito_str,
                            "debito_val": debito_val,
                            "credito_val": credito_val,
                        })

    return data_final, registros


def gerar_txt_contabil(data_final: str, registros: list, plano_contas: dict) -> str:
    """Gera o conteúdo do TXT contábil (separado por ;)."""
    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")

    for reg in registros:
        plano_numero = plano_contas.get(reg["nome_upper"], "")

        if reg["debito_val"] != 0:
            valor_sem_ponto = reg["debito_str"].replace(".", "")
            descricao = "PG. SUAS RETIRADAS N/ MÊS"
            writer.writerow([data_final, plano_numero, "5", valor_sem_ponto, "", descricao, "", "", "", ""])

        if reg["credito_val"] != 0:
            valor_sem_ponto = reg["credito_str"].replace(".", "")
            descricao = "S/ CRÉD. REF. DIVERSOS N/ MÊS"
            writer.writerow([data_final, "5", plano_numero, valor_sem_ponto, "", descricao, "", "", "", ""])

    return output.getvalue()


# ============================================================
# ENDPOINTS
# ============================================================

@app.get("/")
async def root():
    return {
        "message": "API Gerador TXT Proprietários",
        "version": "2.0.0",
        "fluxo": {
            "1. POST /analisar": "Envia PDF + plano_contas (JSON). Retorna nomes pendentes.",
            "2. POST /gerar": "Envia PDF + plano_contas (JSON) + novos_codigos (JSON). Retorna TXT."
        }
    }


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.post("/analisar")
async def analisar_pdf(
    pdf: UploadFile = File(...),
    plano_contas: str = Form(...)
):
    """
    ETAPA 1: Analisa o PDF e identifica nomes sem código.

    Parâmetros:
    - pdf: arquivo PDF do relatório
    - plano_contas: JSON string com o plano de contas {"NOME": "codigo", ...}

    Retorna:
    - data_final, total_registros, mapeados (count), pendentes (lista de nomes)
    """
    try:
        if not pdf.filename.lower().endswith('.pdf'):
            raise HTTPException(status_code=400, detail="O arquivo deve ser um PDF")

        # Parse do plano de contas vindo do banco
        try:
            plano = json.loads(plano_contas)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="plano_contas deve ser um JSON válido")

        # Normalizar chaves do plano
        plano_normalizado = {}
        for nome, codigo in plano.items():
            nome_norm = re.sub(r"\s+", " ", nome.strip().upper())
            plano_normalizado[nome_norm] = str(codigo).strip()

        pdf_bytes = await pdf.read()
        data_final, registros = extrair_dados_pdf(pdf_bytes)

        if not registros:
            raise HTTPException(status_code=422, detail="Nenhum dado de proprietário encontrado no PDF")

        # Separar mapeados e pendentes
        count_mapeados = 0
        pendentes = []
        nomes_pendentes_vistos = set()

        for reg in registros:
            if reg["nome_upper"] in plano_normalizado:
                count_mapeados += 1
            else:
                if reg["nome_upper"] not in nomes_pendentes_vistos:
                    nomes_pendentes_vistos.add(reg["nome_upper"])
                    pendentes.append(reg["nome_upper"])

        return JSONResponse({
            "data_final": data_final,
            "total_registros": len(registros),
            "mapeados": count_mapeados,
            "pendentes": sorted(pendentes)
        })

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao analisar: {str(e)}")


@app.post("/gerar")
async def gerar_txt(
    pdf: UploadFile = File(...),
    plano_contas: str = Form(...),
    novos_codigos: str = Form(default="{}")
):
    """
    ETAPA 2: Gera o TXT contábil.

    Parâmetros:
    - pdf: arquivo PDF do relatório
    - plano_contas: JSON string com o plano de contas do banco {"NOME": "codigo", ...}
    - novos_codigos: JSON string com códigos novos {"NOME": "codigo", ...}

    Retorna:
    - TXT contábil pronto para importação (latin-1, separado por ;)
    - Header X-Novos-Codigos: JSON com os novos códigos para o frontend salvar no banco
    """
    try:
        try:
            plano = json.loads(plano_contas)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="plano_contas deve ser um JSON válido")

        try:
            codigos_novos = json.loads(novos_codigos)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="novos_codigos deve ser um JSON válido")

        # Normalizar plano
        plano_normalizado = {}
        for nome, codigo in plano.items():
            nome_norm = re.sub(r"\s+", " ", nome.strip().upper())
            plano_normalizado[nome_norm] = str(codigo).strip()

        # Incorporar códigos novos
        novos_para_salvar = {}
        for nome, codigo in codigos_novos.items():
            nome_norm = re.sub(r"\s+", " ", nome.strip().upper())
            plano_normalizado[nome_norm] = str(codigo).strip()
            novos_para_salvar[nome_norm] = str(codigo).strip()

        pdf_bytes = await pdf.read()
        data_final, registros = extrair_dados_pdf(pdf_bytes)

        if not registros:
            raise HTTPException(status_code=422, detail="Nenhum dado encontrado no PDF")

        # Verificar se ainda há pendentes
        pendentes = set()
        for reg in registros:
            if reg["nome_upper"] not in plano_normalizado:
                pendentes.add(reg["nome_upper"])

        if pendentes:
            raise HTTPException(
                status_code=422,
                detail=json.dumps({
                    "message": "Ainda há nomes sem código",
                    "pendentes": sorted(pendentes)
                }, ensure_ascii=False)
            )

        # Gerar TXT
        txt_content = gerar_txt_contabil(data_final, registros, plano_normalizado)
        txt_bytes = txt_content.encode("latin-1", errors="replace")

        headers = {
            "Content-Disposition": "attachment; filename=proprietarios_para_importar.txt",
        }

        # Retornar os novos códigos no header pra o frontend salvar no Supabase
        if novos_para_salvar:
            headers["X-Novos-Codigos"] = json.dumps(novos_para_salvar, ensure_ascii=False)
            headers["Access-Control-Expose-Headers"] = "X-Novos-Codigos"

        return StreamingResponse(
            iter([txt_bytes]),
            media_type="text/plain; charset=latin-1",
            headers=headers
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao gerar TXT: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
