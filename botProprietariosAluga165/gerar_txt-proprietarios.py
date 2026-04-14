import pdfplumber
import os
import csv
import re

# --- INÍCIO DAS MODIFICAÇÕES ---

# 1. Defina o diretório base onde seus arquivos estão localizados
base_dir = "C:\\Users\\mathe\\OneDrive\\Documentos\\Python contabilidade\\aluga\\subgrupo165proprietarios"

# 2. Peça ao usuário o nome do arquivo do relatório
nome_relatorio = input("Digite o nome do arquivo do relatório (sem a extensão .pdf): ").strip()

# 3. Construa os caminhos dos arquivos dinamicamente
# Garante que o nome do arquivo tenha .pdf no final
if not nome_relatorio.lower().endswith('.pdf'):
    nome_relatorio += '.pdf'
    
pdf_path = os.path.join(base_dir, nome_relatorio)
csv_path = os.path.splitext(pdf_path)[0] + "_para_importar.txt"
plano_contas_path = os.path.join(base_dir, "PLANODECONTASNAOAPAGAR.csv")

# --- FIM DAS MODIFICAÇÕES ---


# carregar o plano de contas como dicionário com limpeza dos nomes
plano_contas = {}
with open(plano_contas_path, mode="r", encoding="utf-8") as plano_csv:
    reader = csv.DictReader(plano_csv, delimiter=",")
    for linha in reader:
        nome = re.sub(r"\s+", " ", linha["CONDOMINIO"].strip().upper())
        numero = linha["NUMERO"].strip()
        plano_contas[nome] = numero

linhas_final_csv = []
condominios_nao_encontrados = set()
data_final = None

# ler PDF e capturar dados
with pdfplumber.open(pdf_path) as pdf:
    for pagina in pdf.pages:
        texto = pagina.extract_text()
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

                    debito_str = possiveis_valores[1].replace(".", "").replace(",", ".")
                    credito_str = possiveis_valores[2].replace(".", "").replace(",", ".")

                    try:
                        debito_val = float(debito_str)
                        credito_val = float(credito_str)
                    except ValueError:
                        continue

                    # Ignorar linhas onde ambos os valores são zero
                    if debito_val == 0 and credito_val == 0:
                        continue

                    # Buscar plano só se houver valor
                    plano_numero = plano_contas.get(nome_upper, "NÃO ENCONTRADO")
                    if plano_numero == "NÃO ENCONTRADO":
                        condominios_nao_encontrados.add(nome_upper)

                    if debito_val != 0:
                        linhas_final_csv.append([
                            data_final,
                            plano_numero,
                            "5",
                            nome,
                            possiveis_valores[1]
                        ])
                    if credito_val != 0:
                        linhas_final_csv.append([
                            data_final,
                            "5",
                            plano_numero,
                            nome,
                            possiveis_valores[2]
                        ])

# lidar com condomínios sem código no plano de contas
if condominios_nao_encontrados:
    print("\nCondomínios sem código encontrado no plano de contas:")
    for condominio in sorted(condominios_nao_encontrados):
        codigo = input(f"Digite o código para '{condominio}': ").strip()
        plano_contas[condominio] = codigo
        for linha in linhas_final_csv:
            nome_upper = re.sub(r"\s+", " ", linha[3].strip().upper())
            if nome_upper == condominio:
                if linha[1] == "NÃO ENCONTRADO":
                    linha[1] = codigo
                if linha[2] == "NÃO ENCONTRADO":
                    linha[2] = codigo

    # atualizar arquivo plano de contas
    linhas_existentes = []
    nomes_existentes = set()

    with open(plano_contas_path, mode="r", encoding="utf-8") as plano_csv:
        reader = csv.DictReader(plano_csv, delimiter=",")
        for linha in reader:
            nome = re.sub(r"\s+", " ", linha["CONDOMINIO"].strip().upper())
            linhas_existentes.append({"CONDOMINIO": nome, "NUMERO": linha["NUMERO"].strip()})
            nomes_existentes.add(nome)

    for condominio, codigo in plano_contas.items():
        if condominio not in nomes_existentes:
            linhas_existentes.append({"CONDOMINIO": condominio, "NUMERO": codigo})

    with open(plano_contas_path, mode="w", encoding="utf-8", newline="") as plano_csv:
        fieldnames = ["CONDOMINIO", "NUMERO"]
        writer = csv.DictWriter(plano_csv, fieldnames=fieldnames, delimiter=",")
        writer.writeheader()
        for linha in linhas_existentes:
            writer.writerow(linha)

# gerar CSV final
with open(csv_path, mode="w", encoding="latin1", newline="") as arquivo_txt:
    writer = csv.writer(arquivo_txt, delimiter=";")
    # NÃO escreve o cabeçalho aqui
    
    for linha in linhas_final_csv:
        data = linha[0]
        debito = linha[1]
        credito = linha[2]
        valor_raw = linha[4]
        
        # Remove todos os pontos do valor
        valor_sem_ponto = valor_raw.replace(".", "")
        
        # definir descrição conforme débito
        if debito == "5":
            descricao = "S/ CRÉD. REF. DIVERSOS N/ MÊS"
        else:
            descricao = "PG. SUAS RETIRADAS N/ MÊS"
        
        writer.writerow([data, debito, credito, valor_sem_ponto, "", descricao, "", "", "", ""])



print(f"\n✅ CSV gerado com estrutura contábil: {csv_path}")