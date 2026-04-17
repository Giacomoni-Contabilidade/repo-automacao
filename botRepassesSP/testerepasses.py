#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para leitura de arquivos OFX ou PDF e geracao de CSV com dados
para importacao de repasses aos diretorios municipais de SP.

Uso:
    python testerepasses.py arquivo1.ofx arquivo2.pdf [-o saida.csv]
"""

import argparse
import csv
import io
import os
import sys

import httpx

from repasses_parser import processar_caminho_arquivo


APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbzriEVGMb23KfoqDpYNX8vqUZTFJzRXF2FXiIk2sVCqiTUhmxAz5X1INHwsc1BZEAT3xw/exec"


def enviar_para_sheets(csv_string: str, sheet_name: str = "Preencher Dados"):
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


def processar_arquivos(caminhos_entrada, caminho_csv, sheet_name="Preencher Dados"):
    registros = []

    for caminho in caminhos_entrada:
        print(f"\nProcessando: {caminho}")
        try:
            registros_arquivo = processar_caminho_arquivo(caminho)
        except (RuntimeError, ValueError) as exc:
            print(f"  ERRO: {exc}")
            continue

        print(f"  Lancamentos aceitos: {len(registros_arquivo)}")
        registros.extend(registros_arquivo)

    registros.sort(
        key=lambda registro: (
            registro["dtDoacao"],
            registro.get("municipio", ""),
            registro["nrExtratoBancario"],
        )
    )

    for indice, registro in enumerate(registros, start=1):
        registro["nrLancamento"] = str(indice)

    csv_string = ""
    if registros:
        campos = list(registros[0].keys())
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=campos, delimiter=",")
        writer.writeheader()
        writer.writerows(registros)
        csv_string = buf.getvalue()

        with open(caminho_csv, "w", newline="", encoding="utf-8-sig") as arquivo_saida:
            arquivo_saida.write(csv_string)

    print(f"\n{'=' * 60}")
    print("RESUMO")
    print(f"{'=' * 60}")
    print(f"  Aceitas (na lista):           {len(registros)}")
    print(f"  CSV gerado em:                {caminho_csv}")
    print(f"{'=' * 60}")

    if registros:
        resumo_municipio = {}
        for registro in registros:
            municipio = registro.get("municipio", "")
            valor = float(registro["valorDoacao"])
            if municipio not in resumo_municipio:
                resumo_municipio[municipio] = {"qtd": 0, "total": 0.0}
            resumo_municipio[municipio]["qtd"] += 1
            resumo_municipio[municipio]["total"] += valor

        print(f"\n  {'Municipio':<28} {'Qtd':>5} {'Valor Total':>15}")
        print(f"  {'-' * 50}")
        for municipio in sorted(resumo_municipio):
            dados = resumo_municipio[municipio]
            print(f"  {municipio:<28} {dados['qtd']:>5} {dados['total']:>15,.2f}")

        enviar_para_sheets(csv_string, sheet_name)

    return registros


def main():
    parser = argparse.ArgumentParser(
        description="Converte OFX/PDF em CSV para importacao SPCA - Repasses Municipais SP"
    )
    parser.add_argument(
        "arquivos",
        nargs="+",
        help="Caminho(s) do(s) arquivo(s) OFX ou PDF",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="repasses_sp.csv",
        help="Caminho do arquivo CSV de saida (padrao: repasses_sp.csv)",
    )
    parser.add_argument(
        "-s",
        "--sheet",
        default="Preencher Dados",
        help="Nome da aba na planilha Google (padrao: Preencher Dados)",
    )

    args = parser.parse_args()

    for arquivo in args.arquivos:
        if not os.path.isfile(arquivo):
            print(f"ERRO: Arquivo nao encontrado: {arquivo}")
            sys.exit(1)

    processar_arquivos(args.arquivos, args.output, args.sheet)


if __name__ == "__main__":
    main()
