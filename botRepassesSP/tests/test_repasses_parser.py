import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import repasses_parser


NOVO_LAYOUT_BB = """Consultas - Extrato de conta corrente
Cliente - Conta atual
Agência 300-X
Conta corrente 5050050-3P-SOL FUNDO PARTIDARIO
Lançamentos
01/09/2025 1230 99015870 Transferência recebida 551.230.000.905.050 228.663,17 C
01/09 16:12 P SOCI 00006954942000195
02/09/2025 0300 99021470 Transferência enviada 610.053.000.098.515 2.500,00 D
02/09 11:08 00008745772000164
02/09/2025 0300 99021470 Transferência enviada 610.079.000.071.664 2.500,00 D
02/09 11:08 00008745772000164
02/09/2025 0300 99021470 Transferência enviada 617.010.000.020.738 2.500,00 D
02/09 11:08 00008745772000164
"""


class RepassesParserPdfTests(unittest.TestCase):
    def test_extrai_conta_do_layout_consultas_bb(self):
        conta = repasses_parser.extrair_dados_conta_pdf(NOVO_LAYOUT_BB)
        self.assertEqual(conta, ("300", "x", "5050050", "3"))

    def test_reconhece_linha_com_saldo_colado_ao_debito(self):
        principal = repasses_parser.extrair_linha_principal_pdf(
            "01/09/2025 0000 13105303 Pagto via Auto-Atend.BB 90.104 7.915,15 D305.580,80 C"
        )
        self.assertIsNotNone(principal)
        self.assertEqual(principal["data"], "01/09/2025")
        self.assertEqual(principal["documento"], "90104")
        self.assertEqual(principal["sinal"], "-")

    def test_processa_transferencias_do_layout_consultas_bb(self):
        with patch.object(repasses_parser, "extrair_texto_pdf", return_value=NOVO_LAYOUT_BB):
            registros = repasses_parser.processar_pdf(b"%PDF-teste")

        self.assertEqual(len(registros), 3)
        self.assertEqual(
            [registro["municipio"] for registro in registros],
            ["Franca", "Botucatu", "Cotia"],
        )
        self.assertTrue(all(registro["agenciaOrigem"] == "300" for registro in registros))
        self.assertTrue(all(registro["dvAgenciaOrigem"] == "x" for registro in registros))
        self.assertTrue(all(registro["contaCorrenteOrigem"] == "5050050" for registro in registros))
        self.assertTrue(all(registro["dvContaCorrenteOrigem"] == "3" for registro in registros))
        self.assertTrue(all(registro["dtDoacao"] == "2025-09-02" for registro in registros))
        self.assertTrue(all(registro["valorDoacao"] == "2500.00" for registro in registros))
        self.assertTrue(
            all(
                registro["memo_ofx"] == "Transferência enviada | 02/09 11:08 00008745772000164"
                for registro in registros
            )
        )


if __name__ == "__main__":
    unittest.main()
