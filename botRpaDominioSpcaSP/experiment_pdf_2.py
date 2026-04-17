
import pdfplumber
import io

pdf_path = "/home/rafael/Downloads/Extrato Mensal PSOL SP 082025.pdf"

with pdfplumber.open(pdf_path) as pdf:
    page = pdf.pages[0]
    print("\n--- Extraction with x_tolerance=0.5 ---")
    print(page.extract_text(x_tolerance=0.5)[:1000])
