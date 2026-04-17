
import pdfplumber
import io

pdf_path = "/home/rafael/Downloads/Extrato Mensal PSOL SP 082025.pdf"

with pdfplumber.open(pdf_path) as pdf:
    print("--- Default extraction ---")
    page = pdf.pages[0]
    print(page.extract_text()[:500])
    
    print("\n--- Extraction with x_tolerance=2 ---")
    print(page.extract_text(x_tolerance=2)[:500])

    print("\n--- Extraction with x_tolerance=1 ---")
    print(page.extract_text(x_tolerance=1)[:500])
