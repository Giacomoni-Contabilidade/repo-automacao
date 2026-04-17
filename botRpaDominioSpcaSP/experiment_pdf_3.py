
import pdfplumber
import io

pdf_path = "/home/rafael/Downloads/Extrato Mensal PSOL SP 082025.pdf"

with pdfplumber.open(pdf_path) as pdf:
    for page in pdf.pages:
        print(f"--- Page {page.page_number} ---")
        print(page.extract_text(x_tolerance=0.5))
