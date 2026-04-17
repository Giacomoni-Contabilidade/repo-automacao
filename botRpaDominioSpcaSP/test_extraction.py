
import json
import sys
import os

# Add current directory to path to import main
sys.path.append(os.getcwd())

from main import extrair_texto_pdf, extrair_registros_do_texto

def test():
    pdf_path = "/home/rafael/Downloads/Extrato Mensal PSOL SP 082025.pdf"
    if not os.path.exists(pdf_path):
        print(f"File not found: {pdf_path}")
        return

    with open(pdf_path, "rb") as f:
        pdf_bytes = f.read()

    print(f"Extracting text from {pdf_path}...")
    texto = extrair_texto_pdf(pdf_bytes)
    
    # Save raw text for debugging if needed
    with open("extracted_text.txt", "w", encoding="utf-8") as f:
        f.write(texto)
    print("Raw text saved to extracted_text.txt")

    print("Extracting records...")
    registros = extrair_registros_do_texto(texto)

    print(f"Extracted {len(registros)} records.")
    
    # Output as JSON to a file
    output_file = "output.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(registros, f, ensure_ascii=False, indent=2)
    
    print(f"JSON output saved to {output_file}")

    # Also show the first record
    if registros:
        print("\nFirst record sample:")
        print(json.dumps(registros[0], ensure_ascii=False, indent=2))

if __name__ == "__main__":
    test()
