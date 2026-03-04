# Conversores SPCA

Repositório com APIs para conversão de PDFs em CSV para importação no SPCA.

## Estrutura

```
conversor-faturasSPCA/
├── bot-faturas/     # Conversor de faturas de viagem (Nix Travel)
│   ├── main.py
│   ├── requirements.txt
│   ├── Procfile
│   └── runtime.txt
└── bot-rpa/         # Conversor de RPAs
    ├── main.py
    ├── requirements.txt
    ├── Procfile
    └── runtime.txt
```

## Deploy no Railway

Cada bot deve ser deployado separadamente no Railway:

1. Crie um novo projeto no Railway
2. Conecte este repositório
3. Configure o **Root Directory** para a pasta do bot desejado:
   - `bot-faturas` para o conversor de faturas
   - `bot-rpa` para o conversor de RPAs

## Endpoints

### bot-faturas
- `POST /converter` - Converte PDFs de faturas de viagem para CSV

### bot-rpa
- `POST /converter` - Converte PDFs de RPA para CSV
