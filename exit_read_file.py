import requests
import json
from dotenv import load_dotenv
import os
import pandas as pd

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

class CustomsWarehouseAPI:
    def __init__(self):
        self.base_url = os.getenv("base_url")
        self.endpoint = os.getenv("endpoint")
        self.url = f"{self.base_url}{self.endpoint}"
        self.client_id = os.getenv("client_id")
        self.client_secret = os.getenv("client_secret")
        self.headers = {
            "Content-Type": "application/json",
            "x-ibm-Client-ID": self.client_id,
            "x-ibm-Client-Secret": self.client_secret
        }
        self.excel_path = r"C:\Pessoal\Aplicações\SGEADT\DADOS\AbateSGEADT\1000_SGEADT_Abate Manual_19NOV.xlsx"
        self.log_path = self.excel_path.replace(".xlsx", "_log.xlsx")
        self.counter = 0
    
    def send_request(self, payload):
        try:
            response = requests.post(self.url, headers=self.headers, data=json.dumps(payload))
            return response.json(), response.status_code
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}, None

    def log_error(self, row):
        # Ajustar a data para o formato "dd/MM/yyyy"
        row["orderNumberDate"] = row["orderNumberDate"].strftime('%d/%m/%Y') if not pd.isna(row["orderNumberDate"]) else None

        # Criar um DataFrame com a linha que deu erro
        error_df = pd.DataFrame([row])

        # Verificar se o arquivo de log já existe
        if os.path.exists(self.log_path):
            existing_df = pd.read_excel(self.log_path)
            updated_df = pd.concat([existing_df, error_df], ignore_index=True)
            updated_df.to_excel(self.log_path, index=False)
        else:
            error_df.to_excel(self.log_path, index=False)

    def create_payload(self, row):
        return [
            {
                "warehouseCode": row["warehouseCode"],
                "internalReference": row["internalReference"],
                "customsRegime": str(row["customsRegime"]) if not pd.isna(row["customsRegime"]) else None,
                "orderNumber": str(row["orderNumber"]).replace(" ", "") if not pd.isna(row["orderNumber"]) else None,
                "orderNumberDate": row["orderNumberDate"].strftime('%Y-%m-%d') if not pd.isna(row["orderNumberDate"]) else None,
                "diverseInfo1": row["diverseInfo1"] if not pd.isna(row["diverseInfo1"]) else None,
                "diverseInfo2": row["diverseInfo2"] if not pd.isna(row["diverseInfo2"]) else None,
                "customsDebtValue": int(row["customsDebtValue"]) if not pd.isna(row["customsDebtValue"]) else None
            }
        ]

    def process_row(self, index, row, max_retries):
        self.counter += 1
        print(f"Linha {index + 2} | internalReference: {row['internalReference']}")
        payload = self.create_payload(row)
        if self.counter >= 1:
            print(f"payload: {payload}")
            print(f"Tratando internalReference: {row['internalReference']}")

        attempt = 0
        while attempt < max_retries:
            response, status_code = self.send_request(payload)
            print(f"Passou status_code {row['internalReference']} ({status_code}):")
            print(f"Passou Response {row['internalReference']} ({response}):")
            if status_code is None and status_code != 500:
                print(f"Response {row['internalReference']} ({status_code}):", response)
                break
            elif status_code == 500:
                attempt += 1
                print(f"Tentativa {attempt} falhou para {row['internalReference']}: {response.get('error', response)}")
                if attempt == max_retries:
                    self.log_error(row)
            else:
                print(f"Erro não esperado para {row['internalReference']} ({status_code}):", response)
                self.log_error(row)
                break

    def main(self, max_retries=2):
        print(self.client_id)
        print(self.client_secret)
        print(self.base_url)
        print(self.url)

        # df = pd.read_excel(self.excel_path)
        df = pd.read_excel(self.excel_path, dtype={"customsRegime": str}, dtype={"orderNumber": str})
        df["orderNumberDate"] = pd.to_datetime(df["orderNumberDate"], format='%d/%m/%Y', errors='coerce')

        for index, row in df.iterrows():
            self.process_row(index, row, max_retries)

if __name__ == "__main__":    
    api = CustomsWarehouseAPI()
    api.main(max_retries=2)