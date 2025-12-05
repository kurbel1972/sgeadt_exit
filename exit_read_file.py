import requests
import json
from dotenv import load_dotenv
import os
import pandas as pd
from pathlib import Path
import shutil

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
        self.inbox_path = os.getenv("inbox_path")
        self.processed_path = os.getenv("processed_path")
        self.error_path = os.getenv("error_path")
        self.counter = 0
        self.has_errors = False
    
    def send_request(self, payload):
        try:
            response = requests.post(self.url, headers=self.headers, data=json.dumps(payload))
            return response.json(), response.status_code
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}, None

    def log_error(self, row, log_path):
        # Ajustar a data para o formato "dd/MM/yyyy"
        row["orderNumberDate"] = row["orderNumberDate"].strftime('%d/%m/%Y') if not pd.isna(row["orderNumberDate"]) else None

        # Criar um DataFrame com a linha que deu erro
        error_df = pd.DataFrame([row])

        # Verificar se o arquivo de log já existe
        if os.path.exists(log_path):
            existing_df = pd.read_excel(log_path)
            updated_df = pd.concat([existing_df, error_df], ignore_index=True)
            updated_df.to_excel(log_path, index=False)
        else:
            error_df.to_excel(log_path, index=False)
        
        self.has_errors = True

    def create_payload(self, row):
        return [
            {
                "warehouseCode": row["warehouseCode"],
                "internalReference": row["internalReference"],
                "customsRegime": str(row["customsRegime"]).replace(" ", "") if not pd.isna(row["customsRegime"]) else None,
                "orderNumber": str(row["orderNumber"]).replace(" ", "") if not pd.isna(row["orderNumber"]) else None,
                "orderNumberDate": row["orderNumberDate"].strftime('%Y-%m-%d') if not pd.isna(row["orderNumberDate"]) else None,
                "diverseInfo1": row["diverseInfo1"] if not pd.isna(row["diverseInfo1"]) else None,
                "diverseInfo2": row["diverseInfo2"] if not pd.isna(row["diverseInfo2"]) else None,
                "customsDebtValue": int(row["customsDebtValue"]) if not pd.isna(row["customsDebtValue"]) else None
            }
        ]

    def process_row(self, index, row, max_retries, log_path):
        self.counter += 1
        print(f"Linha {index + 2} | internalReference: {row['internalReference']}")
        payload = self.create_payload(row)
        if self.counter <= 1:
            print(f"payload: {payload}")
            print(f"Tratando internalReference: {row['internalReference']}")

        attempt = 0
        while attempt < max_retries:
            response, status_code = self.send_request(payload)
            if status_code and status_code != 500:
                print(f"Response {row['internalReference']} ({status_code}):", response)
                break
            elif status_code == 500 or status_code is None:
                attempt += 1
                print(f"Tentativa {attempt} falhou para {row['internalReference']}: {response.get('error', response)}")
                if attempt == max_retries:
                    self.log_error(row, log_path)
            else:
                print(f"Erro não esperado para {row['internalReference']} ({status_code}):", response)
                self.log_error(row, log_path)
                break

    def move_file(self, file_path, destination_folder):
        """Move o ficheiro para a pasta de destino"""
        try:
            # Criar a pasta de destino se não existir
            os.makedirs(destination_folder, exist_ok=True)
            
            # Construir o caminho de destino
            destination = Path(destination_folder) / Path(file_path).name
            
            # Mover o ficheiro
            shutil.move(str(file_path), str(destination))
            print(f"Ficheiro movido para: {destination}")
            
            # Mover também o ficheiro de log se existir
            log_path = str(file_path).replace(".xlsx", "_log.xlsx")
            if os.path.exists(log_path):
                log_destination = Path(destination_folder) / Path(log_path).name
                shutil.move(log_path, str(log_destination))
                print(f"Ficheiro de log movido para: {log_destination}")
                
        except Exception as e:
            print(f"Erro ao mover ficheiro {file_path}: {str(e)}")

    def process_file(self, file_path, max_retries):
        print(f"\n{'='*80}")
        print(f"Processando ficheiro: {file_path}")
        print(f"{'='*80}\n")
        
        log_path = str(file_path).replace(".xlsx", "_log.xlsx")
        self.has_errors = False
        
        try:
            df = pd.read_excel(file_path, dtype={"customsRegime": str, "orderNumber": str})
            df["orderNumberDate"] = pd.to_datetime(df["orderNumberDate"], format='%d/%m/%Y', errors='coerce')

            for index, row in df.iterrows():
                self.process_row(index, row, max_retries, log_path)
                
            print(f"\nFicheiro {file_path} processado com sucesso!")
            
            # Mover ficheiro para a pasta adequada
            if self.has_errors:
                print(f"Ficheiro contém erros. A mover para pasta de erros...")
                self.move_file(file_path, self.error_path)
            else:
                print(f"Ficheiro processado sem erros. A mover para pasta de processados...")
                self.move_file(file_path, self.processed_path)
                
        except Exception as e:
            print(f"\nErro ao processar ficheiro {file_path}: {str(e)}")
            print(f"A mover ficheiro para pasta de erros...")
            self.move_file(file_path, self.error_path)

    def main(self, max_retries=2):
        print(self.client_id)
        print(self.client_secret)
        print(self.base_url)
        print(self.url)
        print(f"\nA procurar ficheiros em: {self.inbox_path}\n")

        # Verificar se a diretoria existe
        if not os.path.exists(self.inbox_path):
            print(f"ERRO: A diretoria {self.inbox_path} não existe ou não está acessível!")
            return

        # Obter todos os ficheiros .xlsx da diretoria
        excel_files = list(Path(self.inbox_path).glob("*.xlsx"))
        
        if not excel_files:
            print(f"Nenhum ficheiro .xlsx encontrado em {self.inbox_path}")
            return
        
        print(f"Encontrados {len(excel_files)} ficheiro(s) para processar:\n")
        for file in excel_files:
            print(f"  - {file.name}")
        print()

        # Processar cada ficheiro
        for file_path in excel_files:
            self.counter = 0  # Reset counter para cada ficheiro
            self.process_file(file_path, max_retries)

        print(f"\n{'='*80}")
        print("Processamento de todos os ficheiros concluído!")
        print(f"{'='*80}")

if __name__ == "__main__":    
    api = CustomsWarehouseAPI()
    api.main(max_retries=2)