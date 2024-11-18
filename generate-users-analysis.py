import csv
import requests
import json
from datetime import datetime
import pytz

detection_rules_url = "http://localhost:7002"

def generate_profile_data(id):
    url = f"{detection_rules_url}/twitter/data?id={id}"

    try:
        response = requests.post(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print("Erro ao gerar dados de perfil:", e)
        return None

def generate_profile_analysis(username,accountType):
    url = f"{detection_rules_url}/twitter/analysis/{username}?accountType=${accountType}"
    try:
        response = requests.post(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print("Erro ao gerar análise de perfil:", e)
        return None

def generate_analysis(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            if not data:
                print("O arquivo JSON está vazio.")
                return
            count = 0
            for user in data:
                print(f"Gerando dados para {user['username']}")
                user_id = user["id"].lstrip("u")
                data_result = generate_profile_data(user_id) # gera dados de perfil
                if not data_result:
                    print(f"Erro ao processar {user['username']}")
                    with open('failed_data_generation.csv', 'a', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow([user_id, user['username']])
                    continue

                print(f"Gerando análise para {user['username']}")
                a_result = generate_profile_analysis(user['username'], user['accountType']) # gera análise de perfil
                if not a_result:
                    print(f"Erro ao processar {user['username']}")
                    with open('failed_analysis_analysis.csv', 'a', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow([user_id, user['username']])
                    continue

                count += 1
                if count % 1000 == 0:
                    print(f"Já foram processadas {count} contas")
    except FileNotFoundError:
        print(f"O arquivo {file_path} não foi encontrado.")
    except json.JSONDecodeError:
        print("Erro ao decodificar o arquivo JSON.")

# Defina o fuso horário de Brasília
brasilia_tz = pytz.timezone('America/Sao_Paulo')

# Converta o horário de início e término para o fuso horário de Brasília
start_time = datetime.now(pytz.utc).astimezone(brasilia_tz)
print(f"Start time: {start_time}")

generate_analysis("./users.json")

end_time = datetime.now(pytz.utc).astimezone(brasilia_tz)
print(f"End time: {end_time}")
print(f"Duration: {end_time - start_time}")