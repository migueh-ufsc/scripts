import requests
import json
from datetime import datetime
import pymongo
import pytz

twitter_integration_url = "http://localhost:7001"

# USER PROFILE DATA


def create_user(data):
    url = f"{twitter_integration_url}/user"
    headers = {
        "Content-Type": "application/json"
    }
    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()  # Levanta um erro para códigos de status HTTP 4xx/5xx
        return response.json()
    except requests.exceptions.RequestException as e:
        print("Erro ao criar usuário:", e)
        return None


def create_users_from_file(file_path):
    with open(file_path, 'r') as file:
        users = json.load(file)
        for user in users:
            user_data = {
                "id": user["id"].lstrip("u"),
                "username": user["username"],
                "name": user["name"],
                "description": user.get("description", ""),
                "location": user.get("location", ""),
                "verified": user["verified"],
                "accountCreatedAt": user["created_at"],
                "accountDeletedAt": None,  # Supondo que não há informação de exclusão de conta
                "nFollowers": user["public_metrics"]["followers_count"],
                "nFollowing": user["public_metrics"]["following_count"],
                "nTweets": user["public_metrics"]["tweet_count"]
            }
            response = create_user(user)
            if response:
                print(f"Usuário {user['username']} criado com sucesso.")
            else:
                with open('failed_users.json', 'a') as failed_file:
                    json.dump(user, failed_file)
                    failed_file.write('\n')
                print(f"Falha ao criar usuário {user['username']}.")
        file.close()


def add_tweets_to_mongo():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["twitter-integration"]
    collection = db["tweets"]

    for i in range(0, 54):
        file_path = f"./data/split_{i}.json"
        print(f"abrindo {file_path}")
        with open(file_path, 'r') as file:
            tweets = json.load(file)
            count = 0
            for tweet in tweets:
                try:
                    collection.insert_one(tweet)
                    count += 1
                    if (count % 10000 == 0):
                        print(f"foram inseridos {count} tweets")
                except pymongo.errors.PyMongoError as e:
                    with open('failed_tweets.json', 'a') as failed_file:
                        json.dump(tweet, failed_file)
                        failed_file.write('\n')
                    print(f"Erro ao inserir tweet: {e}")
        file.close()


# Defina o fuso horário de Brasília
brasilia_tz = pytz.timezone('America/Sao_Paulo')

# Converta o horário de início e término para o fuso horário de Brasília
start_time = datetime.now(pytz.utc).astimezone(brasilia_tz)
print(f"Start time: {start_time}")

create_users_from_file("./users.json")
add_tweets_to_mongo()

end_time = datetime.now(pytz.utc).astimezone(brasilia_tz)
print(f"End time: {end_time}")
print(f"Duration: {end_time - start_time}")
