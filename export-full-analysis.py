import pymongo
import csv

def export_collection_to_csv():
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["detection-rules"]
        collection = db["profileanalyses"]

        cursor = collection.find({})
        try:
            first_document = next(cursor)
        except StopIteration:
            print("Nenhum dado encontrado para exportar.")
            return
    except pymongo.errors.ConnectionError as e:
        print(f"Erro de conexão: {e}")
        return
    except pymongo.errors.PyMongoError as e:
        print(f"Erro ao acessar o MongoDB: {e}")
        return

    with open('exported_analysis.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [key for key in first_document.keys() if key not in ['_id', 'profileData']]  # Exclui os campos '_id' e 'profileData'
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        # Escreve o primeiro documento sem os campos excluídos
        writer.writerow({key: first_document[key] for key in fieldnames})
        for document in cursor:
            writer.writerow({key: document[key] for key in fieldnames})

    print("Dados exportados com sucesso para 'exported_analysis.csv'.")

    # Fechar o cursor explicitamente
    cursor.close()

export_collection_to_csv()
