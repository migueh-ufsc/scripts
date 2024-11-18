from datetime import datetime
import pandas as pd
import csv
import pymongo
import pytz

sample_path = './data/200k_user_sample.json'
sample_labels = './data/200k_user_sample_labels.csv'
brasilia_tz = pytz.timezone('America/Sao_Paulo')

def find_usernames_by_user_ids(file_path, mongo_uri) -> list:
  # print(f"Opening file: {file_path}")
  user_ids = []
  with open(file_path, 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip the header row
    for row in reader:
      user_ids.append(row[0])  # Assuming user_id is in the first column
  print(f"User IDs loaded: {user_ids}")

  # Connect to MongoDB
  print(f"Connecting to MongoDB at {mongo_uri}")
  client = pymongo.MongoClient(mongo_uri)
  db = client['twitter-integration']
  users_collection = db['users']

  # Find all usernames for the given user_ids
  user_data = []
  for user_id in user_ids:
    print(f"Searching for user with user_id: {user_id}")
    user = users_collection.find_one({'id': user_id})
    if user:
      user_data.append({'id': user_id, 'username': user['username']})
      print(f"Found user: {user_id}, {user['username']}")
    else:
      print(f"No user found for user_id: {user_id}")

  # Close the MongoDB connection
  print("Closing MongoDB connection")
  client.close()
  return user_data

labels = pd.read_csv(sample_labels)
usernames = find_usernames_by_user_ids(sample_labels, 'mongodb://localhost:27017/')
usernames_df = pd.DataFrame(usernames)
labels['id'] = labels['id'].astype(str)
usernames_df['id'] = usernames_df['id'].astype(str)
joined = labels.merge(usernames_df, on='id', how='inner')
output_path = './data/joined_labels.csv'
print(f"Writing joined data to {output_path}")
joined.to_csv(output_path, index=False)
print("Data written successfully")

start = datetime.now(pytz.utc).astimezone(brasilia_tz)
print(f"Start time: {start}")

print(f"Connecting to MongoDB at {'mongodb://localhost:27017/'}")
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['detection-rules']
profiledatas_collection = db['profiledatas']
profileanalyses_collection = db['profileanalyses']

labels = pd.read_csv('./data/joined_labels.csv')

for index, row in labels.iterrows():
    username = row['username']
    label = row['label']
    print(f"Searching for profile data with username: {username}")
    profile_data = profiledatas_collection.find_one({'username': username})
    if profile_data:
        profile_data_id = profile_data['_id']
        print(f"Found profile data ID: {profile_data['_id']}")
        print(f"Updating label for profile data ID: {profile_data_id}")
        result = profileanalyses_collection.update_one({'profileData': profile_data_id}, {'$set': {'accountType': label}})
        if result.modified_count:
            print(f"Label updated successfully for profile data ID: {profile_data_id}")
    else:
        print(f"No profile data found for username: {username}")

# Close the MongoDB connection
print("Closing MongoDB connection")
client.close()

end = datetime.now(pytz.utc).astimezone(brasilia_tz)
print(f"End time: {end}")
print(f"Elapsed time: {end - start}")