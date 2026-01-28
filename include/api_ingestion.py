import requests
import json
import os

def fetch_data(url):
    """
    Fetches JSON data from the given URL.
    """
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def save_data(data, path):
    """
    Saves the data to a local file.
    """
    # Ensure the directory exists
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    with open(path, 'w') as f:
        json.dump(data, f, indent=4)
    print(f"Data saved to {path}")

if __name__ == "__main__":
    url = 'https://jsonplaceholder.typicode.com/posts'
    data = fetch_data(url)
    save_data(data, 'include/temp_data/posts.json')
