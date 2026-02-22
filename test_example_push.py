import json
import urllib.request
import os
from dotenv import load_dotenv

load_dotenv()

url = "http://127.0.0.1:8000/push"
token = os.getenv("API_TOKEN", "o2bt!R$]7*gUt:rP,E\(PT9GoXLV22N")

# Read the file
with open("example_schedule.json", "r") as f:
    data = json.load(f)

json_data = json.dumps(data).encode("utf-8")

req = urllib.request.Request(url, data=json_data, method="POST")
req.add_header("Content-Type", "application/json")
req.add_header("x-api-token", token)

try:
    with urllib.request.urlopen(req) as response:
        print(f"Status: {response.status}")
        print(f"Response: {response.read().decode()}")
except Exception as e:
    print(f"Failed: {e}")
    print(f"Details: {e.read().decode()}")
