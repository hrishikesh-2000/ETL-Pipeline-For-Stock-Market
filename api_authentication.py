import requests
import json

# url = "https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id=816f7365-501c-482f-b865-fa77d3e3bbaf&redirect_uri=https://localhost:5000"

code="YtPn4i"
client_id = '816f7365-501c-482f-b865-fa77d3e3bbaf'
client_secret = '3f9cetrbbg'
redirect_uri = "https://localhost:5000"
grant_type = 'authorization_code'


url = "https://api.upstox.com/v2/login/authorization/token"

payload={
    'code' : code,
    'client_id' : client_id,
    'client_secret' : client_secret,
    'redirect_uri' : redirect_uri,
    'grant_type' : grant_type
}
headers = {
  'Content-Type': 'application/x-www-form-urlencoded',
  'Accept': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

json_response = response.json()

print(json.dumps(json_response, indent=4))