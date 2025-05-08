import json

# Load the original JSON file
with open("configs/offline.apps.json", "r") as f:
    data = json.load(f)
    
user_name = "RevolutionaryEnd3752"
password = "123!@#qweQWEqazQAZ"
client_id = "93VxPCZZCzxhlbcwkLBO5A"
client_secret = "ngQUFUHhRPduskynOmF7SptRYmkq8w"


# Modify the desired fields
for app in data.get("apps", []):
    env = app.get("env", {})
    if "REDDIT_CLIENT_ID" in env:
        env["REDDIT_CLIENT_ID"] = client_id
    if "REDDIT_CLIENT_SECRET" in env:
        env["REDDIT_CLIENT_SECRET"] = client_secret
    if "REDDIT_USERNAME" in env:
        env["REDDIT_USERNAME"] = user_name

# Write the updated data to a new JSON file
with open("output/offline.apps.config.json", "w") as f:
    json.dump(data, f, indent=2)

print("Sanitized JSON saved to 'sanitized.json'")
