import requests
import json
headers = {
    # "Authorization": 'Bearer secret_kkOM1Waqmk3VRhKIemEa0iP9bgo1UplldgPobq2lwaq',
    "Authorization": 'Bearer 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',
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}
# resp = requests.post('http://192.168.0.39:12986/self_re', headers=headers).content
resp = requests.post('https://api.notion.com/v1/databases/141b4c9c2dac803e9700e9f11a76c4ac/query', headers=headers).content
res = json.loads(resp.decode())

for el in res['results']:
    print(el)

res['results'].append(res['results'][0])

send = requests.post(
    "https://api.notion.com/v1/pages",
    json={
        "parent": { "type": "database_id", "database_id": "141b4c9c-2dac-803e-9700-e9f11a76c4ac" },
        "properties": {
            "名稱": {
                "type": "title",
                "title": [{ "type": "text", "text": { "content": "Tomatoes" } }]
            },
            '日期': {'id': 't%60Y%40', 'type': 'date', 'date': {'start': '2024-11-18', 'end': None, 'time_zone': None}},
            '數字': {'id': 'fFiQ', 'type': 'number', 'number': 1}, '日期': {'id': 't%60Y%40', 'type': 'date', 'date': {'start': '2024-11-18', 'end': None, 'time_zone': None}},
            '檔案和媒體': {'id': 'LuY%40', 'type': 'files', 'files': []},
        }
    },
    headers=headers,
)
print(send)
# print(res['results'])