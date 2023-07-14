import json

data = [
    {"name": "John", "age": 25},
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 35}
]

with open("output.json", "w") as f:
    for item in data:
        json.dump(item, f)
        f.write('\n')
