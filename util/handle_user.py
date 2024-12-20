import json

def change_user_status(client_id: str, status: int):
    try:
        with open('util/user_status.json', 'r') as file:
            user_status = json.load(file)
            user_status[client_id] = status
            print(user_status)
        with open('util/user_status.json', 'w', encoding='utf-8') as file:
            json.dump(user_status, file)
    except Exception as e:
        print('add error', e)


def delete_status(project_name: str):
    try:
        with open('util/user_status.json', 'r') as file:
            user_status = json.load(file)
            user_status.pop(project_name)
        with open('util/user_status.json', 'w', encoding='utf-8') as file:
            json.dump(user_status, file)
            return True
    except:
        return False
