import requests
import json
import re
import os

class community_connection(object):

    def __init__(self, working_dir="./", session_id=""):
        with open("settings.conf", "r") as f:
            settings = json.load(f)
        self.url = settings["Community URL"]
        self.user = settings["Community Username"]
        self.password = settings["Community Password"]
        if session_id:
            self.session_id = session_id
        else:
            self.session_id = self.open_session()
            print(self.session_id)

        self.working_dir = working_dir

    def open_session(self):
        endpoint = self.url + "/restapi/vc/authentication/sessions/login"
        post_data = {"user.login": f"{self.user}", "user.password": f"{self.password}"}
        r = requests.post(endpoint, data=post_data)
        if r.status_code == 200:
            response = r.text
            pattern = re.compile(r".+>(.*\.).+", re.DOTALL)
            session_id = pattern.match(response).group(1)
        else:
            print (r.status_code)
            print (r.text)
        return session_id

    def get_data(self, query):
        endpoint = self.url + "/api/2.0/search?q=" + query
        headers = {"content-type": "application/json", "li-api-session-key":f"{self.session_id}"}
        r = requests.get(endpoint, headers=headers)
        if r.status_code == 200:
            return r.json()

if __name__ == "__main__":
    conn = community_connection()
    session_id = conn.open_session()
    query = "SELECT * FROM messages WHERE depth=0 and board.id = 'threat-research-knowledge' AND labels.text = 'TAU-TIN' ORDER BY post_time DESC LIMIT 5"
    query = "SELECT body FROM messages WHERE id = '39757'"
    print(json.dumps(conn.get_data(query), indent=1))


