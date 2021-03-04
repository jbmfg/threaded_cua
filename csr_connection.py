import requests
import json
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # ignore ssl warnings
urllib3.disable_warnings(urllib3.exceptions.SNIMissingWarning)  # ignore SNI warnings
urllib3.disable_warnings(urllib3.exceptions.InsecurePlatformWarning)  # ignore insecure platform warnings


class prod_connection(object):
    def __init__(self, prod, auth_code):
        self.prod = prod
        self.auth_code = auth_code
        with open("settings.conf", "r") as f:
            settings = json.load(f)
        self.backend = settings["backends"][self.prod]
        self.password = settings["passwords"][self.prod]
        self.user = settings["username"]
        self.session = self.get_session()

    def get_session(self):
        s = requests.Session()
        s.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5/0"})
        # Login
        url = f"{self.backend}/auth/v1/sessions"
        pd = {"username":self.user,"password": self.password}
        r = s.post(url, json=pd, verify=False)
        pd["googlePasscode"] = self.auth_code
        r = s.post(url, json=pd, verify=False)
        # Handle failed google auth code
        if r.status_code == 200 and not r.json()["success"]:
            print("Google auth step failed")
            self.auth_code = input(f"Please re-enter auth code for {self.prod}: ")
            return self.get_session()
        s.headers.update({"X-Csrf-Token": r.headers["X-Csrf-Token"]})
        return s

    def request(self, url, pd={}, timeout=30, tries=3):
        if url[0] == "/":
            url = url[1:]
        # If no post data make GET request
        if not pd:
            r = self.session.get(f"{self.backend}/{url}", verify=False, timeout=timeout)
        elif pd:
            try:
                r = self.session.post(f"{self.backend}/{url}", json=pd, verify=False, headers={'Connection':'Close'}, timeout=timeout)
            except requests.exceptions.ReadTimeout:
                if tries > 0:
                    tries -= 1
                    print(f"retrying due to read time out {url}\n\n\n\n\n\n\n\n")
                    return self.request(url, pd, tries=tries)
                else:
                    print("returning false due to read timeout\n\n\n\n\n\n\n\n")
                    return False
            try:
                json.dumps(r.json())
            except json.decoder.JSONDecodeError:
                tries -= 1
                if tries > 0:
                    print(f"retrying due to json error {url}\n\n\n\n\n\n\n")
                    return self.request(url, pd, tries=tries)
                else:
                    print("returning false due to json error\n\n\n\n\n\n\n\n")
                    return False
            except Exception as e:
                print("HERHEHREHRHERHEHRE\n\n\n\n\n\n\n\n\n\n")
                print(e)
                raise
            if r.status_code > 299 and tries > 0:
                tries -= 1
                print("retrying due to >299\n\n\n\n\n")
                return self.request(url, pd, tries=tries)
            elif r.status_code > 299:
                print(f"returning false due to return code > 299 {r.status_code}\n\n\n\n\n\n\n\n\n")
                return False
        return r


