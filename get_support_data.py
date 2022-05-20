import datetime
import dateparser
import requests
import re
from community import community_connection
from bs4 import BeautifulSoup
from db_connections import sqlite_db
from collections import defaultdict

def get_support_data(db):
    urls = [
        ["psc_win", "/cbc-oer-win-sensor-on-desktop/GUID-6197EE6C-3CCD-4F4B-9DCD-110804251ACC.html"],
        ["psc_lin", "/cbc-oer-linux-sensor/GUID-8A478FFF-656E-46EC-8904-50BD08DA542A.html"],
        ["psc_mac", "/cbc-oer-macos-sensor/GUID-D4E1F76A-DDBA-43E2-A651-DFB7ACBAF621.html"]
        ]
    levels = []
    for os, url in urls:
        url = "https://docs.vmware.com/en/VMware-Carbon-Black-Cloud/services" + url
        data = requests.get(url).content
        soup = BeautifulSoup(data, features="html.parser")
        table_bodies = soup.find_all("tbody")
        for i in table_bodies:
            rows = i.find_all("tr")
            row_data = []
            for row in rows:
                entries = row.find_all("td")
                items = [entry.get_text() for entry in entries]
                if any(items):
                    levels += [[os] + items]
    for x, row in enumerate(levels):
        for xx, ri in enumerate(row):
            # First two items are os and version
            if xx < 2: continue
            # Parse the date fields
            matches = re.match(r'([a-zA-Z]+).+?([\d]+)', ri.strip())
            mon, year = matches.groups()[0], matches.groups()[1]
            if len(year) == 2:
                year = f"20{year}"
            mon_year = f"{mon} {year}"
            mon_year = f"{year}-{mon}-01"
            levels[x][xx] = dateparser.parse(mon_year).date().isoformat()
    fields = ["Product", "Release", "Enter Standard", "Enter Extended", "Enter End of Life"]

    # Calculate where the support is right now
    now = datetime.datetime.now()
    date_format = "%Y-%m-%d"
    for x, r in enumerate(levels):
        levels[x][2] = datetime.datetime.strptime(r[2], date_format)
        levels[x][3] = datetime.datetime.strptime(r[3], date_format)
        levels[x][4] = datetime.datetime.strptime(r[4], date_format)
        # Just the dates
        sdates = r[2:]
        if not isinstance(sdates[2], str) and now > sdates[2]:
            lvl = "EOL"
        elif not isinstance(sdates[1], str) and now > sdates[1]:
            lvl = "EX"
        elif not isinstance(sdates[0], str) and now > sdates[0]:
            lvl = "ST"
        levels[x].append(lvl)

    fields = ["os", "version", "standard", "extended", "eol", "current_level"]
    db.insert("version_support", fields, levels, pk=False, del_table=True)

if __name__ == "__main__":
    db = sqlite_db("cua.db")
    get_support_data(db)
