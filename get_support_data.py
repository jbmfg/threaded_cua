import datetime
from community import community_connection
from bs4 import BeautifulSoup
from db_connections import sqlite_db
from collections import defaultdict

def get_support_data(db):
    session_id = ""
    community = community_connection(session_id=session_id)
    # EOL Data https://community.carbonblack.com/t5/Documentation-Downloads/Carbon-Black-Product-Release-Lifecycle-Status/ta-p/39757

    query = "SELECT body FROM messages WHERE id = '39757'"
    body = community.get_data(query)["data"]["items"][0]["body"]
    soup = BeautifulSoup(body, features="html.parser")

    tables = {}
    ps = soup.find_all("p", recursive=False)
    for x, i in enumerate(ps):
        if i.get_text() == "Carbon Black Cloud":
            tables["psc_win"] = i.findNext("table")
            tables["psc_mac"] = tables["psc_win"].findNext("table")
            tables["psc_lin"] = tables["psc_mac"].findNext("table")
    table_rows = defaultdict(list)
    for product in tables:
        trs = tables[product].find_all("tr")
        for tr in trs:
            table_rows[product].append([td.get_text().replace("\n", "").replace("\u00a0", "") for td in tr.find_all("td")])

    rows = []
    for os in table_rows:
        for d in table_rows[os][1:]:
            rows.append([os[-3:].upper()] + d)

    for x, r in enumerate(rows):
        for d in r[2:]:
            if not d: continue
            try:
                rows[x][r.index(d)] = datetime.datetime.strptime(d.strip(), "%b-%y")
            except ValueError:
                try:
                    rows[x][r.index(d)] = datetime.datetime.strptime(d, "%B-%y")
                except ValueError:
                    try:
                        rows[x][r.index(d)] = datetime.datetime.strptime(d, "%B-%Y")
                    except ValueError:
                        try:
                            rows[x][r.index(d)] = datetime.datetime.strptime(d.replace("Sept", "Sep"), "%b %Y")
                        except ValueError:
                            try:
                                rows[x][r.index(d)] = datetime.datetime.strptime(d, "%B %Y")
                            except ValueError:
                                try:
                                    rows[x][r.index(d)] = datetime.datetime.strptime(d, "%b \'%y")
                                except:
                                    raise

    # Calculate where the support is right now
    now = datetime.datetime.now()
    for x, r in enumerate(rows):
        # Just the dates
        sdates = r[2:]
        if not isinstance(sdates[2], str) and now > sdates[2]:
            lvl = "EOL"
        elif not isinstance(sdates[1], str) and now > sdates[1]:
            lvl = "EX"
        elif not isinstance(sdates[0], str) and now > sdates[0]:
            lvl = "ST"
        rows[x].append(lvl)

    fields = ["os", "version", "standard", "extended", "eol", "current_level"]
    db.insert("version_support", fields, rows, pk=False, del_table=True)




if __name__ == "__main__":
    db = sqlite_db("cua.db")
    get_support_data(db)

