import concurrent.futures
import requests
import json
import re
import sqlite3
import time
import sqlite3
from math import ceil
from requests_futures.sessions import FuturesSession

CONNECTIONS = 50

class csr_data(object):
    def __init__(self, sfdb, db, csr, new_run=False):
        self.sfdb = sfdb
        self.db = db
        self.csr = csr
        if new_run:
            self.delete_existing_tables()
        self.insts = self.create_customer_table_thread()

    def delete_existing_tables(self):
        del_tables = ["audit", "kits", "alerts", "endpoints", "dashboards", "connectors", "forwarders", "policy_ids", "rules"]
        for t in del_tables:
            print(f"deleting table {t}")
            query = f"DROP TABLE IF EXISTS {t};"
            self.db.execute(query)

    def create_customer_table_thread(self):
        # Helper to take the data row and request the org_key.  Returns data row with org_key
        def append_orgkey(row, tries=3):
            inst_id, prod, org_id = row[0], row[1], row[2]
            r = self.csr[prod].request(f"/appservices/v5/orgs/{org_id}")
            if r.status_code != 200 and tries != 0:
                tries -= 1
                return append_orgkey(row, tries=tries)
            org_key = r.json()["organization"]["orgKey"] if r.status_code == 200 else ""
            return [inst_id, prod, org_id, org_key]

        # Get all the customers from the sf_data table and push to new table for csr requests
        query = "select inst_id, backend, org_id from sf_data order by inst_id;"
        data = self.db.execute(query)
        url = "/appservices/v5/orgs/{}"
        insert_data = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
            future_to_url = (executor.submit(append_orgkey, r) for r in data)
            for future in concurrent.futures.as_completed(future_to_url):
                insert_data.append(future.result())
        fields = ["inst_id", "prod", "org_id", "org_key"]
        self.db.insert("customers", fields, insert_data, pk=True, del_table=True)

    def  get_audit(self):
        def return_audit(row, audit_item, tries=3):
            inst_id, prod, org_id = row[0], row[1], row[2]
            pd = {
                    "version": "1",
                    "fromRow": 1,
                    "maxRows": 10000,
                    "searchWindow": "ALL",
                    "sortDefinition": {"fieldName": "TIME", "sortOrder": "DESC"},
                    "criteria": {
                        "FLAGGED_ENTRIES": ["false"],
                        "VERBOSE_ENTRIES": ["false"],
                        "QUERY_STRING_TYPE": [audit_item]
                        },
                    "orgId": org_id
                    }
            r = self.csr[prod].request(f"/adminServices/v5/orgs/{org_id}/auditlog", pd=pd)
            if not r:
                return [[inst_id, "wrong", "csr", "prod?"]]
            if r.status_code != 200 and tries != 0:
                tries -= 1
                return return_audit(row, audit_item, tries=tries)
            else:
                audit_rows = []
                if not r.json()["entries"]:
                    return [[inst_id, "", "", ""]]
                for audit_row in r.json()["entries"]:
                    desc = re.sub(r'<.*?>', '', audit_row["description"][:100])
                    audit_rows.append([inst_id, audit_row["loginName"], audit_row["eventTime"], desc])
                return audit_rows

        query = "select distinct inst_id from audit;"
        already_inserted = [i[0] for i in self.db.execute(query)]
        query = "select inst_id, prod, org_id from customers;"
        data = self.db.execute(query)
        needs = [row for row in data if row[0] not in already_inserted]
        audit_items = ["log* success*", "bypass", "policy", "Added user"]
        fields = ["inst_id", "user", "event_time", "description"]
        insert_data = []
        for ai in audit_items:
            ct = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
                future_to_url = (executor.submit(return_audit, r, ai) for r in needs)
                for future in concurrent.futures.as_completed(future_to_url):
                    ct += 1
                    #insert_data.extend(future.result())
                    self.db.insert("audit", fields, future.result(), pk=False, del_table=False)
                    print(f"Audit - {ct} of {len(needs)}")

    def get_endpoints(self):
        start= time.time()
        def return_endpoints(row, tries=3):
            print(f"working on {row[0]}")
            start = time.time()
            req_rows = 10000
            inst_id, prod, org_key = row[0], row[1], row[2]
            pd = {
                    "criteria": {
                        "last_contact_time": {"range": "-31d"}},
                    "rows": req_rows,
                    "start": 0,
                    "sort": [{"field": "last_contact_time", "order": "desc"}]
                    }
            new = time.time()
            r = self.csr[prod].request(f"/appservices/v6/orgs/{org_key}/devices/_search", pd=pd)
            if not r:
                return [[inst_id] + ["TO"] * 15]
            elif len(r.content) < 10:
                # Most likely this installation has the wrong prod in sf
                # But sometimes something else happens that breaks the returned result from making it into futures.results()
                return [[inst_id] + ["NA"] * 15]
            elif r.status_code == 200:
                print(f"time to make first call = {time.time() - new} - {r.status_code}, {inst_id}")
                response = r.json()
                total_endpoints = response["num_found"]
                if total_endpoints == 0:
                    return [[inst_id] + ["No deployment"] * 15]
                results = [
                        [inst_id,
                        i["id"],
                        i["sensor_version"],
                        i["deployment_type"],
                        i["os_version"],
                        i["organization_name"],
                        i["status"],
                        i["registered_time"],
                        i["organization_id"],
                        i["deregistered_time"],
                        i["last_reported_time"],
                        i["sensor_out_of_date"],
                        i["last_contact_time"],
                        i["os"],
                        i["av_vdf_version"],
                        i["av_engine"]]
                        for i in response["results"]
                        ]
                if total_endpoints > req_rows:
                    pages = ceil(total_endpoints / req_rows)
                    for x in range(1, pages):
                        pd["start"] += req_rows
                        new = time.time()
                        r = self.csr[prod].request(f"/appservices/v6/orgs/{org_key}/devices/_search", pd=pd)
                        print(f"time to make inner request = {time.time() - new} - {inst_id}")
                        if not r:
                            return results
                        elif r.status_code == 200:
                            results += [
                                [inst_id,
                                i["id"],
                                i["sensor_version"],
                                i["deployment_type"],
                                i["os_version"],
                                i["organization_name"],
                                i["status"],
                                i["registered_time"],
                                i["organization_id"],
                                i["deregistered_time"],
                                i["last_reported_time"],
                                i["sensor_out_of_date"],
                                i["last_contact_time"],
                                i["os"],
                                i["av_vdf_version"],
                                i["av_engine"]]
                                for i in r.json()["results"]
                                ]
                print(len(results),  time.time() - start, results[0][0])
                return results
        query = "select distinct inst_id from endpoints;"
        already_inserted = [i[0] for i in self.db.execute(query)]
        query = 'select inst_id, prod, org_key from customers where org_key != "";'
        data = self.db.execute(query)
        needs = [row for row in data if row[0] not in already_inserted]
        fields = [
            "inst_id",
            "id",
            "sensor_version",
            "deployment_type",
            "os_version",
            "org_name",
            "status",
            "reg_time",
            "org_id",
            "dereg_time",
            "last_reported_time",
            "sensor_ood",
            "last_contact_time",
            "os",
            "av_vdf_version",
            "av_engine"
                ]
        insert_data = []
        def chunk(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i + n]
        chunks = chunk(needs, CONNECTIONS)
        for x, c in enumerate(chunks):
            with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
                future_to_url = {executor.submit(return_endpoints, r): r[0] for r in c}
                ct = 0
                for future in concurrent.futures.as_completed(future_to_url):
                    ct += 1
                    insert_data.extend(future.result())
                    print(f'result of insert ({future.result()[0][0]}) = {self.db.insert("endpoints", fields, future.result(), pk=False, del_table=False)}')
                    print(f"Endpoints: Just got back {ct} of {x} of {len(needs)}")
        print(f"time to get endpoints = {time.time() - start}")

    def get_alerts(self):
        def return_alerts(row, tries=3):
            inst_id, prod, org_key = row[0], row[1], row[2]
            print(f"alerts - {inst_id}")
            pd = {
                  "terms": {
                    "rows": 10,
                    "fields": [
                      "ALERT_TYPE",
                      "CATEGORY",
                      "DEVICE_NAME",
                      "APPLICATION_NAME",
                      "WORKFLOW",
                      "REPUTATION",
                      "RUN_STATE",
                      "POLICY_APPLIED",
                      "SENSOR_ACTION",
                      "POLICY_NAME",
                      "TAG"
                    ]
                  },
                  "query": "",
                  "criteria": {
                    "group_results": "False",
                    "minimum_severity": "1",
                    "target_value": [
                      "LOW",
                      "MEDIUM",
                      "HIGH",
                      "MISSION_CRITICAL"
                    ],
                    "category": [
                      "THREAT"
                    ],
                    "workflow": [
                      "OPEN",
                      "DISMISSED"
                    ],
                    "create_time": {
                      "range": "-1M"
                    }
                  }
                }
            r = self.csr[prod].request(f"/appservices/v6/orgs/{org_key}/alerts/_facet", pd=pd)
            if not r:
                # Request timed out
                return [[inst_id] + ["TO"] * 9]
            elif r.status_code != 200 and tries != 0:
                tries -= 1
                return return_alerts(row, tries=tries)
            elif r.status_code == 200 and len(r.content) < 10:
                # Most likely this installation has the wrong prod in sf
                # But sometimes something else happens that breaks the returned result from making it into futures.results()
                return [[inst_id] + ["wrong", "prod?", "check", "salesforce", "and", "csr", "", "", ""]]
            else:
                print(len(r.content))
                print(r.status_code)
                response = r.json()["results"]
                open_alerts, closed_alerts, terminated, denied, allow_log, ran, not_ran, policy_applied, policy_not_applied = 0,0,0,0,0,0,0,0,0

                for i in response:
                    if i["field"] == "workflow":
                        for status in i["values"]:
                            if status["id"] == "OPEN":
                                open_alerts = status["total"]
                            elif status["id"] == "DISMISSED":
                                closed_alerts = status["total"]
                    elif i["field"] == "sensor_action":
                        for status in i["values"]:
                            if status["id"] == "TERMINATE":
                                terminated = status["total"]
                            elif status["id"] == "DENY":
                                denied = status["total"]
                            elif status["id"] == "ALLOW_AND_LOG":
                                allow_log = status["total"]
                    elif i["field"] == "run_state":
                        for status in i["values"]:
                            if status["id"] == "RAN":
                                ran = status["total"]
                            elif status["id"] == "DID_NOT_RUN":
                                not_ran = status["total"]
                    elif i["field"] == "policy_applied":
                        for status in i["values"]:
                            if status["id"] == "APPLIED":
                                policy_applied = status["total"]
                            elif status["id"] == "NOT_APPLIED":
                                policy_not_applied = status["total"]
                results = [[inst_id, open_alerts, closed_alerts, terminated, denied, allow_log, ran, not_ran, policy_applied, policy_not_applied]]
                return results

        query = "select distinct inst_id from alerts;"
        already_inserted = [i[0] for i in self.db.execute(query)]
        query = "select inst_id, prod, org_key from customers order by inst_id;"
        data = self.db.execute(query)
        needs = [row for row in data if row[0] not in already_inserted]
        fields = [
                "inst_id",
                "open",
                "dismissed",
                "terminated",
                "denied",
                "allow_log",
                "ran",
                "not_ran",
                "policy_applied",
                "policy_not_applied"
                ]
        insert_data = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
            future_to_url = {executor.submit(return_alerts, r): r[0] for r in needs}
            ct = 0
            for future in concurrent.futures.as_completed(future_to_url):
                ct += 1
                iid = future_to_url[future]
                print(f"Alerts: just got back alerts #{ct} - {iid}")
                #insert_data.extend(future.result())
                self.db.insert("alerts", fields, future.result(), pk=False, del_table=False)

    def get_kits(self):
        CONNECTIONS = 1
        query = "select distinct prod from customers;"
        prods = [i[0] for i in self.db.execute(query)]
        url = "/appservices/v5/orgs/1/kits/published"
        all_rows = []
        for prod in prods:
            if prod in self.csr:
                r = self.csr[prod].request(url)
                if r:
                    kits = r.json()["publishedKits"]
                    rows = [[prod, os, v, kits[os][v][0]["hash"], kits[os][v][0]["createTime"], kits[os][v][0]["status"]] for os in kits for v in kits[os]]
                    all_rows.extend(rows)
        fields = ["backend", "os", "version", "hash", "create_time", "status"]
        self.db.insert("kits", fields, all_rows, del_table=True, pk=False)

    def get_connectors(self):
        def return_connectors(row, tries=3):
            inst_id, prod, org_id = row[0], row[1], row[2]
            pd = {
              "sortDefinition": {
                "fieldName": "TIME",
                "sortOrder": "ASC"
              },
              "searchWindow": "ALL",
              "fromRow": 1,
              "createdByLoginId": 0,
              "maxRows": 10000,
              "orgId": org_id
            }
            r = self.csr[prod].request(f"appservices/v5/orgs/{org_id}/connectors/find", pd=pd)
            if not r:
                return [[inst_id] + ["Failed"] * 10]
            elif r.status_code == 200:
                response = r.json()
            if len(response["entries"]) == 0:
                return [[inst_id] + [""] * 10]
            results = []
            for i in response["entries"]:
                if i["connectorType"] == "SIM":
                    if isinstance(i["notificationState"], dict):
                        if not i["notificationState"]["lastSimNotificationKey"]: 
                            return [[inst_id] + ["Failed"] * 10]
                        last_event = i["notificationState"]["lastSimNotificationKey"]["eventTime"] \
                                if i["notificationState"]["lastSimNotificationKey"]["eventTime"] else ""
                    else:
                        last_event = ""
                elif i["connectorType"] == "API":
                    if isinstance(i["notificationState"], dict) and isinstance(i["notificationState"]["lastAuditLogKey"], dict):
                        last_event = i["notificationState"]["lastAuditLogKey"]["eventTime"]
                    else:
                        last_event = ""
                elif i["connectorType"] == "CUSTOM":
                    last_event = ""
                else:
                    last_event = ""
                    with open("failure.txt", "a+") as f:
                        f.write(f"{inst_id}, {response}\n")
                last_report = i["stats"]["lastReportedTime"] if i["stats"] and "lastReportedTime" in i["stats"] else ""
                results.append([
                    inst_id,
                    i["createTime"],
                    i["connectorId"],
                    i["apiKey"],
                    i["lastUpdatedTime"],
                    i["orgId"],
                    i["connectorType"],
                    last_event,
                    last_report,
                    i["name"],
                    i["description"]])
            return results

        query = "select distinct inst_id from connectors;"
        already_inserted = [i[0] for i in self.db.execute(query)]
        query = "select inst_id, prod, org_id from customers order by inst_id;"
        data = self.db.execute(query)
        needs = [row for row in data if row[0] not in already_inserted]
        fields = [
                "inst_id",
                "create_time",
                "connector_id",
                "api_key",
                "last_updated",
                "org_id",
                "connector_type",
                "last_event",
                "last_report",
                "name",
                "description"
                ]
        with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
            future_to_url = {executor.submit(return_connectors, r): r[0] for r in needs}
            ct = 0
            for future in concurrent.futures.as_completed(future_to_url):
                ct += 1
                iid = future_to_url[future]
                print(f"Connectors: just got back #{ct} - {iid}")
                self.db.insert("connectors", fields, future.result(), pk=False, del_table=False)

    def get_forwarders(self):
        def return_forwarders(row, tries=3):
            inst_id, prod, org_key = row[0], row[1], row[2]
            r = self.csr[prod].request(f"/data_forwarder/v2/orgs/{org_key}/configs")
            if r.status_code == 200:
                response = r.json()
            elif r.status_code == 404:
                return [[inst_id] + [""] * 9]
            else:
                print(r.status_code)
                raise
            results = []
            for i in response:
                for nw in ("version_constraint", "current_version", "destination", "azure_container_name", "azure_tenant_id", "azure_storage_account"):
                    if nw in i:
                        del i[nw]
                #results.append([inst_id] + [v for v in i.values()])
                fields = ["id", "org_key", "name", "enabled", "s3_bucket_name"]
                fields += ["s3_prefix", "type", "create_time", "update_time"]
                results.append([inst_id] + [i.get(f, None) for f in fields])
            return results
        query = "select inst_id, prod, org_key from customers order by inst_id"
        needs = self.db.execute(query)
        fields = ["inst_id", "id", "org_key", "name", "enabled", "s3_bucket_name"]
        fields += ["s3_prefix", "type", "create_time", "update_time"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
            future_to_url = {executor.submit(return_forwarders, r): r[0] for r in needs}
            ct = 0
            for future in concurrent.futures.as_completed(future_to_url):
                ct += 1
                iid = future_to_url[future]
                print(f"Forwarders: just got back #{ct} - {iid}")
                self.db.insert("forwarders", fields, future.result(), pk=False, del_table=False)

    def get_dashboards(self):
        def return_dashboards(row, tries=3):
            inst_id, prod, org_id = row[0], row[1], row[2]
            pd = {
                "searchDefinition":
                {
                    "version": "1",
                    "searchWindow": "ONE_MONTH",
                    "dataGrouping": "NO_GROUP_RESULTS",
                    "criteria":
                    {
                        "SEVERITY":
                        [
                            "WARNING"
                        ],
                        "THREAT_SCORE":
                        [
                            "1"
                        ],
                        "DISMISSED":
                        [
                            "true",
                            "false"
                        ]
                    },
                    "orgId": 1035
                }}
            r = self.csr[prod].request(f"appservices/v5/orgs/{org_id}/dashboard/attacks-stopped", pd=pd)
            if not r:
                return [[inst_id] + ["Failed"] * 16]
            elif r.status_code == 200:
                response = r.json()
            if response["message"] != "success":
                return [[inst_id] + [""] * 16]

            threat_types = ["RISKY_PROGRAM", "NON_MALWARE", "UNKNOWN", "NEW_MALWARE", "KNOWN_MALWARE"]
            metrics = ["count", "percentage", "percentageChange"]
            results = [inst_id]
            for tt in threat_types:
                if tt in response["threatSummary"]["ATTACKS_STOPPED"]:
                    for m in metrics:
                        if m in response["threatSummary"]["ATTACKS_STOPPED"][tt]:
                            results.append(response["threatSummary"]["ATTACKS_STOPPED"][tt][m])
                        else:
                            results.append(0)
                else:
                    results.extend([0, 0, 0])
            return [results]

        query = "select distinct inst_id from dashboards;"
        already_inserted = [i[0] for i in self.db.execute(query)]
        query = 'select inst_id, prod, org_id from customers where org_key != "";'
        data = self.db.execute(query)
        needs = [row for row in data if row[0] not in already_inserted]
        fields = ["inst_id", "pup_count", "pup_perc", "pup_perc_change"]
        fields += ["non_malware_count", "non_malware_perc", "non_malware_perc_change"]
        fields += ["unknown_count", "unknown_perc", "unknown_perc_change"]
        fields += ["sus_malware_count", "sus_malware_perc", "sus_malware_perc_change"]
        fields += ["known_malware_count", "known_malware_perc", "known_malware_perc_change"]
        insert_data = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
            future_to_url = {executor.submit(return_dashboards, r): r[0] for r in needs}
            ct = 0
            for future in concurrent.futures.as_completed(future_to_url):
                ct += 1
                iid = future_to_url[future]
                print(f"Dashboards: just got back dashboards #{ct} - {iid}")
                #insert_data.extend(future.result())
                try:
                    self.db.insert("dashboards", fields, future.result(), pk=True, del_table=False)
                except sqlite3.ProgrammingError:
                    print(future.result())
                    print(fields)
                    raise

    def get_policy_ids(self):
        query = 'select prod, inst_id, org_id from customers;'
        data = self.db.execute(query, dict_simp=True)
        for prod in data:
            urls = [[inst_id, org_id, f"/appservices/v5/orgs/{org_id}/policies/summaries"] for inst_id, org_id in data[prod]]
            future_session = FuturesSession(
                executor=concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS), session=self.csr[prod].session
            )
            futures = [future_session.get(self.csr[prod].backend + url) for inst_id, org_id, url in urls] 
            c = 0
            for f in concurrent.futures.as_completed(futures):
                try:
                    r = f.result().json()
                except requests.exceptions.ConnectionError:
                    print("caught a connection error in policy_ids")
                    continue
                if "list" not in r:
                    c+=1
                    continue
                elif not r["list"]:
                    print("not list")
                    c+=1
                    continue
                org_id = str(r["list"][0]["orgId"])
                inst_id = [i[0] for i in data[prod] if i[1] == org_id][0]
                rows = [[inst_id, org_id, i["id"], i["name"], i["priority"], i["numDevices"]] for i in r["list"]]  
                fields = ["inst_id", "org_id", "policy_id", "policy_name", "priority", "num_devices"]
                self.db.insert("policy_ids", fields, rows, pk=False, del_table=False)

    def get_rules(self):
        query = "select prod, inst_id, org_id from customers;"
        data = self.db.execute(query, dict_simp=True)
        for prod in data:
            urls = []
            for inst_id, org_id in data[prod]:
                query = f"select policy_id from policy_ids where inst_id = '{inst_id}'"
                policy_ids = [i[0] for i in self.db.execute(query)]
                for pid in policy_ids:
                    urls.append([inst_id, org_id, f"/appservices/v5/orgs/{org_id}/policies/{pid}"])
            future_session = FuturesSession(
                executor=concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS), session=self.csr[prod].session
            )
            futures = [future_session.get(self.csr[prod].backend + url) for inst_id, org_id, url in urls] 
            for f in concurrent.futures.as_completed(futures):
                rows = []
                try:
                    r = f.result().json()
                except requests.exceptions.JSONDecodeError:
                    print(f.result().content)
                    print(f.result().request.url)
                    print(f.result().status_code)
                except requests.exceptions.ConnectionError:
                    print("caught a connection error")

                org_id = str(r["orgId"])
                inst_id = [i[0] for i in data[prod] if i[1] == org_id][0]
                name = r["name"]
                pol_id = r["id"]
                rules = r["policy"]["rules"]
                for rule in rules:
                    op = rule["operation"]
                    action = rule["action"]
                    r_type = rule["application"]["type"]
                    value = rule["application"]["value"]
                    rows.append([inst_id, pol_id, name, op, action, r_type, value])
                fields = ["inst_id", "policy_id", "rule_name", "operation", "action", "rule_type", "rule_definition"]
                self.db.insert("rules", fields, rows, pk=False)

    def get_everything(self):
        pass

if __name__ == "__main__":
    import db_connections
    from frontend import setup
    sfdb = db_connections.tesseract_connection()
    db = db_connections.sqlite_db("cua.db")
    csr, custs = setup(sfdb)
    test_run = csr_data(sfdb, db, csr, new_run=False)
    print("making customer table")
    #test_run.get_policy_ids()
    #test_run.get_rules()
    #test_run.get_dashboards()
    print("Getting em")
    test_run.get_forwarders()
    benjam
    test_run.get_endpoints()
    test_run.get_alerts()
    test_run.get_audit()
    test_run.get_kits()
    test_run.get_connectors()
