import db_connections
import re
from collections import defaultdict

class summary_data(object):
    def __init__(self, db):
        self.db = db
        self.inst_ids = [i[0] for i in self.db.execute("select inst_id from customers;")]

    def merge_dicts(self, d, d1):
            for k,v in d1.items():
                if k in d:
                    d[k].update(d1[k])
                else:
                    d[k] = d1[k]
            return d

    def endpoint_lookup(self):
        ''' Table of all versions and their current support status && availablilty'''
        query = "select distinct sensor_version, os from endpoints where sensor_version not in ('TO', 'No deployment', '') order by sensor_version;"
        all_versions = self.db.execute(query)

        # Available for download
        # Technically this should be done per prod.  But discrepancies will be small and syd/nrt don't allow kits access anyway
        available = self.db.execute("select os, version from kits;", dict=True)
        available["linux"] = available["ubuntu"]
        for x, row in enumerate(all_versions):
            v, os = row[0], row[1]
            if v in available[os.lower()]:
                all_versions[x].append("True")
            else:
                all_versions[x].append("False")

        # Support level
        lookup = self.db.execute("select os, version, current_level from version_support;", dict=True)
        def eolife(os, v):
            while len(v) > 0:
                if v[:-1] in lookup[os]:
                    return lookup[os][v[:-1]]
                else:
                    return eolife(os, v[:-1])
            return "EOL"

        for x, r in enumerate(all_versions):
            v, os = r[0], r[1][:3]
            all_versions[x].append(eolife(os, v))

        fields = ["version", "os", "dl_available", "support_level"]
        self.db.insert("sensor_lookup", fields, all_versions, pk=True, del_table=True)

    def direct_inserts(self):
        ''' Those fields thats are easily available from existing tables'''

        # Start with sf_data, which is basically everything
        query = "select * from sf_data;"
        data = self.db.execute(query)
        for x, r in enumerate(data):
            products = r[13].split(", ")
            products[:] = set([p.strip() for p in products])
            products = ", ".join(products)
            data[x][13] = products
        fields = ["inst_id", "Prod", "OrgID", "Account_Name", "ARR", "CSM", "CSE", "CSM_Role", "GS_Meter", "GS_Overall", "GS_Last_Updated"]
        fields += ["Account_ID", "Licenses", "Products", "ACV", "Opportunity_Ct", "Next_Renewal", "Next_Renewal_Qt"]
        self.db.insert("master", fields, data, del_table=True)

        # Everything in alerts table too
        query = "select * from alerts;"
        data = self.db.execute(query)
        fields = ["inst_id", "Open_Alerts", "Dismissed_Alerts", "Terminated_Alerts", "Denied_Alerts", "Allow_and_Log_Alerts"]
        fields += ["Ran_Alerts", "Not_Ran_Alerts", "Policy_Applied_Alerts", "Policy_Not_Applied_Alerts"]
        self.db.insert("master", fields, data)

        # A selection from data science table
        query = """
        select
        sf.inst_id,
        Predictive_Churn_Meter,
        ds.Account_Risk_Factors,
        ds.Intent___Cylance,
        ds.Intent___Crowdstrike,
        ds.Intent___Endgame,
        ds.Intent___Sentinelone,
        ds.Intent___Microsoft_Defender_ATP,
        ds.Searching_For_Solution,
        ds.Previous_Predictive_Churn_Meter,
        ds.Predictive_Churn_Meter_Changed,
        ds.Indicators_Changed,
        ds.MSSP
        from sf_data sf
        left join data_science ds on sf.account_id = ds.AccountSFID;
        """
        data = self.db.execute(query)
        fields = ["inst_id", "Predictive_Churn_Meter", "Account_Risk_Factors", "Intent___Cylance", "Intent___Crowdstrike"]
        fields += ["Intent___Endgame", "Intent___Sentinelone", "Intent___Microsoft_Defender_ATP", "Searching_For_Solution"]
        fields += ["Previous_Predictive_Churn_Meter", "Predictive_Churn_Meter_Changed", "Indicators_Changed", "MSSP"]
        self.db.insert("master", fields, data)

    def connector_inserts(self):
        ''' Mainly looking for integrations by parsing the connector names'''
        # SIEMs/ Integrations
        terms = [
                "arctic.*wolf",
                "splunk",
                "log.*rhythm",
                "q.*radar",
                "z.*scaler",
                "dmisto",
                "rapid.*7",
                "alien.*vault",
                "sentinel",
                "exabeam",
                "phantom",
                "axonius",
                "xsoar",
                "sumo",
                "elk",
                "elastic",
                "arc.*sight",
                "insight.*idr",
                "net.*skope",
                "trust.*wave",
                "proof.*point",
                "raytheon",
                "masergy"
                ]
        data = self.db.execute("select inst_id, name from connectors;")
        data_dict = defaultdict(list)
        for r in data:
            data_dict[r[0]].append(r[1])
        rows = []
        for x, inst_id in enumerate(list(data_dict)):
            rows.append([inst_id])
            for term in terms:
                for name in data_dict[inst_id]:
                    if re.search(term, name.lower()):
                        rows[x].append(term.replace(".*", ""))
        # Remove dupes and convert list of terms to a comma separated string
        rows = [[r[0]] +  [", ".join(set(r[1:]))] for r in rows]
        fields = ["inst_id", "Integrations"]
        self.db.insert("master", fields, rows)

    def audit_log_inserts(self):
        ''' Audit log has info on logins, bypass, policy-, and user-adds'''
        # Login counts
        query = """
        select
        inst_id,
        SUM(CASE WHEN description like 'log%in success%' and description not like 'Connector%' THEN 1 ELSE 0 END)
        from audit
        where datetime(event_time /1000, 'unixepoch') > datetime('now', '-30 day')
        group by inst_id;
        """
        data = self.db.execute(query)
        fields = ["inst_id", "Last_30d_Login_Count"]
        self.db.insert("master", fields, data)

        # Login Metadata
        query = """
        select inst_id, max(event_time), (strftime('%s', 'now') - max(event_time) / 1000) / 86400
        from audit
        where description like 'log%in success%'
        and description not like 'Connector%'
        group by inst_id;
        """
        data = self.db.execute(query)
        fields = ["inst_id", "Last_Login", "Days_Since_Login"]
        self.db.insert("master", fields, data)

        # Connector login counts
        query = """
        select inst_id,
        SUM(CASE WHEN description like 'Connector % logged in successfully' THEN 1 ELSE 0 END)
        from audit
        where datetime(event_time /1000, 'unixepoch') > datetime('now', '-30 day')
        group by inst_id;
        """
        data = self.db.execute(query)
        fields = ["inst_id", "Last_30d_Connector_Count"]
        self.db.insert("master", fields, data)

        # Bypass counts
        query = """
        select inst_id,
        SUM(CASE
        WHEN (description like '%all'
            and description like '%bypass%'
            and (description like '%enabled%' or description like '%to on %')
            and description not like '%Action)')
        THEN 1000000
        WHEN (description not like '%all'
            and description like '%bypass%'
            and (description like '%enabled%' or description like '%to on %')
            and description not like '%Action)')
        THEN (LENGTH(description) - LENGTH(REPLACE(description, ',', '')) +1)
        ELSE 0
        END)
        from audit
        where datetime(event_time /1000, 'unixepoch') > datetime('now', '-30 day')
        group by inst_id;
        """
        data = self.db.execute(query)
        for x, r in enumerate(data):
            if r[1] >= 1000000:
                data[x][1] = "All"
        fields = ["inst_id", "Last_30d_Bypass_Count"]
        self.db.insert("master", fields, data)

        # Dates for last polcy created/modified and user added
        searches = [
                ["Last_Created_Policy", "%Created policy%"],
                ["Last_Modified_Policy", "%Policy% was modified%"],
                ["Last_Added_User",  "%Added user%"]
                ]
        for si in searches:
            query = f"""
            select inst_id, max(event_time)
            from audit
            where description like '{si[1]}'
            group by inst_id;
            """
            data = self.db.execute(query)
            fields = ["inst_id", si[0]]
            self.db.insert("master", fields, data)

    def endpoint_inserts(self):
        ''' Endpoint counts broken in several ways ie deployed, bypass, eol, etc'''
        # Deployed total
        query = """
        select e.inst_id,
        SUM(CASE WHEN e.status in ('REGISTERED', 'BYPASS') THEN 1 ELSE 0 END),
        ROUND(SUM(CASE WHEN e.status in ('REGISTERED', 'BYPASS') THEN 1 ELSE 0 END) * 1.0 / sf.licenses_purchased * 100, 2)
        from endpoints e
        left join sf_data sf on e.inst_id = sf.inst_id
        where e.last_contact_time > datetime('now', '-30 day')
        group by e.inst_id;
        """
        data = self.db.execute(query)
        fields = ["inst_id", "Deployment", "Deployment_Perc"]
        self.db.insert("master", fields, data)

        # Bypass counts
        query = """
        select e.inst_id,
        SUM(CASE when e.status like 'BYPASS' THEN 1 ELSE 0 END),
        round(SUM(CASE when e.status like 'BYPASS' THEN 1 ELSE 0 END) * 1.0 / sf.licenses_purchased * 100, 2)
        from endpoints e
        left join sf_data sf on e.inst_id = sf.inst_id
        where e.last_contact_time > datetime('now', '-30 day')
        group by e.inst_id;
        """
        data = self.db.execute(query)
        fields = ["inst_id", "Bypass", "Bypass_Perc"]
        self.db.insert("master", fields, data)

        # Sensor Support level & DL Availability
        query = f"""
        select
        e.inst_id,
        SUM(CASE sl.support_level WHEN "ST" THEN 1 ELSE 0 END),
        round(SUM(CASE sl.support_level WHEN "ST" THEN 1 ELSE 0 END) * 1.0 / count(e.sensor_version) * 100, 2),
        SUM(CASE sl.support_level WHEN "EX" THEN 1 ELSE 0 END),
        round(SUM(CASE sl.support_level WHEN "EX" THEN 1 ELSE 0 END) * 1.0 / count(e.sensor_version) * 100, 2),
        SUM(CASE sl.support_level WHEN "EOL" THEN 1 ELSE 0 END),
        round(SUM(CASE sl.support_level WHEN "EOL" THEN 1 ELSE 0 END) * 1.0 / count(e.sensor_version) * 100, 2),
        SUM(CASE sl.dl_available WHEN "False" THEN 1 ELSE 0 END),
        round(SUM(CASE sl.dl_available WHEN "False" THEN 1 ELSE 0 END) * 1.0 / count(e.sensor_version) * 100, 2)
        from endpoints e
        left join sensor_lookup sl on e.sensor_version = sl.version
        where e.last_contact_time > datetime('now', '-30 day')
        and e.status in ('REGISTERED', 'BYPASS')
        group by e.inst_id;
        """
        data = self.db.execute(query)
        fields = ["inst_id", "Sensor_Standard_Support", "Standard_Perc", "Sensor_Extended_Support", "Extended_Perc"]
        fields += ["Sensor_EOL_Support", "EOL_Perc", "Sensor_Download_Unavailable", "Download_Unavailable_perc"]
        self.db.insert("master", fields, data)

    def cua_brag(self):
        ''' Set of rules to evaluate the health of each customer based on the cua data'''
        # Get all the inst_ids & make a dict out of them
        cua = {}
        data = [i[0] for i in self.db.execute("select inst_id from master;")]
        for inst_id in data:
            cua[inst_id] = []

        # Days since last login
        query = "select inst_id from master where cast(Days_Since_Login as real) > 15;"
        data = self.db.execute(query)
        for row in data:
            cua[row[0]].append("> 15 days since login")

        # Days since last login
        query = "select inst_id from master where cast(Last_30d_Login_Count as real) > 500;"
        data = self.db.execute(query)
        for row in data:
            cua[row[0]].append("> 500 logins last 30d")

        # Bypass > 5%
        query = "select inst_id from master where cast(bypass_perc as real) >= 5.0;"
        data = self.db.execute(query)
        for row in data:
            cua[row[0]].append(">5% in Bypass")

        # Bypass > 50 endpoints
        query = "select inst_id from master where cast(bypass as real) >= 50;"
        data = self.db.execute(query)
        for row in data:
            cua[row[0]].append(">50 in Bypass")

        # Bypass used in last 30d > 10
        query = "select inst_id from master where cast(Last_30d_Bypass_Count as real) > 10;"
        data = self.db.execute(query)
        for row in data:
            cua[row[0]].append("> 10 bypass use last 30d")

        # EOL > 50 endpoints
        query = "select inst_id from master where cast(Sensor_EOL_Support as real)>= 50;"
        data = self.db.execute(query)
        for row in data:
            cua[row[0]].append(">50 in EOL")

        # Open Alerts > 10k
        query = "select inst_id from master where cast(Open_Alerts as real) >= 10000;"
        data = self.db.execute(query)
        for row in data:
            cua[row[0]].append(">10k alerts open")

        # > 3 alerts per endpoint
        query = "select inst_id from master where (cast(Open_Alerts as real) + cast(Dismissed_Alerts as real)) / (cast(Deployment as real) * 1.0) >= 3.0;"
        data = self.db.execute(query)
        for row in data:
            cua[row[0]].append(">=3 alerts per endpoint")

        # Deployment < 75%
        query = "select inst_id from master where cast(Deployment_Perc as real) <= 75;"
        data = self.db.execute(query)
        for row in data:
            cua[row[0]].append("Deployment < 75%")

        # No Policy mods or creates in 90d
        # These dates are stored in epoch(ms), 90d = 7.776e+8
        ms_ago = 90 * 24 * 60 * 60 * 1000
        query = f"""
        select
        inst_id, Last_Created_Policy
        from master
        where cast(Last_Created_Policy as real) < (strftime('%s', 'now') * 1000 - {ms_ago})
        and cast(Last_Modified_Policy as real) < (strftime('%s', 'now') * 1000 - {ms_ago})
        and cast(Last_Added_User as real) < (strftime('%s', 'now') * 1000 - {ms_ago});
        """
        data = self.db.execute(query)
        for row in data:
            cua[row[0]].append("no policy add/mod or user add in 90d")

        # Flatten the dict into a list, add count of violation, add cua status in one go
        def get_color(count):
            if count < 2:
                return "Green"
            elif 1 < count < 4:
                return "Yellow"
            elif count >= 4:
                return "Red"
        data = [[inst_id, ", ".join(cua[inst_id]), len(cua[inst_id]), get_color(len(cua[inst_id]))] for inst_id in cua]

        # Insert the whole deal
        fields = ["inst_id", "Violations_Triggered", "Count_of_Violations", "CUA_Brag"]
        self.db.insert("master", fields, data)

    def sensor_versions(self):
        query = """
        select
        e.inst_id,
        e.sensor_version || " (" || sl.os || ") " || "(" || sl.dl_available || ") " || "("  || sl.support_level || ")", count(e.sensor_version)
        from endpoints e
        left join sensor_lookup sl on e.sensor_version = sl.version
        where e.last_contact_time > datetime('now', '-30 day')
        and sensor_version <> 'No deployment'
        and e.status in ('REGISTERED', 'BYPASS')
        group by e.inst_id, e.sensor_version, sl.os, sl.dl_available, sl.support_level;
        """
        ep_data = self.db.execute(query, dict=True)
        # Fix up the column names
        for inst_id in ep_data:
            for v in list(ep_data[inst_id]):
                new_header = v.replace("(true) ", "").replace("(false)", "(Unavailable)")
                ep_data[inst_id][new_header] = ep_data[inst_id].pop(v)
        # Make all inst_ids the same length, ie put a 0 for any versions that dont appear in the installation
        all_versions = list(set([v for inst_id in ep_data for v in ep_data[inst_id]]))
        all_versions.sort()
        for v in all_versions:
            for inst_id in ep_data:
                ep_data[inst_id][v] = ep_data[inst_id].get(v, 0)
        # Sort & Flatten
        data = []
        for inst_id in ep_data:
            data.append([inst_id] + [ep_data[inst_id][v] for v in all_versions])
        fields = ["inst_id"] + [f"{v}" for v in all_versions]
        self.db.insert("sensor_versions_summary", fields, data, del_table=True, pk=True)

    def os_versions(self):
        query = """
        select e.inst_id, e.os_version, count(e.os_version)
        from endpoints e
        where e.os_version <> "No deployment"
        and e.last_contact_time > datetime('now', '-30 day')
        and e.status in ('REGISTERED', 'BYPASS')
        group by e.inst_id, e.os_version;
        """
        data = self.db.execute(query, dict=True)
        # Make all inst_ids the same length, ie put a 0 for any versions that dont appear in the installation
        all_versions = list(set([v for inst_id in data for v in data[inst_id]]))
        all_versions.sort()
        for v in all_versions:
            for inst_id in data:
                data[inst_id][v] = data[inst_id].get(v, 0)
        # Sort & Flatten
        rows = [[inst_id] + [data[inst_id][v] for v in all_versions] for inst_id in data]
        fields = ["inst_id"] + [v for v in all_versions]
        self.db.insert("os_versions_summary", fields, rows, del_table=True, pk=True)

    def deployment_summary(self):
        # OS Versions
        query = """
        select e.inst_id, e.os, count(e.os)
        from endpoints e
        where e.os <> "No deployment"
        and e.last_contact_time > datetime('now', '-30 day')
        and e.status in ('REGISTERED', 'BYPASS')
        group by e.inst_id, e.os;
        """
        os = self.db.execute(query, dict=True)

        # Sensor Families
        query = """
        select inst_id, substr(e.sensor_version, 0, 4) as fam, count(*)
        from endpoints e
        where e.os <> "No deployment"
        and e.sensor_version <> ""
        and e.last_contact_time > datetime('now', '-30 day')
        and e.status in ('REGISTERED', 'BYPASS')
        group by inst_id, fam;
        """
        os_fam = self.db.execute(query, dict=True)
        data = self.merge_dicts(os, os_fam)

        # Sensor support level
        query = """
        select
        e.inst_id,
        sl.support_level,
        count(*)
        from endpoints e
        left join sensor_lookup sl on e.sensor_version = sl.version
        where e.sensor_version <> ""
        and e.last_contact_time > datetime('now', '-30 day')
        and e.status in ('REGISTERED', 'BYPASS')
        group by inst_id, sl.support_level;
        """
        supp_lvl = self.db.execute(query, dict=True)
        data = self.merge_dicts(data, supp_lvl)

        # Account metadata
        query = "select inst_id, CSM, ARR, Licenses, Deployment from master;"
        sf = self.db.execute(query)
        sf_dict = {}
        for r in sf:
            sf_dict[r[0]] = {}
            sf_dict[r[0]]["CSM"] = r[1]
            sf_dict[r[0]]["ARR"] = r[2]
            sf_dict[r[0]]["Licenses"] = r[3]
            sf_dict[r[0]]["Deployment"] = r[4]
        data = self.merge_dicts(data, sf_dict)

        # Make all inst_ids the same length
        all_keys = list(set([k for inst_id in data for k in data[inst_id]]))
        for k in all_keys:
            for inst_id in data:
                data[inst_id][k] = data[inst_id].get(k, 0)
        # Sort & Flatten
        rows = [[inst_id] + [data[inst_id][k] for k in all_keys] for inst_id in data]
        fields = ["inst_id"] + all_keys
        self.db.insert("deployment_summary", fields, rows, del_table=True)

    def master_archive(self):
        # Delete any entries in the master archive from today
        self.db.execute("delete from master_archive where date = date();")
        # Check for new columns in master not in the master archive
        m_cols = [i[1] for i in self.db.execute("pragma table_info(master)")]
        ma_cols = [i[1] for i in self.db.execute("pragma table_info(master_archive)")]
        new_cols = [[x, i] for x, i in enumerate(m_cols) if i not in ma_cols]
        # Get everything from master_archive and add any new columns
        ma = self.db.execute("select * from master_archive;")
        for xx, i in enumerate(ma):
            for x, col in new_cols:
                ma[xx].insert(x+2, "")
        query = "select date() || inst_id, date(), * from master;"
        data = ma + self.db.execute(query)
        fields = ["Unique_id", "Date"] + [i[1] for i in self.db.execute("pragma table_info(master);")]
        self.db.insert("master_archive", fields, data, del_table=True, pk=True, update=False)

    def prod_deployment_trend(self):
        # Delete any data from today
        self.db.execute("delete from deployment_trend where date = date();")
        # Deployment by prod
        query = """
        select
        sf.backend,
        e.sensor_version,
        count(e.sensor_version) as "Version_Count"
        from endpoints e
        left join sf_data sf on e.inst_id = sf.inst_id
        where e.last_contact_time > datetime('now', '-30 day')
        and e.sensor_version <> 'No deployment'
        and e.status in ('REGISTERED', 'BYPASS')
        group by sf.backend, e.sensor_version;
        """
        data = self.db.execute(query, dict=True)
        date = self.db.execute("select date();")[0][0]
        all_versions = set([v for prod in data for v in data[prod]])

        # Sum up all of them
        for v in all_versions:
            total = sum([data[prod][v] for prod in data])
            data["all"][v] = total

        for prod in data:
            if not prod: continue
            fields = ["unique_id", "backend", "date"] + list(data[prod].keys())
            rows = [[prod+date, prod, date] +  list(data[prod].values())]
            self.db.insert("deployment_trend", fields, rows, update=False)

if __name__ == "__main__":
    db = db_connections.sqlite_db("cua.db")
    report = summary_data(db)
    report.endpoint_lookup()
    report.direct_inserts()
    report.audit_log_inserts()
    report.connector_inserts()
    report.endpoint_inserts()
    report.cua_brag()
    report.os_versions()
    report.deployment_summary()
    report.master_archive()
    report.prod_deployment_trend()
