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
        query = """
        select distinct sensor_version,
        os
        from endpoints
        where sensor_version not in ('TO', 'No deployment', '')
        order by sensor_version;
        """
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
        for os in list(lookup):
            for v in list(lookup[os]):
                if ".x" in v:
                    lookup[os][v.replace(".x", "")] = lookup[os].pop(v)
        lookup["WIN"] = lookup.pop("psc_win")
        lookup["MAC"] = lookup.pop("psc_mac")
        lookup["LIN"] = lookup.pop("psc_lin")
        def eolife(os, v):
            os = os.replace("psc_", "").upper()
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
            products = r[14].split(", ") if r[14] else ["None"]
            products[:] = set([p.strip() for p in products])
            products = ", ".join(products)
            data[x][14] = products
        fields = ["inst_id", "Prod", "OrgID", "Account_Name", "ARR", "CSM", "CSE", "CSM_Role", "GS_Meter", "GS_Overall"]
        fields += ["GS_Last_Updated", "Account_ID", "CS_Tier", "Prev_CS_Tier", "Licenses"]
        fields += ["account__c", "created_date", "days_to_50perc", "Products", "ACV", "Opportunity_Ct"]
        fields += ["Next_Renewal", "Next_Renewal_Qt", "total_cases_30d", "cbc_cases_30d", "open_cases", "open_cbc_cases"]
        fields += ["Last_CUA_CTA", "CUA_Status", "Last_TA", "Last_WB"]
        self.db.insert("master", fields, data, del_table=True)

        # Everything in alerts table too
        query = "select * from alerts;"
        data = self.db.execute(query)
        fields = ["inst_id", "Open_Alerts", "Dismissed_Alerts", "Terminated_Alerts", "Denied_Alerts", "Allow_and_Log_Alerts"]
        fields += ["Ran_Alerts", "Not_Ran_Alerts", "Policy_Applied_Alerts", "Policy_Not_Applied_Alerts"]
        self.db.insert("master", fields, data)

        '''
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
        '''

    def cse_activity_inserts(self):
        ''' cse timeline activities '''
        lookup = self.db.execute("select account_name, inst_id from master;", dict=True)
        data = self.db.execute("select account, max(activity_date) from cse_activity group by account;")
        rows = []
        for acct, act_date in data:
            for inst_id in lookup[acct.lower()]:
                rows.append([inst_id, act_date])
        fields = ["inst_id", "last_cse_timeline"]
        self.db.insert("master", fields, rows)

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
        #data = self.db.execute("select inst_id, name from forwarders UNION select inst_id, name from connectors;")
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
        query = "select max(event_time) from audit where event_time <> 'csr';"
        max_time = int(self.db.execute(query)[0][0])
        # 30 days before the max
        diff = max_time - 30 * 24 * 60 * 60 * 1000

        query = f"""
        select inst_id,
        SUM(CASE WHEN description like 'Connector % logged in successfully' THEN 1 ELSE 0 END)
        from audit
        where event_time >= {diff}
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
        where e.last_contact_time >= (select max(date(last_contact_time, '-30 days')) from endpoints)
        group by e.inst_id;
        """
        data = self.db.execute(query)
        fields = ["inst_id", "Deployment", "Deployment_Perc"]
        self.db.insert("master", fields, data)

        # Workloads deployed
        query = """
        select e.inst_id,
        count(*)
        from endpoints e
        where e.last_contact_time >= (select max(date(last_contact_time, '-30 days')) from endpoints)
        and e.status in ('REGISTERED', 'BYPASS')
        and deployment_type = 'WORKLOAD'
        group by e.inst_id;
        """
        data = self.db.execute(query)
        fields = ["inst_id", "Workload_Deployment"]
        self.db.insert("master", fields, data)

        # Deployed, last 24 hours and 7 days
        query = """
        select e.inst_id,
        SUM(CASE WHEN e.status in ('REGISTERED', 'BYPASS') and e.last_contact_time >= (select max(date(last_contact_time, '-1 days')) from endpoints) THEN 1 ELSE 0 END),
        SUM(CASE WHEN e.status in ('REGISTERED', 'BYPASS') and e.last_contact_time >= (select max(date(last_contact_time, '-7 days')) from endpoints) THEN 1 ELSE 0 END)
        from endpoints e
        where deployment_type != 'WORKLOAD'
        group by e.inst_id;
        """
        data = self.db.execute(query)
        fields = ["inst_id", "Last_24_contact", "Last_7d_contact"]
        self.db.insert("master", fields, data)

        # Bypass counts
        query = """
        select e.inst_id,
        SUM(CASE when e.status like 'BYPASS' THEN 1 ELSE 0 END) ,
        round(
            SUM(CASE when e.status like 'BYPASS' THEN 1 ELSE 0 END) * 1.0 /
            SUM(CASE WHEN e.status in ('REGISTERED', 'BYPASS') THEN 1 ELSE 0 END)
        * 100,
        2)
        from endpoints e
        where e.last_contact_time >= (select max(date(last_contact_time, '-30 days')) from endpoints)
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
        where e.last_contact_time >= (select max(date(last_contact_time, '-30 days')) from endpoints)
        and e.status in ('REGISTERED', 'BYPASS')
        group by e.inst_id;
        """
        data = self.db.execute(query)
        fields = ["inst_id", "Sensor_Standard_Support", "Standard_Perc", "Sensor_Extended_Support", "Extended_Perc"]
        fields += ["Sensor_EOL_Support", "EOL_Perc", "Sensor_Download_Unavailable", "Download_Unavailable_perc"]
        self.db.insert("master", fields, data)

    def cua_brag(self, table):
        ''' Set of rules to evaluate the health of each customer based on the cua data'''
        def rule_eval(query, name, score):
            data = self.db.execute(query)
            for row in data:
                cua[row[0]][0].append(name)
                cua[row[0]][1] += score

        # Get all the primary keys & make a dict out of them
        if table == "master":
            pk = "inst_id"
        elif table == "account_master":
            pk = "Account_Name"
        cua = {}
        data = [i[0] for i in self.db.execute(f"select {pk} from {table};")]
        for inst_id in data:
            cua[inst_id] = [[], 0]

        # Days since last login
        name = "> 7 days since login"
        score = 1
        query = f"select {pk} from {table} where cast(Days_Since_Login as real) > 7;"
        rule_eval(query, name, score)

        name = "> 15 days since login"
        score = 2
        query = f"select {pk} from {table} where cast(Days_Since_Login as real) > 15;"
        rule_eval(query, name, score)

        name = "> 15 days since login"
        score = 2
        query = f"select {pk} from {table} where cast(Days_Since_Login as real) > 30;"
        rule_eval(query, name, score)

        # Bypass Use
        name = ">5% in Bypass"
        score = 2
        query = f"select {pk} from {table} where cast(bypass_perc as real) >= 5.0;"
        rule_eval(query, name, score)

        name = ">10% in Bypass"
        score = 3
        query = f"select {pk} from {table} where cast(bypass_perc as real) >= 10.0;"
        rule_eval(query, name, score)

        name = ">20% in Bypass"
        score = 4
        query = f"select {pk} from {table} where cast(bypass_perc as real) >= 20.0;"
        rule_eval(query, name, score)

        name = ">25 in Bypass"
        score = 1
        query = f"select {pk} from {table} where cast(bypass as real) >= 25;"
        rule_eval(query, name, score)

        name = ">50 in Bypass"
        score = 1
        query = f"select {pk} from {table} where cast(bypass as real) >= 50;"
        rule_eval(query, name, score)

        name = "> 10 bypass use last 30d"
        score = 2
        query = f"select {pk} from {table} where cast(Last_30d_Bypass_Count as real) > 10;"
        rule_eval(query, name, score)

        # EOL Sensors
        name = ">50 in EOL"
        score = 1
        query = f"select {pk} from {table} where cast(Sensor_EOL_Support as real)>= 50;"
        rule_eval(query, name, score)

        name = ">10% in EOL"
        score = 1
        query = f"select {pk} from {table} where cast(eol_perc as real) >= 10.0;"
        rule_eval(query, name, score)

        name = ">25% in EOL"
        score = 2
        query = f"select {pk} from {table} where cast(eol_perc as real) >= 25.0;"
        rule_eval(query, name, score)

        # Alerts
        name = ">10k alerts open"
        score = 1
        query = f"select {pk} from {table} where cast(Open_Alerts as real) >= 10000;"
        rule_eval(query, name, score)

        name = ">=3 alerts per endpoint"
        score = 2
        query = f"""
        select {pk}
        from {table}
        where (cast(Open_Alerts as real) + cast(Dismissed_Alerts as real)) / (cast(Deployment as real) * 1.0) >= 3.0;
        """
        rule_eval(query, name, score)

        name = "Alert count too low for number of endpoints"
        score = 2
        query = f"""
        select
        {pk}
        from {table}
        where (cast(open_alerts as real) + cast(dismissed_alerts as real)) / cast(deployment as real) < .33;
        """
        rule_eval(query, name, score)

        # Deployment
        name = "Deployment < 50%"
        score = 2
        query = f"select {pk} from {table} where cast(Deployment_Perc as real) <= 50;"
        rule_eval(query, name, score)

        name = "Deployment < 75%"
        score = 1
        query = f"select {pk} from {table} where cast(Deployment_Perc as real) <= 75;"
        rule_eval(query, name, score)

        # Policy changes
        # These dates are stored in epoch(ms), 90d = 7.776e+8
        ms_ago_90d = 90 * 24 * 60 * 60 * 1000
        ms_ago_180d = 180 * 24 * 60 * 60 * 1000

        name = "no policy add/mod in 90d"
        score = 1
        query = f"""
        select
        {pk}
        from {table}
        where cast(Last_Created_Policy as real) < (strftime('%s', 'now') * 1000 - {ms_ago_90d})
        and cast(Last_Modified_Policy as real) < (strftime('%s', 'now') * 1000 - {ms_ago_90d});
        """
        rule_eval(query, name, score)

        name = "no policy add/mod in 180d"
        score = 1
        query = f"""
        select
        {pk}
        from {table}
        where cast(Last_Created_Policy as real) < (strftime('%s', 'now') * 1000 - {ms_ago_180d})
        and cast(Last_Modified_Policy as real) < (strftime('%s', 'now') * 1000 - {ms_ago_180d});
        """
        rule_eval(query, name, score)

        # Users
        name = "no user add in 90d"
        score = 1
        query = f"""
        select
        {pk}
        from {table}
        where cast(Last_Added_User as real) < (strftime('%s', 'now') * 1000 - {ms_ago_90d});
        """
        rule_eval(query, name, score)

        # DS
        '''
        name = "DS evaluates to red"
        score = 1
        query = f"""
        select m.{pk}
        from {table} m
        left join data_science ds on m.Account_ID = ds.AccountSFID
        where ds.Predictive_Churn_Meter = 'Red';
        """
        rule_eval(query, name, score)
        '''

        # Deployment swings
        name = ">10% in deployment counts"
        score = 3
        query = f"""
        select m.{pk},
        cast(m.deployment_perc as real) - cast(ma.deployment_perc as real)
        from {table} m
        left join {table}_archive ma on m.inst_id = ma.inst_id
        where ma.date = (select max(date) from {table}_archive);
        """

        # Flatten the dict into a list, add count of violation, add cua status in one go
        def get_color(count):
            if count <= 3:
                return "Green"
            elif count <= 9:
                return "Yellow"
            elif count >= 10:
                return "Red"
        data = [[inst_id, ", ".join(cua[inst_id][0]), cua[inst_id][1], get_color(cua[inst_id][1])] for inst_id in cua]

        # Insert the whole deal
        fields = [pk, "Violations_Triggered", "Count_of_Violations", "CUA_Brag"]
        self.db.insert(f"{table}", fields, data)

    def changes_over_time(self, table):
        ''' Look for things in our own evalutaion that should be flagged '''
        if table == "account_master":
            pk = "Account_Name"
            query = "select account_name, 'NA' from account_master;"
        elif table == "master":
            pk = "inst_id"
            query = """
            select m.inst_id,
            case when m.cua_brag in ('Yellow', 'Red') and ma.cua_brag like 'green' then 'True'
            when m.cua_brag like 'Red' and ma.cua_brag like 'yellow' then 'True'
            else 'False' end as "decresing_cua"
            from master m
            join master_archive ma on m.inst_id = ma.inst_id
            and ma.date = (select max(date) from master_archive)
            group by m.inst_id;
            """
        fields = [pk, "brag_decrease"]
        data = self.db.execute(query)
        if data: self.db.insert(table, fields, data)

    def sensor_versions(self):
        query = """
        select
        e.inst_id,
        e.sensor_version || " (" || sl.os || ") " || "(" || sl.dl_available || ") " || "("  || sl.support_level || ")", count(e.sensor_version)
        from endpoints e
        left join sensor_lookup sl on e.sensor_version = sl.version
        where e.last_contact_time >= (select max(date(last_contact_time, '-30 days')) from endpoints)
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
        and e.last_contact_time >= (select max(date(last_contact_time, '-30 days')) from endpoints)
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
        and e.last_contact_time >= (select max(date(last_contact_time, '-30 days')) from endpoints)
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
        and e.last_contact_time >= (select max(date(last_contact_time, '-30 days')) from endpoints)
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
        and e.last_contact_time >= (select max(date(last_contact_time, '-30 days')) from endpoints)
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

    def master_archive(self, ma_type):
        if ma_type == "installation":
            master_table, ma_table = "master", "master_archive"
        elif ma_type == "account":
            master_table, ma_table = "account_master", "account_master_archive"
        # Delete any entries in the master archive from today
        self.db.execute(f"delete from {ma_table} where date = date();")
        # Check for new columns in master not in the master archive
        m_cols = [i[1] for i in self.db.execute(f"pragma table_info({master_table})")]
        ma_cols = [i[1] for i in self.db.execute(f"pragma table_info({ma_table})")]
        new_cols = [[x, i] for x, i in enumerate(m_cols) if i not in ma_cols]
        # Get everything from master_archive and add any new columns
        ma = self.db.execute(f"select * from {ma_table};")
        for xx, i in enumerate(ma):
            for x, col in new_cols:
                ma[xx].insert(x+2, "")
        query = f"select date() || inst_id, date(), * from {master_table};"
        data = ma + self.db.execute(query)
        fields = ["Unique_id", "Date"] + [i[1] for i in self.db.execute(f"pragma table_info({master_table});")]
        self.db.insert(f"{ma_table}", fields, data, del_table=True, pk=True, update=False)

    def deployment_archive(self):
        query = """
        select
        date('now'),
        e.inst_id,
        e.os,
        sum(case when sl.support_level = "ST" then 1 else 0 end) as st,
        sum(case when sl.support_level = "EX" then 1 else 0 end) as ex,
        sum(case when sl.support_level = "EOL" then 1 else 0 end) as eol,
        count(*) as total
        from endpoints e
        left join sensor_lookup sl on e.sensor_version = sl.version
        where e.last_contact_time > datetime('now', '-30 day')
        group by e.inst_id, e.os;
        """
        data = self.db.execute(query)
        fields = ["date", "inst_id", "os", "standard", "extended", "eol", "total"]
        self.db.insert("deployment_archive", fields, data, del_table=False, pk=False, update=False)

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
        where e.last_contact_time >= (select max(date(last_contact_time, '-30 days')) from endpoints)
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

    def acct_rollup(self):
        query = "select Account_Name,"
        query += "max(csm),"
        query += "max(cse),"
        query += "max(csm_role),"
        query += "max(cs_tier),"
        query += "max(cast(arr as real)),"
        query += "max(cast(acv as real)),"
        query += "max(cast(Opportunity_Ct as int)),"
        query += "max(products),"
        query += "max(next_renewal),"
        query += "max(next_renewal_qt),"
        query += "max(cast(gs_meter as int)),"
        query += "max(cast(gs_overall as int)),"
        query += "max(gs_last_updated),"
        query += "max(last_cua_cta),"
        query += "max(cua_status),"
        query += "max(last_ta),"
        query += "max(last_wb),"
        query += "max(last_cse_timeline),"
        query += "max(last_login),"
        query += "min(cast(days_since_login as int)),"
        query += "sum(last_30d_login_count),"
        query += "sum(Last_30d_Connector_Count),"
        query += "group_concat(integrations),"
        query += "min(last_added_user),"
        query += "min(Last_Created_Policy),"
        query += "min(last_modified_policy),"
        query += "max(created_date),"
        query += "max(days_to_50perc),"
        query += "max(licenses),"
        query += "sum(deployment),"
        query += "round(sum(cast(deployment as real)) / sum(licenses) * 100, 2),"
        query += "sum(last_24_contact),"
        query += "sum(last_7d_contact),"
        query += "sum(workload_deployment),"
        query += "sum(bypass),"
        query += "round(sum(cast(bypass as real)) / sum(deployment) * 100, 2),"
        query += "sum(Last_30d_Bypass_Count),"
        query += "sum(Sensor_Download_Unavailable),"
        query += "round(sum(cast(sensor_download_unavailable as real)) / sum(deployment) * 100, 2),"
        query += "sum(Sensor_Standard_Support),"
        query += "round(sum(cast(sensor_standard_support as real)) / sum(deployment) * 100, 2),"
        query += "sum(Sensor_Extended_Support),"
        query += "round(sum(cast(sensor_extended_support as real)) / sum(deployment) * 100, 2),"
        query += "sum(Sensor_EOL_Support),"
        query += "round(sum(cast(sensor_eol_support as real)) / sum(deployment) * 100, 2),"
        query += "sum(total_cases_30d),"
        query += "sum(cbc_cases_30d),"
        query += "sum(open_cases),"
        query += "sum(open_cbc_cases),"
        query += "sum(open_alerts),"
        query += "sum(dismissed_alerts),"
        query += "sum(terminated_alerts),"
        query += "sum(denied_alerts),"
        query += "sum(allow_and_log_alerts),"
        query += "sum(ran_alerts),"
        query += "sum(not_ran_alerts),"
        query += "sum(policy_applied_alerts),"
        query += "sum(policy_not_applied_alerts),"
        query += "max(prod),"
        query += "group_concat(orgid),"
        query += "max(account_id),"
        query += "group_concat(inst_id),"
        query += "group_concat(account__c)"
        query += "from master "
        query += "group by account_name"
        query += ";"
        fields = [
            "Account_Name",
            "csm",
            "cse",
            "csm_role",
            "cs_tier",
            "arr",
            "acv",
            "opportunity_ct",
            "products",
            "next_renewal",
            "next_renewal_qt",
            "gs_meter",
            "gs_overall",
            "gs_last_updated",
            "last_cua_cta",
            "cua_status",
            "last_ta",
            "last_wb",
            "last_cse_timeline",
            "last_login",
            "days_since_login",
            "last_30d_login_count",
            "last_30d_connector_count",
            "integrations",
            "last_added_user",
            "last_created_policy",
            "last_modified_policy",
            "created_date",
            "days_to_50perc",
            "licenses",
            "deployment",
            "deployment_perc",
            "last_24_contact",
            "last_7d_contact",
            "workload_deployment",
            "bypass",
            "bypass_perc",
            "last_30d_bypass_count",
            "sensor_download_unavailable",
            "Download_Unavailable_perc",
            "Sensor_Standard_Support",
            "standard_perc",
            "Sensor_Extended_Support",
            "extended_perc",
            "Sensor_EOL_Support",
            "eol_perc",
            "total_cases_30d",
            "cbc_cases_30d",
            "open_cases",
            "open_cbc_cases",
            "open_alerts",
            "dismissed_alerts",
            "terminated_alerts",
            "denied_alerts",
            "allow_and_log_alerts",
            "ran_alerts",
            "not_ran_alerts",
            "policy_applied_alerts",
            "policy_not_applied_alerts",
            "prod",
            "orgid",
            "account_id",
            "inst_id",
            "account__c"]
        data = self.db.execute(query)
        self.db.insert("account_master", fields, data, del_table=True)

if __name__ == "__main__":
    db = db_connections.sqlite_db("cua.db")
    report = summary_data(db)
    report.endpoint_lookup()
    report.direct_inserts()
    report.audit_log_inserts()
    report.connector_inserts()
    report.endpoint_inserts()
    report.cua_brag("master")
    report.sensor_versions()
    report.os_versions()
    report.deployment_summary()
    report.cse_activity_inserts()
    report.changes_over_time("master")
    report.master_archive("installation")
    report.master_archive("account")
    report.deployment_archive()
    report.prod_deployment_trend()
    report.acct_rollup()
    report.cua_brag("account_master")
    report.changes_over_time("account_master")
