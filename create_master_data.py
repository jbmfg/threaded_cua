import db_connections
from collections import defaultdict

class master_data(object):
    def __init__(self, db):
        #self.db = db_connections.sqlite_db(db)
        self.db = db
        self.inst_ids = [i[0] for i in self.db.execute("select inst_id from customers;")]

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
        fields = ["inst_id", "Prod", "OrgID", "Account_Name", "ARR", "CSM", "CSE", "Tier", "GS_Meter", "GS_Overall", "GS_Last_Updated"]
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
        fields = ["inst_id", "Account_Risk_Factors", "Intent___Cylance", "Intent___Crowdstrike", "Intent___Endgame", "Intent___Sentinelone"]
        fields += ["Intent___Microsoft_Defender_ATP", "Searching_For_Solution", "Previous_Predictive_Churn_Meter"]
        fields += ["Predictive_Churn_Meter_Changed", "Indicators_Changed", "MSSP"]
        self.db.insert("master", fields, data)

    def audit_log_inserts(self):
        ''' Audit log has info on logins, bypass, policy-, and user-adds'''
        # Login counts
        query = """
        select inst_id, count (*), max(event_time), (strftime('%s', 'now') - max(event_time) / 1000) / 86400
        from audit
        where description like 'log%in success%'
        and description not like 'Connector%'
        and datetime(event_time /1000, 'unixepoch') > datetime('now', '-30 day')
        group by inst_id;
        """
        data = self.db.execute(query)
        fields = ["inst_id", "Last_30d_Login_Count", "Last_Login", "Days_Since_Login"]
        self.db.insert("master", fields, data)

        # Bypass counts
        query = """
        select inst_id,
        SUM(CASE
        WHEN description like '%all' THEN 1000000
        WHEN description not like '%all' THEN (LENGTH(description) - LENGTH(REPLACE(description, ',', '')) +1)
        END)
        from audit
        where description like '%bypass%'
        and (description like '%enabled%' or description like '%to on %')
        and description not like '%Action)'
        and datetime(event_time /1000, 'unixepoch') > datetime('now', '-30 day')
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
        count(e.status),
        round((count(e.status) * 1.0 / sf.licenses_purchased) * 100, 2)
        from endpoints e
        left join sf_data sf on e.inst_id = sf.inst_id
        where e.last_contact_time > datetime('now', '-30 day')
        and e.status in ('REGISTERED', 'BYPASS')
        group by e.inst_id;
        """
        data = self.db.execute(query)
        fields = ["inst_id", "Deployment", "Deployment_Perc"]
        self.db.insert("master", fields, data)

        # Bypass counts
        query = """
        select e.inst_id,
        count(e.status),
        round((count(e.status) * 1.0 / sf.licenses_purchased) * 100, 2)
        from endpoints e
        left join sf_data sf on e.inst_id = sf.inst_id
        where e.last_contact_time > datetime('now', '-30 day')
        and e.status in ('BYPASS')
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
        cua = defaultdict(list)

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
        fields = ["inst_id", "Violations_Triggered", "Count_of_Violations", "CUA_Brag"]
        self.db.insert("master", fields, data)

if __name__ == "__main__":
    db = db_connections.sqlite_db("cua.db")
    report = master_data(db)
    #report.endpoint_lookup()
    #report.direct_inserts()
    #report.audit_log_inserts()
    #report.endpoint_inserts()
    report.cua_brag()
