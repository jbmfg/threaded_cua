import xlsxwriter
import re
import datetime

class report(object):
    def __init__(self, db, csm):
        self.db = db
        self.csm = csm
        self.csm_q = "%" if self.csm == "all" else self.csm.replace("'", "''")
        self.wb = xlsxwriter.Workbook("customer_usage_{}.xlsx".format(self.csm))
        self.master_sheet()
        self.sensor_versions()
        self.os_versions()
        self.deployment_summary()
        if self.csm != "all":
            query = f"select inst_id, account_name from master where csm like '{self.csm_q}' order by account_name"
            self.accounts = [[x] + i for x, i in enumerate(self.db.execute(query))]
            for account in self.accounts:
                self.account_report(account)
        self.wb.close()

    def writerows(self, sheet, data, linkBool=False, setwid=True, col1url=False, bolder=False):
        bold = self.wb.add_format({"bold": True})
        # first get the length of the longest sting to set column widths
        numCols = len(data[0])
        widest = [10 for _ in range(numCols)]
        if setwid:
            try:
                for i in data:
                    for x in range(len(data[0])):
                        if type(i[x]) == int:
                            pass
                        elif i[x] is None:
                            pass
                        elif not isinstance(i[x], float) and widest[x] < len(i[x].encode("ascii", "ignore")):
                            if len(str(i[x])) > 50:
                                widest[x] = 50
                            else:
                                widest[x] = len(str(i[x])) #+ 4
            except IndexError:
                pass
                # print ("--INFO-- Index Error when setting column widths")
            except TypeError:
                print ("type error")
            except AttributeError:
                # Added check for floats above so this probably isnt needed any more
                print ("\n--INFO-- Can't encode a float\n")

        for x, i in enumerate(widest):
            sheet.set_column(x, x, i)

        # Then write the data
        for r in data:
            for i in r:
                if type(i) == str:
                    i = i.encode("ascii", "ignore")
        counter = 0
        for x, r in enumerate(data):
            counter += 1
            cell = "A" +str(counter)
            if bolder and (data[x-1] == "" or x==0):
                sheet.write_row(cell, r, bold)
            else:
                sheet.write_row(cell, r)
            if col1url:
                if x == 0:
                    pass
                else:
                    sheet_name = f"{x-1}. {r[0]}"[:31]
                    #sheet.write_url(cell, "internal:'{}'!A1".format("{}. {}".format(x,str(r[0]).replace("'","''"))[:31]), string=r[0])
                    sheet.write_url(cell, f"internal:'{sheet_name}'!A1", string=r[0])
            if linkBool:
                sheet.write_url(0, 6, "internal:Master!A1", string="Mastersheet")
        return True

    def master_sheet(self):
        sheet = self.wb.add_worksheet("Master")

        fields = ["Account_Name", "CSM", "CSE", "CSM_Role", "ARR", "ACV", "Products", "Next_Renewal", "Next_Renewal_Qt"]
        fields += ["GS_Meter", "GS_Overall", "GS_Last_Updated", "CUA_BRAG", "Count_of_Violations", "Violations_Triggered"]
        fields += ["Last_Login", "Days_Since_Login", "Last_30d_Login_Count", "Last_30d_Connector_Count", "Last_Added_User"]
        fields += ["Last_Created_Policy", "Last_Modified_Policy", "Licenses", "Deployment", "Deployment_Perc", "Bypass"]
        fields += ["Bypass_Perc", "Last_30d_Bypass_Count", "Sensor_Download_Unavailable", "Download_Unavailable_Perc"]
        fields += ["Sensor_Standard_Support", "Standard_Perc", "Sensor_Extended_Support", "Extended_Perc", "Sensor_EOL_Support"]
        fields += ["EOL_Perc", "Open_Alerts", "Dismissed_Alerts", "Terminated_Alerts", "Denied_Alerts", "Allow_and_Log_Alerts"]
        fields += ["Ran_Alerts", "Not_Ran_Alerts", "Policy_Applied_Alerts", "Policy_Not_Applied_Alerts", "Prod", "OrgID"]
        fields += ["Predictive_Churn_Meter", "Account_Risk_Factors", "Intent___Cylance", "Intent___Crowdstrike"]
        fields += ["Intent___Endgame", "Intent___Sentinelone", "Intent___Microsoft_Defender_ATP", "Searching_For_Solution"]
        fields += ["Previous_Predictive_Churn_Meter", "Predictive_Churn_Meter_Changed", "Indicators_Changed", "MSSP"]
        fields += ["Account_ID", "inst_id"]

        fields_txt = ",".join(fields)
        col1url = False if self.csm_q == "%" else True
        query = f"select {fields_txt} from master where CSM like '{self.csm_q}' order by Account_Name"
        data = self.db.execute(query)
        for x, row in enumerate(data):
            for xx, cell in enumerate(row):
                if cell:
                    if re.match(r"[0-9]+\.[0-9]+", cell):
                        data[x][xx] = float(cell)
                    elif cell.isnumeric():
                        data[x][xx] = int(cell)
                        if data[x][xx] > 612272122559:
                            data[x][xx] = datetime.datetime.strftime(datetime.datetime.fromtimestamp(data[x][xx]/1000).date(), "%Y-%m-%d")

        header = [i.replace("___", " - ").replace("_", " ").replace("Perc", "%") for i in fields]
        data.insert(0, header)
        self.writerows(sheet, data, col1url=col1url, bolder=True)

        money = self.wb.add_format({'num_format': '$#,##0'})
        percent = self.wb.add_format({'num_format': '0.00"%"'})
        money_cols, percent_cols = [], []
        for h in header:
            if "%" in h:
                percent_cols.append(header.index(h))
            elif "ARR" in h or "ACV" in h:
                money_cols.append(header.index(h))
        for h in money_cols:
            sheet.write_column(1, h, [r[h] for r in data[1:]], money)
        for h in percent_cols:
            sheet.write_column(1, h, [r[h] for r in data[1:]], percent)

    def sensor_versions(self):
        sheet = self.wb.add_worksheet("Sensor Versions")
        cols = ', '.join([f'svs."{i[1]}"' for i in self.db.execute("pragma table_info(sensor_versions_summary);")][1:])
        query = f"""
        select sf.account_name, {cols}
        from sensor_versions_summary svs
        left join sf_data sf on svs.inst_id = sf.inst_id
        where sf.csm like '{self.csm_q}'
        order by sf.account_name;
        """
        data = self.db.execute(query)
        for x, row in enumerate(data):
            for xx, cell in enumerate(row):
                data[x][xx] = int(cell) if cell.isnumeric() else cell
        header = ["Account Name"] + [i[1] for i in self.db.execute("pragma table_info(sensor_versions_summary);")][1:]
        data.insert(0, header)
        self.writerows(sheet, data, bolder=True)

    def os_versions(self):
        sheet = self.wb.add_worksheet("OS Versions")
        cols = ', '.join([f'ovs."{i[1]}"' for i in self.db.execute("pragma table_info(os_versions_summary);")][1:])
        query = f"""
        select sf.account_name, {cols}
        from os_versions_summary ovs
        left join sf_data sf on ovs.inst_id = sf.inst_id
        where sf.csm like '{self.csm_q}'
        order by sf.account_name;
        """
        data = self.db.execute(query)
        for x, row in enumerate(data):
            for xx, cell in enumerate(row):
                data[x][xx] = int(cell) if cell.isnumeric() else cell
        header = ["Account Name"] + [i[1] for i in self.db.execute("pragma table_info(os_versions_summary);")][1:]
        data.insert(0, header)
        self.writerows(sheet, data, bolder=True)

    def deployment_summary(self):
        sheet = self.wb.add_worksheet("Deployment Summary")
        # Get the columns into the order needed for the query & xlsx header
        cols = [i[1] for i in self.db.execute("pragma table_info(deployment_summary);")][1:]
        cols.sort()
        versions_cols = [i for i in cols if i.replace(".", "").isnumeric()]
        os_cols = [i for i in cols if i.islower()]
        sf_cols = [i for i in cols if i.istitle() or i.isupper()]
        header = ["Account Name", "CSM", "ARR", "Licenses", "Deployment"] + os_cols + versions_cols
        fields = ", ".join([f"ds.'{i}'" for i in header[1:]])

        query = f"""
        select sf.account_name, {fields}
        from deployment_summary ds
        left join sf_data sf on ds.inst_id = sf.inst_id
        where sf.csm like '{self.csm_q}'
        order by sf.account_name;
        """
        data = self.db.execute(query)
        data = [[int(cell) if cell and cell.isnumeric() else cell for cell in r] for r in data]
        data.insert(0, header)
        self.writerows(sheet, data, bolder=True)

    def account_report(self, account):
        x, inst_id, account_name = account[0], account[1], account[2]
        account_name = account_name.replace("*", "").replace("/", "")
        sheet = self.wb.add_worksheet(f"{x}. {account_name}"[:31])

        # Active Bypass counts
        query = f"""
        select
        e.sensor_version,
        sl.os,
        sl.dl_available,
        sl.support_level,
        sum(case when e.status = 'BYPASS' then 1 else 0 end) as Bypass,
        sum(case when e.status = 'REGISTERED' then 1 else 0 end) as Active
        from endpoints e
        join sensor_lookup sl on e.sensor_version = sl.version
        where e.last_contact_time > datetime('now', '-30 day')
        and e.inst_id = '{inst_id}'
        group by e.sensor_version, sl.os, sl.dl_available, sl.support_level
        order by sl.os, e.sensor_version;
        """
        header = ["Version", "OS", "Available to DL", "Support Level", "Bypass", "Active"]
        data = [header] + self.db.execute(query)
        if data: self.writerows(sheet, data)



if __name__ == "__main__":
    from db_connections import sqlite_db
    db = sqlite_db("cua.db")
    cua = report(db, "all")
    csms = [i[0] for i in db.execute("select distinct csm from master;")]
    for csm in csms:
        print(f"Writing report for {csm}")
        cua = report(db, csm)

