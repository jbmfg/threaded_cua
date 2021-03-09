import xlsxwriter
import re
import datetime

class report(object):
    def __init__(self, db, csm):
        self.db = db
        self.csm = csm
        self.wb = xlsxwriter.Workbook("customer_usage_{}.xlsx".format(self.csm))
        self.master_sheet()
        self.sensor_versions()
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
                    sheet.write_url(cell, "internal:'{}'!A1".format("{}. {}".format(x,str(r[0]).replace("'","''"))[:31]), string=r[0])
            if linkBool:
                sheet.write_url(0, 6, "internal:Master!A1", string="Mastersheet")
        return True

    def master_sheet(self):
        sheet = self.wb.add_worksheet("Master")

        fields = ["Account_Name", "CSM", "CSE", "Tier", "ARR", "ACV", "Products", "Next_Renewal", "Next_Renewal_Qt"]
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
        csm = "%" if self.csm == "all" else self.csm
        col1url = False if csm == "%" else True
        query = f"select {fields_txt} from master where CSM like '{csm}' order by Account_Name"
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
        order by sf.account_name;
        """
        data = self.db.execute(query)
        for x, row in enumerate(data):
            for xx, cell in enumerate(row):
                data[x][xx] = int(cell) if cell.isnumeric() else cell
        header = ["Account Name"] + [i[1] for i in self.db.execute("pragma table_info(sensor_versions_summary);")][1:]
        data.insert(0, header)
        self.writerows(sheet, data, bolder=True)

if __name__ == "__main__":
    from db_connections import sqlite_db
    db = sqlite_db("cua.db")
    csm = "all"
    cua = report(db, csm)
