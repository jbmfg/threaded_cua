import xlsxwriter
import re
import datetime
import time

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
        self.cse_report()
        if self.csm == "all":
            self.deployment_trend()
            self.deployment_trend_perc()
        if self.csm != "all":
            query = f"select inst_id, account_name from master where csm like '{self.csm_q}' order by account_name"
            self.accounts = [[x] + i for x, i in enumerate(self.db.execute(query))]
            printProgressBar(0, len(self.accounts))
            for x, account in enumerate(self.accounts):
                printProgressBar(x+1, len(self.accounts))
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

    def multi_series_chart(self, worksheet, sheetname, charttype, datastart, dataend, cols, placement, chartname, scale=1.25):
        chart = self.wb.add_chart(charttype)
        for x, col in enumerate(cols):
            chart.add_series({
                'name': [sheetname, datastart, col],
                'categories': [sheetname, datastart+1, 0, dataend-1, 0],
                'values': [sheetname, datastart+1, col, dataend-1, col]
                })
        chart.set_style(2)
        chart.set_size({'x_scale': scale, 'y_scale': scale})
        chart.set_legend({"none": True})
        chart.set_chartarea({'fill': {"color": "white"}})
        chart.set_title({'name': chartname})
        #chart.set_legend({"position": "bottom"})
        worksheet.insert_chart(placement, chart)

    def master_sheet(self):
        sheet = self.wb.add_worksheet("Master")

        fields = ["Account_Name", "CSM", "CSE", "CSM_Role", "ARR", "ACV", "Products", "Next_Renewal", "Next_Renewal_Qt"]
        fields += ["GS_Meter", "GS_Overall", "GS_Last_Updated", "Last_CUA_CTA", "CUA_Status", "Last_TA"]
        fields += ["CUA_Brag", "Count_of_Violations", "Violations_Triggered", "brag_decrease", "Last_Login", "Days_Since_Login"]
        fields += ["Last_30d_Login_Count", "Last_30d_Connector_Count", "Integrations", "Last_Added_User"]
        fields += ["Last_Created_Policy", "Last_Modified_Policy", "Licenses", "Deployment", "Deployment_Perc", "Last_24_contact"]
        fields += ["Last_7d_contact", "Bypass", "Bypass_Perc", "Last_30d_Bypass_Count", "Sensor_Download_Unavailable"]
        fields += ["Download_Unavailable_perc"]
        fields += ["Sensor_Standard_Support", "Standard_Perc", "Sensor_Extended_Support", "Extended_Perc", "Sensor_EOL_Support"]
        fields += ["EOL_Perc", "Open_Alerts", "Dismissed_Alerts", "Terminated_Alerts", "Denied_Alerts", "Allow_and_Log_Alerts"]
        fields += ["Ran_Alerts", "Not_Ran_Alerts", "Policy_Applied_Alerts", "Policy_Not_Applied_Alerts", "Prod", "OrgID"]
        fields += ["Predictive_Churn_Meter", "Account_Risk_Factors", "Intent___Cylance", "Intent___Crowdstrike"]
        fields += ["Intent___Endgame", "Intent___Sentinelone", "Intent___Microsoft_Defender_ATP", "Searching_For_Solution"]
        fields += ["Previous_Predictive_Churn_Meter", "Predictive_Churn_Meter_Changed", "Indicators_Changed", "MSSP"]
        fields += ["Account_ID", "inst_id"]

        # Get any new fields and remove them before getting the data (then theyll be put back in)
        new_fields = [[x, i] for x, i in enumerate(fields) if i not in [row[1] for row in self.db.execute("pragma table_info(master);")]]
        for idx, f in new_fields:
            fields.remove(f)

        fields_txt = ",".join(fields)
        col1url = False if self.csm_q == "%" else True
        query = f"select {fields_txt} from master where CSM like '{self.csm_q}' order by Account_Name"
        data = self.db.execute(query)

        # Reinsert the new fields
        for idx, nf in new_fields:
            fields.insert(idx, nf)
        for row in data:
            for idx, nf in new_fields:
                row.insert(idx, nf)
        fields_txt = ",".join(fields)

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
        header = [i.replace("Count of Violations", "CUA Score") for i in header]
        data.insert(0, header)
        self.writerows(sheet, data, col1url=col1url, bolder=True)

        # Want to use this order on the individual sheets too
        self.master_order = fields
        self.master_order_txt = fields_txt
        self.master_header = header

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
        # Delete all columns that have no data
        dels = []
        for x, _ in enumerate(data[0]):
            if x == 0: continue
            if sum([r[x] for r in data[1:]]) == 0:
                dels.append(x)
        for x in dels[::-1]:
            for row in data:
                del row[x]
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
                # Delete all columns that have no data
        dels = []
        for x, _ in enumerate(data[0]):
            if x == 0: continue
            if sum([r[x] for r in data[1:]]) == 0:
                dels.append(x)
        for x in dels[::-1]:
            for row in data:
                del row[x]
        self.writerows(sheet, data, bolder=True)

    def deployment_summary(self):
        sheet = self.wb.add_worksheet("Deployment Summary")
        # Get the columns into the order needed for the query & xlsx header
        cols = [i[1] for i in self.db.execute("pragma table_info(deployment_summary);")][1:]
        cols.sort()
        versions_cols = [i for i in cols if i.replace(".", "").isnumeric()]
        os_cols = [i.title() for i in cols if i in ["windows", "linux", "mac"]]
        supp_cols = [i.title() for i in cols if i in ["st", "ex", "eol"]]
        sf_cols = [i for i in cols if i.istitle() or i.isupper()]
        header = ["Account Name", "CSM", "ARR", "Licenses", "Deployment"] + os_cols + supp_cols + versions_cols
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

    def deployment_trend(self):
        sheet = self.wb.add_worksheet("Deployment Trend")
        # Get the columns into the order needed for the query & xlsx header
        cols = [i[1] for i in self.db.execute("pragma table_info(deployment_trend);")][1:]
        text_cols = [i for i in cols if i.isalpha()][::-1]
        version_cols = [i for i in cols if i.replace(".","").isnumeric() and i[0]=="3"]
        version_cols.sort()
        header = text_cols + version_cols
        fields = ", ".join([f'"{i}"' for i in header])
        prods = list(set([i[0] for i in self.db.execute("select backend from deployment_trend;")]))
        prods.sort()
        data = []
        for prod in prods:
            query = f"""
            select {fields}
            from deployment_trend
            where backend = '{prod}';
            """
            data.extend([header] + self.db.execute(query) + [""])
        data = [[int(cell) if cell and cell.isnumeric() else cell for cell in r] for r in data]
        self.writerows(sheet, data, bolder=True)

        # ############# Charts  ################ #
        breaks = [0]
        for x,r in enumerate(data):
            if len(r) == 0:
                breaks.append(x+1)
        stacked_bar = {"type": "column", "subtype": "stacked"}
        bar = {"type": "column"}
        line = {"type": "line"}

        row = 1
        for x, prod in enumerate(prods):
            if not x % 2:
                col = "A"
            else:
                col = "P"
            chart = self.multi_series_chart(
                    sheet, "Deployment Trend", line, breaks[x], breaks[x+1] - 1,
                    list(range(2, len(data[0]))), f"{col}{row}", f"Deployment -  {prod.title()}", scale=2.0
                    )
            if col == "P": row += 32

    def deployment_trend_perc(self):
        sheet = self.wb.add_worksheet("Deployment Trend Percent")
        # Get the columns into the order needed for the query & xlsx header
        cols = [i[1] for i in self.db.execute("pragma table_info(deployment_trend);")][1:]
        text_cols = [i for i in cols if i.isalpha() and i != 'to'][::-1]
        version_cols = [i for i in cols if i.replace(".","").isnumeric() and i[0]=="3"]
        version_cols.sort()
        header = text_cols + version_cols
        fields = ", ".join([f'"{i}"' for i in header])
        prods = list(set([i[0] for i in self.db.execute("select backend from deployment_trend;")]))
        prods.sort()
        data = []
        for prod in prods:
            query = f"""
            select {fields}
            from deployment_trend
            where backend = '{prod}';
            """
            raw_numbers = self.db.execute(query)
            for x, row in enumerate(raw_numbers):
                total = sum([int(i) for i in row[2:] if i])
                for xx, n in enumerate(row):
                    raw_numbers[x][xx] = round(float(n) / total * 100, 2) if total and n and n.isnumeric() else 0
            data.extend([header] + raw_numbers + [""])
        data = [[cell for cell in r] for r in data]
        self.writerows(sheet, data, bolder=True)

        # ############# Charts  ################ #
        breaks = [0]
        for x,r in enumerate(data):
            if len(r) == 0:
                breaks.append(x+1)
        stacked_bar = {"type": "column", "subtype": "stacked"}
        bar = {"type": "column"}
        line = {"type": "line"}

        row = 1
        for x, prod in enumerate(prods):
            if not x % 2:
                col = "A"
            else:
                col = "P"
            chart = self.multi_series_chart(
                    sheet, "Deployment Trend Percent", line, breaks[x], breaks[x+1] - 1,
                    list(range(2, len(data[0]))), f"{col}{row}", f"Deployment -  {prod.title()}", scale=2.0
                    )
            if col == "P": row += 32

    def account_report(self, account):
        x, inst_id, account_name = account[0], account[1], account[2]
        account_name = account_name.replace("*", "").replace("/", "")
        sheet_name = f"{x}. {account_name}"[:31]
        sheet = self.wb.add_worksheet(sheet_name)

        # Active Bypass counts by version
        start = time.time()
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
        where e.inst_id = '{inst_id}'
        and e.last_contact_time > datetime('now', '-30 day')
        group by e.sensor_version, sl.os, sl.dl_available, sl.support_level
        order by sl.os, e.sensor_version;
        """
        header = ["Version", "OS", "Available to DL", "Support Level", "Bypass", "Active"]
        data = [header] + self.db.execute(query) + [""]

        # Active Bypass
        query = f"""
        select
        e.os_version,
        sum(case when e.status = 'BYPASS' then 1 else 0 end) as Bypass,
        sum(case when e.status = 'REGISTERED' then 1 else 0 end) as Active,
        count(*)
        from endpoints e
        join sensor_lookup sl on e.sensor_version = sl.version
        where e.inst_id = '{inst_id}'
        and e.last_contact_time > datetime('now', '-30 day')
        group by e.os_version, sl.dl_available, sl.support_level
        order by e.os_version;
        """
        header = ["OS", "Bypass", "Active", "Total"]
        data += [header] + self.db.execute(query) + [""]

        # OS Family support levels
        query = f"""
        select
        e.os,
        sum(case when sl.support_level = "ST" then 1 else 0 end) as st,
        sum(case when sl.support_level = "EX" then 1 else 0 end) as ex,
        sum(case when sl.support_level = "EOL" then 1 else 0 end) as eol,
        count(*) as total
        from endpoints e
        left join sensor_lookup sl on e.sensor_version = sl.version
        where e.inst_id = '{inst_id}'
        and e.last_contact_time > datetime('now', '-30 day')
        group by e.os;
        """
        header = ["OS Family", "Standard", "Extended", "EOL", "Total"]
        data += [header] + self.db.execute(query) + [""]

        # Login, Bypass, Connector over time
        query = f"""
        select
        min(nullif(event_time, ""))
        from audit
        where inst_id = '{inst_id}'
        and event_time not like 'csr';
        """
        # Get the earliest event and put into list of [earliest event, now]
        min_item = self.db.execute(query)[0][0]
        min_max = [int(min_item) / 1000, int(time.time())] if min_item else [int(time.time()) - 604800, int(time.time())]
        min_max = [datetime.datetime.fromtimestamp(i) for i in min_max]
        alldays = [min_max[0] + datetime.timedelta(days=x) for x in range((min_max[1] - min_max[0]).days + 2)]
        format_str = "%Y-%m" if (datetime.datetime.now() - min_max[0]).days >= 61 else "%Y-%m-%d"
        counts_dict = {datetime.datetime.strftime(i, format_str): [0, 0, "No", 0] for i in alldays}

        # Logins
        query = f"""
        select event_time
        from audit
        where inst_id = '{inst_id}'
        and description like "log%in success%";
        """
        results = [int(i[0]) / 1000 for i in self.db.execute(query)]
        for r in results:
            dk = datetime.datetime.strftime(datetime.datetime.fromtimestamp(r), format_str)
            counts_dict[dk][0] += 1

        # Bypass
        query = f"""
        select event_time, description
        from audit
        where inst_id = '{inst_id}'
        and description like '%bypass%'
        and (description like '%enabled%' or description like '%to on %')
        and description not like '%Action)';
        """
        results = [[int(i[0]) / 1000, i[1]] for i in self.db.execute(query)]
        for r in results:
            dk = datetime.datetime.strftime(datetime.datetime.fromtimestamp(r[0]), format_str)
            counts_dict[dk][1] += 1
            if "all" in r[1]:
                counts_dict[dk][2] = "Yes"

        # Connector Logins
        query = f"""
        select event_time
        from audit
        where inst_id = '{inst_id}'
        and description like "%connector% log%";
        """
        results = [int(i[0]) / 1000 for i in self.db.execute(query)]
        for r in results:
            dk = datetime.datetime.strftime(datetime.datetime.fromtimestamp(r), format_str)
            counts_dict[dk][3] += 1

        # Flatten
        header = ["Date", "Login Count", "Bypass Count", "All in Bypass", "Connector Logins"]
        data += [header] + [[dt] + counts_dict[dt] for dt in counts_dict] + [""]

        # Master Trending
        # Add any new fields into the master archive
        new_fields = [i for i in self.master_order if i not in [row[1] for row in self.db.execute("pragma table_info(master_archive);")]]
        '''
        input(new_fields)
        for nf in new_fields:
            self.db.execute(f"ALTER TABLE master_archive ADD {nf} TEXT;")
        '''
        query = f"select date, {self.master_order_txt} from master_archive where inst_id like '{inst_id}' order by date"
        results = self.db.execute(query)
        for x, row in enumerate(results):
            for xx, cell in enumerate(row):
                if cell:
                    if re.match(r"[0-9]+\.[0-9]+", cell):
                        results[x][xx] = float(cell)
                    elif cell.isnumeric():
                        results[x][xx] = int(cell)
                        if results[x][xx] > 612272122559:
                            results[x][xx] = datetime.datetime.strftime(datetime.datetime.fromtimestamp(results[x][xx]/1000).date(), "%Y-%m-%d")
        header = ["Date"] + [i.replace("___", " - ").replace("_", " ").replace("Perc", "%") for i in self.master_header]
        data += [header] + results + [""]

        if data: self.writerows(sheet, data, bolder=True, linkBool=True)

        # ############# Charts  ################ #
        breaks = []
        for x,r in enumerate(data):
            if len(r) == 0:
                breaks.append(x)
        stacked_bar = {"type": "column", "subtype": "stacked"}
        bar = {"type": "column"}
        line = {"type": "line"}

        sversion_chart = self.multi_series_chart(sheet, sheet_name, stacked_bar, 0, breaks[0], [4, 5], "H2", "Sensor Version")
        os_chart = self.multi_series_chart(sheet, sheet_name, stacked_bar, breaks[0]+1, breaks[1], [1, 2], "S2", "OS Distribution")
        osfam_chart = self.multi_series_chart(sheet, sheet_name, stacked_bar, breaks[1]+1, breaks[2], [1, 2, 3], "H21", "OS Families")
        login_chart = self.multi_series_chart(sheet, sheet_name, line, breaks[2]+1, breaks[3], [1,2], "S21", "Login & Bypass Trend")
        connector_chart = self.multi_series_chart(sheet, sheet_name, line, breaks[2]+1, breaks[3], [4], "H40", "Connector Trend")

    def write_masterlike_data(self, wb, sheet_name, data):
        sheet = wb.add_worksheet(sheet_name)
        header = self.master_header
        header = [i.replace("___", " - ").replace("_", " ").replace("Perc", "%") for i in header]
        header = [i.replace("Count of Violations", "CUA Score") for i in header]
        data.insert(0, header)
        money = wb.add_format({'num_format': '$#,##0'})
        percent = wb.add_format({'num_format': '0.00"%"'})
        for x, row in enumerate(data):
            for xx, cell in enumerate(row):
                if cell:
                    if re.match(r"[0-9]+\.[0-9]+", cell):
                        data[x][xx] = float(cell)
                    elif cell.isnumeric():
                        data[x][xx] = int(cell)
                        if data[x][xx] > 612272122559:
                            data[x][xx] = datetime.datetime.strftime(datetime.datetime.fromtimestamp(data[x][xx]/1000).date(), "%Y-%m-%d")
        self.writerows(sheet, data, col1url=True, bolder=True)
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
        return sheet

    def cse_report(self):
        query = "select cse, inst_id from sf_data where cse != 'None';"
        cse_dict = self.db.execute(query, dict=True)
        for cse in cse_dict:
            wb = xlsxwriter.Workbook("customer_usage_{}.xlsx".format(cse))

            # Regular master
            inst_ids_txt = "', '".join([i for i in cse_dict[cse]])
            query = f"select {self.master_order_txt} from master where inst_id in ('{inst_ids_txt}');"
            data = self.db.execute(query)
            sheet = self.write_masterlike_data(wb, "Master", data)

            # Reds only
            query = f"select {self.master_order_txt} from master where inst_id in ('{inst_ids_txt}') and CUA_Brag = 'Red';"
            data = self.db.execute(query)
            sheet = self.write_masterlike_data(wb, "Reds", data)

            # Yellows only
            query = f"select {self.master_order_txt} from master where inst_id in ('{inst_ids_txt}') and CUA_Brag = 'Yellow';"
            data = self.db.execute(query)
            sheet = self.write_masterlike_data(wb, "Yellows", data)
            wb.close()

def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()

if __name__ == "__main__":
    from db_connections import sqlite_db
    db = sqlite_db("cua.db")
    cua = report(db, "all")
    csms = [i[0] for i in db.execute("select distinct csm from master;")]
    for csm in csms:
        print(f"Writing report for {csm}")
        cua = report(db, csm)

