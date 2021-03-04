import pyodbc
import json
import datetime
import sqlite3
import os
import sys
from collections import defaultdict

class sf_connection(object):
    def __init__(self):
        with open("settings.conf", "r") as f:
            settings = json.load(f)
        server = settings["Salesforce Server"]
        db = settings["Salesforce DB"]
        conn_str = "DRIVER={{ODBC Driver 17 for SQL Server}};Server={};Database={};Trusted_Connection=yes;".format(server, db)
        self.conn = pyodbc.connect(conn_str)
        self.cur = self.conn.cursor()

    def execute(self, query, dict=False):
        data = self.cur.execute(query).fetchall()
        if dict:
            if len(data[0]) == 2:
                d = defaultdict(list)
                for r in data:
                    d[r[0]].append(r[1])
                return d
        return [list(i) for i in data]

class sqlite_db(object):
    def __init__(self, db_file, **kwargs):
        self.db_file = db_file
        self.connection = sqlite3.connect(self.db_file)

    def execute(self, query, dict=False):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            #print("Query executed successfully")
        except sqlite3.Error as e:
            if "no such table" in str(e):
                return([])
            else:
                print(query)
                print(f"The error '{e}' occurred")
                input("    Press Enter to continue")
        if dict:
            data = cursor.fetchall()
            cursor.close()
            if not data:
                return {} # if there's no data returned return an empty dict
            if len(data[0]) == 4:
                d = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
                for r in data:
                    d[r[0]][r[1].lower()][r[2].lower()] = r[3]
                return d
            if len(data[0]) == 3:
                d = defaultdict(lambda: defaultdict(int))
                for r in data:
                    d[r[0]][r[1].lower()] = r[2]
                return d
            elif len(data[0]) == 2:
                d = defaultdict(list)
                for r in data:
                    d[r[0].lower()].append(r[1])
                return d
        else:
            data = [list(i) for i in cursor.fetchall()]
            cursor.close()
            return data

    def insert(self, table, fields, data, pk=True, del_table=False):
        if del_table: self.execute(f"DROP TABLE IF EXISTS '{table}';")

        # Check if table exists
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';"
        exists = True if self.execute(query) else False

        # Create the table if not
        if not exists:
            table_create = f"CREATE TABLE IF NOT EXISTS '{table}' ("
            for x, f in enumerate(fields):
                if pk:
                    table_create += f"'{f}' Text{' PRIMARY KEY, ' if x==0 else ', '}"
                else:
                    table_create += f"'{f}' Text, "
            table_create = table_create[:-2]
            table_create += ");"
            self.execute(table_create)

        # Update the table if it exists
        elif exists:
            # Check if all the columns are already in the table
            query = f"PRAGMA table_info({table});"
            columns = [i[1] for i in self.execute(query)]
            adds = [f for f in fields if f not in columns]
            if adds:
                for f in adds:
                    query = f"ALTER TABLE {table} ADD COLUMN {f} text"
                    self.execute(query)

        # Insert data into new table or update existing table
        def chunks(data, rows=5000):
            for i in range(0, len(data), rows):
                yield data[i:i+rows]
        cur = self.connection.cursor()
        chunks = chunks(data)
        qms = ",".join("?" * len(fields))

        if "adds" in locals() and adds:
            # if we have to add a column it means we are appending a value to the rows
            # data submitted needs to have the item at index 0 be the primary key
            for chunk in chunks:
                cur.execute("BEGIN TRANSACTION")
                for row in chunk:
                    query = f"UPDATE {table} SET "
                    for x, f in enumerate(fields):
                        if x == 0 : continue
                        row[x] = row[x].replace("\"", "\'") if isinstance(row[x], str) else row[x]
                        query += f"""{f} = "{row[x]}", """
                    query = query[:-2]
                    query += f" where {fields[0]} = '{row[0]}';"
                    cur.execute(query)
                cur.execute("COMMIT")
        else:
            # Otherwise we just want to insert the new rows at the bottom of the table
            for chunk in chunks:
                cur.execute("BEGIN TRANSACTION")
                for row in chunk:
                    query = f"INSERT INTO {table} ({','.join(fields)}) VALUES ({qms})"
                    cur.execute(query, row)
                cur.execute("COMMIT")
            return True
        return data[0]

    def close_db(self):
        self.connection.close()


if __name__ == "__main__":
    pass
