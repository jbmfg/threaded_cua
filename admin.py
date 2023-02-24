import os
import subprocess
import datetime

today = str(datetime.datetime.date(datetime.datetime.now() - datetime.timedelta(hours=14))).replace("-", ".")

def run_cmd(cmd):
    print(cmd)
    process = subprocess.Popen(cmd, shell=True)
    
cmds = []
cmds += ["mv /home/bgoff/onedrive/CSE\ Team\ Shares/CUA/cua.db.zip /home/bgoff/onedrive/CSE\ Team\ Shares/CUA/cua.db.zip.last1"]
cmds += ["mv cua.db.zip cua.db.zip.last1"]
cmds += ["zip cua.db.zip cua.db"]
cmds += ["cp cua.db.zip /home/bgoff/onedrive/CSE\ Team\ Shares/CUA/"]
cmds += ["rm /home/bgoff/onedrive/CSE\ Team\ Shares/CUA/xlsx_files/*"]
cmds += [f"zip {today}.zip customer_usage*.xlsx Distinct*.xlsx"]
cmds += [f"cp /home/bgoff/dev/threaded_cua/{today}.zip /home/bgoff/onedrive/CSE\\ Team\\ Shares/CUA/.archive/{today}.zip"]
cmds += [f"mv customer_usage*.xlsx Distinct*.xlsx /home/bgoff/onedrive/CSE\\ Team\\ Shares/CUA/xlsx_files/"]
cmds += ["sh power_bi.cmd"]
cmds += ["cp cua_power_bi.csv /home/bgoff/onedrive/PowerBI/"]

for cmd in cmds:
    run_cmd(cmd)
