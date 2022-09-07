import db_connections
import get_csr_data
import get_sf_data
from csr_connection import prod_connection
from get_csr_data import csr_data
from get_support_data import get_support_data
from create_summary_data import summary_data
from report_writer import report
import time

def setup(tess_db):
    '''Get all the accounts we'll be fetching and login to required CSR'''
    with open("report_setup_tess.sql", "r") as f:
    #with open("report_setup_tess_nonCSM.sql", "r") as f:
        query = f.read()
    custs = tess_db.execute(query)

    prods = list(set([i[1] for i in custs]))
    prods.sort()
    # Get Google auth codes from user
    auth_dict = {}
    for prod in prods:
        auth_dict[prod] = input(f"Google Auth Code for {prod}: ")
    # Set up the connection to CSR for each prod
    csr = {}
    for prod in auth_dict:
        csr[prod] = prod_connection(prod, auth_dict[prod])
    return csr, custs

if __name__ == "__main__":
    start = time.time()
    tess_db = db_connections.tesseract_connection()
    print(f"Time to setup connection  {time.time() - start}")
    db = db_connections.sqlite_db("cua.db")
    print(f"Time to create db {time.time() - start}")
    csr, custs = setup(tess_db)
    print(f"Time to get initial data {time.time() - start}")
    inst_ids = [i[0] for i in custs]
    print(f"Time to get inst_id list {time.time() - start}")
    get_sf_data.initial_insert(db, custs)
    print(f"Time to first insert {time.time() - start}")
    get_sf_data.get_act_info(tess_db, inst_ids, db)
    print(f"Time to do acct info {time.time() - start}")
    get_sf_data.get_installation_info(tess_db, inst_ids, db)
    print(f"Time to do inst info {time.time() - start}")
    get_sf_data.get_opp_info(tess_db, inst_ids, db)
    print(f"Time to do opp info {time.time() - start}")
    get_sf_data.get_case_info(tess_db, inst_ids, db)
    print(f"Time to do case info {time.time() - start}")
    #get_sf_data.get_ds_info(inst_ids, db)
    #print(f"Time to ds info {time.time() - start}")
    get_sf_data.get_cta_info(tess_db, inst_ids, db, "Product Usage Analytics")
    get_sf_data.get_cta_info(tess_db, inst_ids, db, "Tech Assessment")
    get_sf_data.get_cta_info(tess_db, inst_ids, db, "CSA Whiteboarding")
    print(f"Time to cta info {time.time() - start}")
    get_sf_data.get_activity(db)
    print(f"Time to cse activity {time.time() - start}")
    csr_getter = csr_data(tess_db, db, csr, new_run=True)
    print("Getting Endpoints")
    csr_getter.get_endpoints()
    print("Getting Alerts")
    csr_getter.get_alerts()
    print("Getting Audit")
    csr_getter.get_audit()
    print("Getting Kits")
    csr_getter.get_kits()
    print("Getting Connectors")
    csr_getter.get_connectors()
    print("Getting Forwarders")
    csr_getter.get_forwarders()
    print("Getting Dashboards")
    csr_getter.get_dashboards()
    print("Getting Support Data")
    get_support_data(db)
    master_builder = summary_data(db)
    master_builder.endpoint_lookup()
    master_builder.direct_inserts()
    master_builder.audit_log_inserts()
    master_builder.connector_inserts()
    master_builder.endpoint_inserts()
    master_builder.cse_activity_inserts()
    master_builder.cua_brag("master")
    master_builder.sensor_versions()
    master_builder.os_versions()
    master_builder.deployment_summary()
    master_builder.changes_over_time("master")
    master_builder.master_archive("installation")
    master_builder.master_archive("account")
    master_builder.deployment_archive()
    master_builder.prod_deployment_trend()
    master_builder.acct_rollup()
    master_builder.cua_brag("account_master")
    master_builder.changes_over_time("account_master")

    # create indexes if they dont exist
    queries = [
            "create index if not exists audit_inst_id_idx on audit(inst_id);",
            "create index if not exists endpoints_inst_id_idx on endpoints(inst_id);",
            "create index if not exists alert_inst_id_idx on alerts(inst_id);"
            ]
    for q in queries: db.execute(q)

    cua = report(db, "all")
    csms = [i[0] for i in db.execute("select distinct csm from master;")]
    for csm in csms:
        cua = report(db, csm)

