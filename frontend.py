import db_connections
import get_csr_data
import get_sf_data
from csr_connection import prod_connection
from get_csr_data import csr_data
from get_support_data import get_support_data
from create_summary_data import summary_data
from report_writer import report
import time

def setup(sfdb):
    '''Get all the accounts we'll be fetching and login to required CSR'''
    with open("report_setup.sql", "r") as f:
        query = f.read()
    custs = sfdb.execute(query)
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
    """for prod in csr:
        print (prod)
        print(csr[prod].request("appservices/v5/eulas").status_code)
    """
    return csr, custs

if __name__ == "__main__":
    start = time.time()
    sfdb = db_connections.sf_connection("ods")
    ctadb  = db_connections.sf_connection("cta")
    print(f"Time to setup connection  {time.time() - start}")
    db = db_connections.sqlite_db("cua.db")
    print(f"Time to create db {time.time() - start}")
    csr, custs = setup(sfdb)
    print(f"Time to get initial data {time.time() - start}")
    inst_ids = [i[0] for i in custs]
    print(f"Time to get inst_id list {time.time() - start}")
    get_sf_data.initial_insert(db, custs)
    print(f"Time to first insert {time.time() - start}")
    get_sf_data.get_act_info(sfdb, inst_ids, db)
    print(f"Time to do acct info {time.time() - start}")
    get_sf_data.get_installation_info(sfdb, inst_ids, db)
    print(f"Time to do inst info {time.time() - start}")
    get_sf_data.get_opp_info(sfdb, inst_ids, db)
    print(f"Time to do opp info {time.time() - start}")
    get_sf_data.get_ds_info(inst_ids, db)
    print(f"Time to ds info {time.time() - start}")
    get_sf_data.get_cta_info(sfdb, ctadb, inst_ids, db, "Product Usage Analytics")
    get_sf_data.get_cta_info(sfdb, ctadb, inst_ids, db, "Tech Assessment")
    print(f"Time to cta info {time.time() - start}")
    csr_getter = csr_data(sfdb, db, csr, new_run=True)
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
    print("Getting Support Data")
    get_support_data(db)
    master_builder = summary_data(db)
    master_builder.endpoint_lookup()
    master_builder.direct_inserts()
    master_builder.audit_log_inserts()
    master_builder.connector_inserts()
    master_builder.endpoint_inserts()
    master_builder.cua_brag()
    master_builder.sensor_versions()
    master_builder.os_versions()
    master_builder.deployment_summary()
    master_builder.master_archive()
    master_builder.prod_deployment_trend()

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

