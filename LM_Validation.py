import psycopg2
import pandas as pd
from neo4j import GraphDatabase
from config import load_config
from Queries import (
    loc, loc_per_tech, dlms,
    extension_count, missing_code_db,
    analyzed_files, critical_violations,
    missing_code, check_schemas,
    check_app_schema, loc_null,
    customized_jobs
)

# ------------------ LOAD CONFIG ------------------
config = load_config()
# config['NEO4J_DB'] = config['NEO4J_DB'].split(',')

# ------------------ POSTGRES CONNECTION ------------------
def postgres_connection():
    return psycopg2.connect(
        host=config['CSS_HOST'],
        port=config['CSS_PORT'],
        dbname=config['CSS_DB'],
        user=config['CSS_USERNAME'],
        password=config['CSS_PASSWORD']
    )

# ------------------ NEO4J CONNECTION ------------------
def neo4j_connection(uri, username, password):
    return GraphDatabase.driver(uri, auth=(username, password))

# ------------------ FETCH ALL NEO4J OBJECT COUNTS ------------------
def fetch_neo4j_object_counts(driver, database_names):
    """
    Returns:
    {
        "AppName1": total_object_count,
        "AppName2": total_object_count
    }
    """
    app_object_counts = {}

    for db in database_names:
        with driver.session(database=db) as session:
            # Fetch applications
            apps = session.run("""
                MATCH (a:Application)
                RETURN a.Name AS app_name
            """)

            for record in apps:
                app_name = record["app_name"]
                if not app_name:
                    continue

                # Dynamic label based query
                query = f"""
                MATCH (o:Object:{app_name})
                WHERE NOT 'Deleted' IN labels(o)
                RETURN count(o) AS cnt
                """

                result = session.run(query).single()
                count = result["cnt"] if result else 0

                # Aggregate across DBs
                app_object_counts[app_name] = (
                    app_object_counts.get(app_name, 0) + count
                )

    return app_object_counts

# ------------------ VALUE EXTRACTION ------------------
def extract_v2(param, value):
    if not value:
        return 0

    if param == "loc":
        return value[0][0]

    if param == "loc_per_tech":
        return ", ".join(f"{tech}:{int(loc)}" for tech, loc in value)

    if param in (
        "extension_count", "dlms",
        "analyzed_files", "Total Object Count",
        "Customized Jobs"
    ):
        return value[0][0]

    if param in ("missing_code_db", "Missing Code"):
        return len(value)

    if param == "Dashboard - Critical violations":
        return value[0][3]

    return 0

# ------------------ BUILD EXCEL ROWS ------------------
def build_excel_rows(all_data):
    rows = []
    app_name = all_data["app_name"]
    first = True

    for key, value in all_data.items():
        if key in ("domain_name", "app_name"):
            continue

        rows.append({
            "App Name": app_name if first else "",
            "Parameters": key.replace("_", " ").title(),
            "V2": extract_v2(key, value),
            "V3": "",
            "Variation": ""
        })
        first = False

    return rows

# ------------------ MAIN REPORT ------------------
def generate_report():
    connection = postgres_connection()
    cursor = connection.cursor()

    neo4j_driver = neo4j_connection(
        config['NEO4J_URL'],
        config['NEO4J_USER'],
        config['NEO4J_PASSWORD']
    )
    tenants = config["NEO4J_DB"].split(',')

    # 🔥 Fetch Neo4j data ONCE
    neo4j_object_counts = fetch_neo4j_object_counts(
        neo4j_driver,
        tenants
    )

    # Fetch domains
    cursor.execute("SELECT guid, name FROM aip_node.domain ORDER BY guid ASC")
    domains = cursor.fetchall()

    with pd.ExcelWriter("V3_Upgrade_Apps_Validation.xlsx", engine="openpyxl") as writer:

        for domain_guid, domain_name in domains:

            cursor.execute("""
                SELECT name, guid, domain_guid
                FROM aip_node.application
                WHERE domain_guid = %s
                   OR domain_guid IS NULL
                ORDER BY guid ASC
            """, (domain_guid,))
            apps = cursor.fetchall()

            cursor.execute(check_schemas)

            for app_name, guid, app_domain_guid in apps:

                sheet = domain_name if app_domain_guid else "default"
                guid_mod = guid.replace("-", "_")
                param = f"%{guid_mod}%"

                cursor.execute(check_app_schema, (param,))
                schemas = cursor.fetchall()

                set_search_path = f"uuid_{guid_mod}" if schemas else app_name

                # -------- CENTRAL --------
                cursor.execute(f"SET search_path TO {set_search_path}_central")

                loc_query = loc_null if app_domain_guid is None else loc
                if app_domain_guid is None:
                    cursor.execute(loc_query, (app_name,))
                else:
                    cursor.execute(loc_query, (app_domain_guid, app_name))

                loc_rows = cursor.fetchall()
                cursor.execute(loc_per_tech)
                loc_per_tech_rows = cursor.fetchall()
                cursor.execute(extension_count)
                extension_count_rows = cursor.fetchall()
                cursor.execute(critical_violations)
                critical_violations_rows = cursor.fetchall()

                # -------- LOCAL --------
                cursor.execute(f"SET search_path TO {set_search_path}_local")
                cursor.execute(dlms)
                dlms_rows = cursor.fetchall()
                cursor.execute(missing_code_db)
                missing_code_db_rows = cursor.fetchall()
                cursor.execute(analyzed_files)
                analyzed_files_rows = cursor.fetchall()
                cursor.execute(missing_code)
                missing_codes = cursor.fetchall()

                # -------- MNGT --------
                cursor.execute(f"SET search_path TO {set_search_path}_mngt")
                cursor.execute(customized_jobs)
                customized_jobs_rows = cursor.fetchall()
                print("posgres queries exution completed")
                # -------- MERGE NEO4J --------
                total_object_count = neo4j_object_counts.get(app_name, 0)

                all_data = {
                    "domain_name": sheet,
                    "app_name": app_name,
                    "loc": loc_rows,
                    "loc_per_tech": loc_per_tech_rows,
                    "extension_count": extension_count_rows,
                    "dlms": dlms_rows,
                    "missing_code_db": missing_code_db_rows,
                    "analyzed_files": analyzed_files_rows,
                    "Missing Code": missing_codes,
                    "Dashboard - Critical violations": critical_violations_rows,
                    "Total Object Count": [(total_object_count,)],
                    "Customized Jobs": customized_jobs_rows
                }

                rows = build_excel_rows(all_data)
                df = pd.DataFrame(rows)

                startrow = writer.sheets[sheet].max_row if sheet in writer.sheets else 0
                df.to_excel(
                    writer,
                    sheet_name=sheet,
                    index=False,
                    startrow=startrow,
                    header=startrow == 0
                )

    cursor.close()
    connection.close()
    neo4j_driver.close()
    print("V3_Upgrade_Apps_Validation.xlsx generated successfully")

# ------------------ RUN ------------------
if __name__ == "__main__":
    generate_report()
