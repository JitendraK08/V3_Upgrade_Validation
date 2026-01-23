loc="""SELECT DISTINCT ON (app.name)
    ss.lines_of_code

FROM
    aip_node.application app
JOIN
    aip_node.snapshot ss ON ss.application_guid = app.guid where domain_guid =%s and  app.name=%s;"""

loc_null="""SELECT DISTINCT ON (app.name)
    ss.lines_of_code

FROM
    aip_node.application app
JOIN
    aip_node.snapshot ss ON ss.application_guid = app.guid where domain_guid is NULL and  app.name=%s;"""


loc_per_tech="""select distinct dos.object_name as technology,dmr.metric_num_value as LOC from dss_metric_results dmr
join dss_objects dos on dmr.object_id = dos.object_id
where dmr.object_id in (select object_id from dss_objects 
where object_type_id in (select object_type_id from dss_object_types where object_group = 2))
and snapshot_id = (select snapshot_id from adg_delta_snapshots where latest = 1)
and metric_id = 10151
"""

extension_count="""SELECT COUNT(*) AS total_extensions
FROM sys_package_version
WHERE package_name LIKE '/%';
"""

analyzed_files="""SELECT COUNT(*) AS total_files_analyzed
FROM dss_code_sources
"""

dlms="select count(*) AS dlm_count from acc  where prop=1"


critical_violations="""SELECT
    dmt.metric_id,
    dmt.metric_name,
    dmr.metric_num_value AS current
FROM dss_metric_results dmr
JOIN dss_metric_types dmt
    ON dmr.metric_id = dmt.metric_id
JOIN dss_objects o
    ON dmr.object_id = o.object_id
WHERE o.object_type_id = -102
AND dmr.snapshot_id = (
    SELECT MAX(snapshot_id)
    FROM dss_snapshots
)
AND dmr.metric_id IN (67011)
AND dmr.metric_value_index IN (0, 1);
"""

missing_code_db="""SELECT COUNT(*)
FROM cdt_objects
WHERE object_type_str LIKE 'Missing%';
"""
missing_code="""SELECT COUNT(*)
FROM cdt_objects
WHERE object_fullname LIKE '%Unknown%';
"""

customized_jobs="SELECT count(*) FROM cms_objectlinks where symbol = 'afterTools';"

total_object_count="MATCH (o:Object:%s) WHERE NOT 'Deleted' IN labels(o) RETURN count(o) AS total_object_count"

check_schemas="""SELECT schema_name
FROM information_schema.schemata
WHERE schema_name NOT LIKE 'pg_%'
  AND schema_name <> 'information_schema'
ORDER BY schema_name;"""


check_app_schema="""
                    SELECT nspname
                    FROM pg_namespace
                    WHERE nspname LIKE %s;
                """

fetch_app_schema=""" set search_path to aip_node;
               SELECT a.name, b.schema_prefix,a.domain_guid
FROM application a
JOIN connection_profile b
  ON a.connection_profile_guid = b.guid
WHERE (a.domain_guid = %s
       OR a.domain_guid IS NULL);
            """