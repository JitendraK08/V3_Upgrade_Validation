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


critical_violations="""select prev_snap.metric_id,
 
prev_snap.metric_name,
 
prev_snap.metric_num_value as previous,
 
curr_snap.metric_num_value as current,
 
case when prev_snap.metric_num_value <> 0 then round(((curr_snap.metric_num_value - prev_snap.metric_num_value) * 100 / prev_snap.metric_num_value),2) else 0 end varPercent
 
from
 
(
 
select dmt.metric_id, dmt.metric_name, dmr.metric_num_value, metric_value_index
 
from dss_metric_results dmr, dss_metric_types dmt, dss_objects o
 
where
 
dmr.metric_id = dmt.metric_id
 
and dmr.object_id = o.object_id
 
and o.object_type_id = -102
 
and snapshot_id = (select max(snapshot_id) from dss_snapshots)
 
and
 
(dmr.metric_value_index = 0 and dmr.metric_id in (67011)
 
or
 
dmr.metric_value_index = 1 and dmr.metric_id in (67011)))
 
curr_snap,
 
(
 
select dmt.metric_id, dmt.metric_name, dmr.metric_num_value, metric_value_index
 
from dss_metric_results dmr, dss_metric_types dmt, dss_objects o
 
where
 
dmr.metric_id = dmt.metric_id
 
and dmr.object_id = o.object_id
 
and o.object_type_id = -102
 
and snapshot_id = (select max(snapshot_id) from dss_snapshots where snapshot_id < (select max(snapshot_id) from dss_snapshots))
 
and (dmr.metric_value_index = 0 and dmr.metric_id in (67011)or
 
dmr.metric_value_index = 1 and dmr.metric_id in (67011)))prev_snap
 
where curr_snap.metric_id=prev_snap.metric_id
 
and curr_snap.metric_value_index=prev_snap.metric_value_index
"""

missing_code="""select distinct c1.file_path as "Referenced File",
c2.object_name as "Missing Object Name",c2.object_type_str
as "Missing Object Type"
from cdt_objects c2,csv_file_objects c1,ctv_links ctv
where c2.object_fullname like '%Unknown%'
and c1.object_id = ctv.caller_id
and c2.object_id = ctv.called_id
and c2.object_id not in (select c1.object_id from cdt_objects c1,ctv_links ctv,cdt_objects c2
where c1.object_id = ctv.caller_id and c2.object_id = ctv.called_id
and c2.object_type_str like '%Program') 
"""

missing_code_db="""select distinct c1.file_path as "Referenced File",
c2.object_name as "Missing Object Name",c2.object_type_str
as "Missing Object Type"
from cdt_objects c2,csv_file_objects c1,ctv_links ctv
where c2.object_type_str like 'Missing%'
and c1.object_id = ctv.caller_id
and c2.object_id = ctv.called_id 
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