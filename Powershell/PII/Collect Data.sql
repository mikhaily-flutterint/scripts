IF OBJECT_ID('tempdb..#PII') IS NOT NULL
	DROP TABLE #PII

CREATE TABLE #PII(ServerName sysname,DBName sysname, ObjectType sysname, ObjectName sysname, ColumnName sysname, TypeName sysname)

INSERT INTO #PII
EXEC sp_msforeachDB 'USE [?]
SELECT @@SERVERNAME AS ServerName,
db_name() as [DB], 
o.type_desc as ObjectType,
OBJECT_SCHEMA_NAME(o.object_id)+ ''.'' +object_name(o.object_id) AS [Object], 
c.name AS [Column],
t.name AS TypeName
FROM	sys.columns c 
		join sys.objects o ON c.object_id = o.object_id 
		join sys.types t on t.user_type_id = c.user_type_id
		left join sys.dm_hadr_database_replica_states repo on repo.database_id = DB_ID() and is_primary_replica = 0
WHERE o.type IN (''U'',''V'') /* tables and views */
AND repo.database_id is null
AND (c.name like ''%mail%''
OR c.name like ''%first%name%''
OR c.name like ''%last%name%''
OR c.name like ''%birth%''
OR c.name like ''%sex%''
OR c.name like ''%address%''
OR c.name like ''%phone%''
--OR c.name like ''%social%''
OR c.name like ''%ssn%''
OR c.name like ''%postcode%''
OR c.name like ''%gender%'')
AND db_name() NOT IN (''msdb'',''tempdb'',''master'')
AND t.name <> ''bit'''

SELECT * FROM #PII
