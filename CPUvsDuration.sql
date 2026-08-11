SELECT 
    r.session_id AS [SPID],
    s.login_name AS [Login],
    DB_NAME(r.database_id) AS [Database],
    r.status AS [Status],
    
    -- Execution duration (actual clock time)
    r.total_elapsed_time AS [Duration (ms)],
    (r.total_elapsed_time / 1000.0) AS [Duration (sec)],
    
    -- Raw CPU time
    r.cpu_time AS [CPU Time (ms)],
    (r.cpu_time / 1000.0) AS [CPU Time (sec)],
    
    -- The Ratio (Values > 1.0 mean parallelism is active)
    CAST(r.cpu_time AS FLOAT) / ISNULL(NULLIF(r.total_elapsed_time, 0), 1) AS [CPU to Duration Ratio],
    
    -- The query text
    SUBSTRING(t.text, (r.statement_start_offset/2)+1,   
        (((CASE r.statement_end_offset  
            WHEN -1 THEN DATALENGTH(t.text)  
            ELSE r.statement_end_offset  
          END) - r.statement_start_offset)/2) + 1) AS [Query Text]
FROM sys.dm_exec_requests r
INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
WHERE s.is_user_process = 1 
  AND r.session_id <> @@SPID
ORDER BY r.total_elapsed_time DESC;

