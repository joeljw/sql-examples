DECLARE @SourceTable NVARCHAR(256) = 'YourSourceTable';
DECLARE @TargetTable NVARCHAR(256) = 'YourTargetTable';

SELECT 
    s_col.name AS [Column Name],
    
    -- Source Data Type Info
    s_typ.name AS [Source Type],
    CASE 
        WHEN s_typ.name IN ('varchar', 'nvarchar', 'char', 'nchar', 'binary', 'varbinary') 
        THEN CASE WHEN s_col.max_length = -1 THEN 'MAX' ELSE CAST(s_col.max_length AS VARCHAR(10)) END
        ELSE NULL 
    END AS [Source Max Length (Bytes)],
    
    -- Target Data Type Info
    t_typ.name AS [Target Type],
    CASE 
        WHEN t_typ.name IN ('varchar', 'nvarchar', 'char', 'nchar', 'binary', 'varbinary') 
        THEN CASE WHEN t_col.max_length = -1 THEN 'MAX' ELSE CAST(t_col.max_length AS VARCHAR(10)) END
        ELSE NULL 
    END AS [Target Max Length (Bytes)],
    
    -- Reason for truncation
    CASE 
        WHEN s_typ.name <> t_typ.name THEN 'Data Type Mismatch'
        WHEN s_col.max_length = -1 AND t_col.max_length <> -1 THEN 'Source is MAX, Target is Fixed'
        WHEN s_col.max_length > t_col.max_length AND t_col.max_length <> -1 THEN 'Target column is too short'
        ELSE 'Potential Type Mismatch'
    END AS [Truncation Risk Assessment]

FROM sys.tables s_tbl
INNER JOIN sys.columns s_col ON s_tbl.object_id = s_col.object_id
INNER JOIN sys.types s_typ ON s_col.user_type_id = s_typ.user_type_id

-- Match with the Target Table by Column Name
INNER JOIN sys.tables t_tbl ON t_tbl.name = @TargetTable
INNER JOIN sys.columns t_col ON t_tbl.object_id = t_col.object_id AND s_col.name = t_col.name
INNER JOIN sys.types t_typ ON t_col.user_type_id = t_typ.user_type_id

WHERE s_tbl.name = @SourceTable
  AND (
      -- Filter to show only columns where target capacity is strictly smaller than source capacity
      (s_col.max_length > t_col.max_length AND t_col.max_length <> -1) 
      OR (s_col.max_length = -1 AND t_col.max_length <> -1)
      OR (s_typ.name <> t_typ.name)
  );

