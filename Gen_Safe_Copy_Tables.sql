DECLARE @SourceTable NVARCHAR(256) = 'YourSourceTable';
DECLARE @TargetTable NVARCHAR(256) = 'YourTargetTable';

DECLARE @SelectFields NVARCHAR(MAX) = '';

SELECT @SelectFields = @SelectFields + 
    CASE 
        -- If it's a character column and the target is shorter, inject a LEFT() truncation function
        WHEN s_typ.name IN ('varchar', 'nvarchar', 'char', 'nchar') 
             AND (s_col.max_length > t_col.max_length OR s_col.max_length = -1) 
             AND t_col.max_length <> -1
        THEN 'LEFT([' + s_col.name + '], ' + 
             CAST(CASE WHEN t_typ.name IN ('nvarchar', 'nchar') THEN t_col.max_length / 2 ELSE t_col.max_length END AS VARCHAR(10)) + 
             ') AS [' + s_col.name + '], '
        
        -- Otherwise, keep the column as-is
        ELSE '[' + s_col.name + '], '
    END
FROM sys.tables s_tbl
INNER JOIN sys.columns s_col ON s_tbl.object_id = s_col.object_id
INNER JOIN sys.types s_typ ON s_col.user_type_id = s_typ.user_type_id
INNER JOIN sys.tables t_tbl ON t_tbl.name = @TargetTable
INNER JOIN sys.columns t_col ON t_tbl.object_id = t_col.object_id AND s_col.name = t_col.name
WHERE s_tbl.name = @SourceTable
ORDER BY s_col.column_id;

-- Trim trailing comma
SET @SelectFields = SUBSTRING(@SelectFields, 1, LEN(@SelectFields) - 1);

-- Print out ready-made script
PRINT 'INSERT INTO [' + @TargetTable + ']';
PRINT 'SELECT ' + @SelectFields;
PRINT 'FROM [' + @SourceTable + ']';

