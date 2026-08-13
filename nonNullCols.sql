DECLARE @TableName NVARCHAR(256) = 'YourTableName'; -- Replace with your table name
DECLARE @SQL NVARCHAR(MAX) = '';

SELECT @SQL = @SQL + 'SELECT ''' + name + ''' AS ColumnName WHERE EXISTS (SELECT 1 FROM ' + @TableName + ' WHERE ' + QUOTENAME(name) + ' IS NOT NULL) UNION ALL '
FROM sys.columns 
WHERE object_id = OBJECT_ID(@TableName);

-- Remove trailing UNION ALL
IF LEN(@SQL) > 0
BEGIN
    SET @SQL = LEFT(@SQL, LEN(@SQL) - 11) + ' ORDER BY ColumnName;';
    EXEC sp_executesql @SQL;
END

