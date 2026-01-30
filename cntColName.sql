DECLARE @TableName  sysname = N'dbo.YourTableName';
DECLARE @PartialName sysname = N'YourPartialString';

DECLARE @sql nvarchar(max);

;WITH Cols AS (
    SELECT c.name
    FROM sys.columns AS c
    WHERE c.object_id = OBJECT_ID(@TableName)
      AND c.name LIKE N'%' + @PartialName + N'%'
)
SELECT @sql = STRING_AGG(
    N'SELECT ''' + QUOTENAME(name) + ''' AS ColumnName
      WHERE EXISTS (SELECT 1 FROM ' + @TableName + N' WHERE ' + QUOTENAME(name) + N' IS NOT NULL)',
    N' UNION ALL '
)
FROM Cols;

SET @sql = N'SELECT COUNT(*) AS MatchingNonNullColumnCount FROM (' + @sql + N') AS x;';

EXEC sys.sp_executesql @sql;
