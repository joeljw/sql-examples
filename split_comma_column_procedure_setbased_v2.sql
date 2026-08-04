CREATE PROCEDURE sp_SplitCommaString
    @TableName NVARCHAR(255),
    @SourceColumn NVARCHAR(255),
    @Part1Column NVARCHAR(255),
    @Part2Column NVARCHAR(255),
    @ErrorFlagColumn NVARCHAR(255) = '[Tunknown Agency]'  -- Default column name for error flag
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @SQL NVARCHAR(MAX);
    
    -- Build the dynamic SQL statement
    SET @SQL = N'
    UPDATE ' + QUOTENAME(@TableName) + '
    SET 
        ' + QUOTENAME(@Part1Column) + ' = CASE 
            WHEN LEN(' + QUOTENAME(@SourceColumn) + ') - LEN(REPLACE(' + QUOTENAME(@SourceColumn) + ', '','', '''')) = 1 
            THEN LEFT(' + QUOTENAME(@SourceColumn) + ', CHARINDEX('','', ' + QUOTENAME(@SourceColumn) + ') - 1)
            ELSE NULL 
        END,
        ' + QUOTENAME(@Part2Column) + ' = CASE 
            WHEN LEN(' + QUOTENAME(@SourceColumn) + ') - LEN(REPLACE(' + QUOTENAME(@SourceColumn) + ', '','', '''')) = 1 
            THEN SUBSTRING(' + QUOTENAME(@SourceColumn) + ', CHARINDEX('','', ' + QUOTENAME(@SourceColumn) + ') + 1, LEN(' + QUOTENAME(@SourceColumn) + '))
            ELSE NULL 
        END,
        ' + QUOTENAME(@ErrorFlagColumn) + ' = CASE 
            WHEN LEN(' + QUOTENAME(@SourceColumn) + ') - LEN(REPLACE(' + QUOTENAME(@SourceColumn) + ', '','', '''')) != 1 
            THEN 1 
            ELSE 0 
        END';
    
    -- Execute the dynamic SQL
    EXEC sp_executesql @SQL;
    
    -- Return count of affected rows
    SELECT @@ROWCOUNT AS RowsAffected;
END
GO
