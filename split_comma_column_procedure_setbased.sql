/*
 * Procedure: SplitCommaColumn_SetBased
 * Purpose: Splits a comma-separated string column into two separate columns.
 *          Validates that exactly one comma exists; otherwise, flags an error.
 *          Set-based implementation (no cursor) for better performance.
 *          Compatible with SQL Server compatibility level < 130.
 *
 * Assumptions:
 *   - Table name: YourTableName (replace as needed)
 *   - Input column: YourInputColumn (NVARCHAR/VARCHAR)
 *   - Output columns: YourOutputColumn1, YourOutputColumn2
 *   - Error flag column: YourErrorColumn (NVARCHAR(100) or similar)
 *   - Primary key column: YourPrimaryKeyColumn (INT or appropriate type)
 */

CREATE PROCEDURE dbo.SplitCommaColumn_SetBased
AS
BEGIN
    SET NOCOUNT ON;

    /*
     * Set-based update using a derived table with computed split values.
     * No cursor, no loops - single UPDATE statement for all rows.
     * Uses LTRIM/RTRIM for compatibility with level < 130.
     */

    UPDATE t
    SET 
        YourOutputColumn1 = v.Part1,
        YourOutputColumn2 = v.Part2,
        YourErrorColumn = v.ErrorMessage
    FROM 
        YourTableName AS t
    CROSS APPLY (
        SELECT 
            -- Count commas: must be exactly 1
            CommaCount = LEN(t.YourInputColumn) - LEN(REPLACE(t.YourInputColumn, ',', '')),
            
            -- Part 1: before the comma (trimmed)
            Part1 = CASE 
                WHEN LEN(t.YourInputColumn) - LEN(REPLACE(t.YourInputColumn, ',', '')) = 1
                THEN LTRIM(RTRIM(LEFT(t.YourInputColumn, CHARINDEX(',', t.YourInputColumn) - 1)))
                ELSE NULL
            END,
            
            -- Part 2: after the comma (trimmed)
            Part2 = CASE 
                WHEN LEN(t.YourInputColumn) - LEN(REPLACE(t.YourInputColumn, ',', '')) = 1
                THEN LTRIM(RTRIM(SUBSTRING(t.YourInputColumn, CHARINDEX(',', t.YourInputColumn) + 1, LEN(t.YourInputColumn))))
                ELSE NULL
            END,
            
            -- Error message for invalid comma count
            ErrorMessage = CASE 
                WHEN LEN(t.YourInputColumn) - LEN(REPLACE(t.YourInputColumn, ',', '')) = 1
                THEN NULL
                ELSE 'Invalid comma count: ' + CAST((LEN(t.YourInputColumn) - LEN(REPLACE(t.YourInputColumn, ',', ''))) AS NVARCHAR(10))
            END
    ) AS v;
END;
GO

/*
 * Example usage:
 *
 * -- First, ensure your table has the necessary columns:
 * -- YourPrimaryKeyColumn (INT or appropriate PK type)
 * -- YourInputColumn (NVARCHAR/VARCHAR) - the comma-separated input
 * -- YourOutputColumn1 (NVARCHAR/VARCHAR) - first split value
 * -- YourOutputColumn2 (NVARCHAR/VARCHAR) - second split value
 * -- YourErrorColumn (NVARCHAR(100)) - error message or NULL
 *
 * EXEC dbo.SplitCommaColumn_SetBased;
 *
 * Sample data transformation:
 *   Input: "John,Doe"     → Output1: "John", Output2: "Doe", Error: NULL
 *   Input: "John,Doe,III"  → Output1: NULL,  Output2: NULL,  Error: "Invalid comma count: 2"
 *   Input: "JohnDoe"       → Output1: NULL,  Output2: NULL,  Error: "Invalid comma count: 0"
 *
 * Compatibility notes:
 *   - Uses CREATE PROCEDURE (no OR ALTER) for < 130 compatibility.
 *   - Uses LTRIM/RTRIM instead of TRIM (TRIM requires 130+).
 *   - Uses CROSS APPLY (available in all compatibility levels).
 *   - No cursor, no WHILE loop - fully set-based for better performance.
 *
 * Performance notes:
 *   - Processes all rows in a single UPDATE statement.
 *   - Much more efficient than cursor-based approach for large tables.
 *   - Consider adding an index on YourInputColumn if filtering is needed.
 */