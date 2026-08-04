DECLARE @CommaCount INT;
DECLARE @Pos INT;

UPDATE #TempData
SET 
    @CommaCount = LEN(OriginalString) - LEN(REPLACE(OriginalString, ',', '')),
    @Pos = CASE 
        WHEN @CommaCount = 1 THEN CHARINDEX(',', OriginalString) 
        ELSE 0 
    END,
    Part1 = CASE 
        WHEN @CommaCount = 1 THEN SUBSTRING(OriginalString, 1, @Pos - 1)
        ELSE NULL 
    END,
    Part2 = CASE 
        WHEN @CommaCount = 1 THEN SUBSTRING(OriginalString, @Pos + 1, LEN(OriginalString))
        ELSE NULL 
    END,
    HasError = CASE 
        WHEN @CommaCount != 1 THEN 1 
        ELSE 0 
    END,
    ErrorMessage = CASE 
        WHEN @CommaCount = 0 THEN 'No comma found'
        WHEN @CommaCount > 1 THEN 'Multiple commas found'
        ELSE NULL 
    END;
