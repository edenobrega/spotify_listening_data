CREATE   PROCEDURE ETL.AddFile
    @name NVARCHAR(2000)
AS
BEGIN
    DECLARE @out TABLE(ID INT)

    INSERT INTO ETL.Files([Name])
    OUTPUT inserted.ID INTO  @out
    VALUES (@name)

    SELECT ID
    FROM @out
END