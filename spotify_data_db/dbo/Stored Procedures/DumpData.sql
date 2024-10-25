
CREATE PROCEDURE [dbo].[DumpData]
AS
BEGIN
    return
    delete from dbo.[Data]
    delete from dbo.Song
    delete from dbo.Album
    delete from dbo.Artist
    delete from dbo.Platform
    delete from dbo.Reason
    DBCC CHECKIDENT ('[Platform]', RESEED, 0)
    DBCC CHECKIDENT ('[Album]', RESEED, 0)
    DBCC CHECKIDENT ('[Artist]', RESEED, 0)
    DBCC CHECKIDENT ('[Song]', RESEED, 0)
    DBCC CHECKIDENT ('[Data]', RESEED, 0)
    DBCC CHECKIDENT ('[Reason]', RESEED, 0)
END