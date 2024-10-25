CREATE     PROCEDURE [dbo].[DumpData]
AS
BEGIN
    DELETE FROM dbo.AlbumToArtist
    DELETE FROM dbo.Album
    DELETE FROM dbo.Artist
    DELETE FROM dbo.AudioFeature
    DELETE FROM dbo.[Data]
    DELETE FROM dbo.Platform
    DELETE FROM dbo.Reason
    DELETE FROM dbo.Song
    DELETE FROM dbo.[User]

    DBCC CHECKIDENT ('[Data]', RESEED, 0)
    DBCC CHECKIDENT ('[Album]', RESEED, 0)
    DBCC CHECKIDENT ('[Artist]', RESEED, 0)
    DBCC CHECKIDENT ('[Song]', RESEED, 0)
    DBCC CHECKIDENT ('[Platform]', RESEED, 0)
    DBCC CHECKIDENT ('[Reason]', RESEED, 0)
END