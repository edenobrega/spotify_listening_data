namespace SpotifyLoader.Models
{
    internal struct Song
    {
        [AutoIncrementColumn]
        public int ID { get; set; }
        public string Name { get; set; }
        [ExcludeFromBulk]
        public string ArtistName { get; set; }
        [ExcludeFromBulk]
        public int ArtistID { get; set; }
        [ExcludeFromBulk]
        public string AlbumName { get; set; }
        [ExcludeFromBulk]
        public int AlbumID { get; set; }
        public string Track_Uri { get; set; }

        public static bool operator ==(Song Left, Song Right)
        {
            return Left.Track_Uri == Right.Track_Uri;
        }

        public static bool operator !=(Song Left, Song Right)
        {
            return Left.Track_Uri != Right.Track_Uri;
        }
    }
}
