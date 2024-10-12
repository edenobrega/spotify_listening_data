namespace SpotifyLoader.Models
{
    internal struct Song
    {
        public int ID { get; set; }
        public string Name { get; set; }
        public int ArtistID { get; set; }
        public int AlbumID { get; set; }
        public string Track_Uri { get; set; }

        public static bool operator ==(Song Left, Song Right)
        {
            return Left.Name == Right.Name && Left.Track_Uri == Right.Track_Uri;
        }

        public static bool operator !=(Song Left, Song Right)
        {
            return Left.Name == Right.Name && Left.Track_Uri != Right.Track_Uri;
        }
    }
}
