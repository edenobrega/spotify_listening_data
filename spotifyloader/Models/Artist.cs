namespace SpotifyLoader.Models
{
    [BulkTableName("Artist")]
    internal struct Artist
    {
        [AutoIncrementColumn]
        public int ID { get; set; }
        public string Name { get; set; }
        public string URI { get; set; }

        public static bool operator ==(Artist Left, Artist Right)
        {
            return Left.URI == Right.URI;
        }

        public static bool operator !=(Artist Left, Artist Right)
        {
            return Left.URI != Right.URI;
        }
    }
}
