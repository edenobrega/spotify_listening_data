namespace SpotifyLoader.Models
{
    [BulkTableName("Artist")]
    internal struct Artist
    {
        [AutoIncrementColumn]
        public int ID { get; set; }
        public string Name { get; set; }

        public static bool operator ==(Artist Left, Artist Right)
        {
            return Left.Name == Right.Name;
        }

        public static bool operator !=(Artist Left, Artist Right)
        {
            return Left.Name != Right.Name;
        }
    }
}
