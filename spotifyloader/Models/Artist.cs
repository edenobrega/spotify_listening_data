namespace SpotifyLoader.Models
{
    internal struct Artist
    {
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
