namespace SpotifyLoader.Models
{
    internal struct Platform
    {
        public int ID { get; set; }
        public string Name { get; set; }

        public static bool operator ==(Platform Left, Platform Right)
        {
            return Left.Name == Right.Name;
        }

        public static bool operator !=(Platform Left, Platform Right)
        {
            return Left.Name != Right.Name;
        }
    }
}
