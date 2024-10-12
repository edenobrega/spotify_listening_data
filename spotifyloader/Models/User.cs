namespace SpotifyLoader.Models
{
    public struct User
    {
        [AutoIncrementColumn]
        public int ID { get; set; }
        public string Name { get; set; }
        public static bool operator ==(User Left, User Right)
        {
            return Left.Name == Right.Name;
        }

        public static bool operator !=(User Left, User Right)
        {
            return Left.Name != Right.Name;
        }
    }
}
