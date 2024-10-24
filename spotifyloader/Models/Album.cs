﻿namespace SpotifyLoader.Models
{
    internal struct Album
    {
        [AutoIncrementColumn]
        public int ID { get; set; }
        public string Name { get; set; }
        public int ArtistID { get; set; }
        public string URI { get; set; }
        [ExcludeFromBulk]
        public string ArtistName { get; set; }
        public static bool operator ==(Album Left, Album Right)
        {
            return Left.URI == Right.URI;
        }

        public static bool operator !=(Album Left, Album Right)
        {
            return Left.URI != Right.URI;
        }
    }
}
