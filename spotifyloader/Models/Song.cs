using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace spotifyloader.Models
{
    internal class Song
    {
        public int ID { get; set; }
        public string Name { get; set; }
        public int ArtistID { get; set; }
        public int AlbumID { get; set; }
        public string Track_Uri { get; set; }
    }
}
