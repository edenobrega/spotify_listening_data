using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SpotifyLoader.Models.API
{
    internal class TrackResponse
    {
        public string uri { get; set; }
        public int duration_ms { get; set; }
        public InnerTrackResponseAlbum album { get; set; }
        public InnerTrackResponseArtist[] artists { get; set; }
    }

    internal class InnerTrackResponseAlbum
    {
        public string id { get; set; }
        public string uri { get; set; }
        public string name { get; set; }
        public string release_date { get; set; }
        public string album_type { get; set; }
        public InnerTrackResponseArtist[] artists { get; set; }
    }

    internal class InnerTrackResponseArtist
    {
        public string uri { get; set; }
        public string name { get; set; }
    }
}
