using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace spotifyloader.Models.API
{
    public class SongData
    {
        public DateTime ts { get; set; }
        public string username { get; set; }
        public float ms_played { get; set; }
        public string platform { get; set; }
        public string conn_country { get; set; }
        public string ip_addr_decrypted { get; set; }
        public string user_agent_decrypted { get; set; }
        public string master_metadata_track_name { get; set; }
        public string master_metadata_album_artist_name { get; set; }
        public string master_metadata_album_album_name { get; set; }
        public string spotify_track_uri { get; set; }
        public string episode_name { get; set; }
        public string episode_show_name { get; set; }
        public string spotify_episode_uri { get; set; }
        public string reason_start { get; set; }
        public string reason_end { get; set; }
        public bool shuffle { get; set; }
        public bool? skipped { get; set; }
        public bool? offline { get; set; }
        public double? offline_timestamp { get; set; }
        public bool incognito_mode { get; set; }

    }
}
