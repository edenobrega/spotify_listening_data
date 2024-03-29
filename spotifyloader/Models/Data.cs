﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace spotifyloader.Models
{
    internal class Data
    {
        public int ID { get; set; }
        public DateTime EndTime { get; set; }
        public float ms_played { get; set; }
        public int ReasonStart { get; set; }
        public int ReasonEnd { get; set; }
        public int PlatformID { get; set; }
        public int SongID { get; set; }
        public string Country { get; set; }
        public bool? Skipped { get; set; }
        public bool? Offline { get; set; }
        public DateTime? OfflineTimestamp { get; set; }
        public bool IncognitoMode { get; set; }
        public bool Shuffle { get; set; }
        public int UserID { get; set; }
    }
}
