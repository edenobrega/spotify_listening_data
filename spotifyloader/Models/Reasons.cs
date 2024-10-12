using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SpotifyLoader.Models
{
    internal struct Reason
    {
        public int ID { get; set; }
        public string Name { get; set; }
        public static bool operator ==(Reason Left, Reason Right)
        {
            return Left.Name == Right.Name;
        }

        public static bool operator !=(Reason Left, Reason Right)
        {
            return Left.Name != Right.Name;
        }
    }
}
