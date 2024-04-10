using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Web;

namespace project_vidhi.Models
{
    public class LoginAuthentication
    {
        [Key]
        public String Username { get; set; }

        public String Password { get; set; }
    }
}