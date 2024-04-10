using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using project_vidhi.Models;
using Newtonsoft.Json;
using System.Net;
using System.ComponentModel.DataAnnotations;

namespace project_vidhi.Controllers
{
    public class HomeController : Controller
    {
        public ActionResult Index()
        {
            return View();
        }
       
    }
}