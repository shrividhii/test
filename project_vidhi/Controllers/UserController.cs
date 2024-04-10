using Npgsql;
using project_vidhi.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Web.Mvc;

namespace project_vidhi.Controllers
{
    public class UserController : Controller
    {
        // GET: User
        public ActionResult Index()
        {
            return View();
        }
        public ActionResult Login()
        {
            return View();
        }
        public ActionResult UserProfile()
        {
            if (!string.IsNullOrEmpty(Session["Username"].ToString())) return RedirectToAction("Login");            
            else return View();
        }

        public ActionResult AdminProfile()
        {
            if (Session["Username"] == null)  return RedirectToAction("Login");            
            else return View();
        }

        public ActionResult Logout()
        {
            Session.Remove("Username");
            Session.Abandon();
            Session.Clear();
            return RedirectToAction("Login");
        }
        public ActionResult Details() 
        {
            List<LoginAuthentication> displayuser = new List<LoginAuthentication>();
            NpgsqlConnection conn = new NpgsqlConnection("Server=192.168.61.227; Port=5432; Database=Trainee_Test; User Id=Trainee; Password=Trainee@123;");
            conn.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = conn;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select * from public.\"Vidhi\"";

            NpgsqlDataReader reader = cmd.ExecuteReader();

            while(reader.Read())
            {
                var user = new LoginAuthentication();
                user.Username = Convert.ToString(reader["username"]);
                user.Password = Convert.ToString(reader["password"]);
                displayuser.Add(user);
            }
            
            return View(displayuser);
        }
    }
}