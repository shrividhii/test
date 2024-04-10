using Connectivity.DAL.Helper;
using Npgsql;
using System;
using System.Configuration;
using System.Data;
using System.Web.Mvc;
namespace project_vidhi.Controllers
{
    public class LoginController : Controller
    {
        [HttpPost]
        public JsonResult Authenticate(string username, string password)
        {
            Session["Username"] = username;

            #region Not using from v1.2
            //if (username == "vidhi" && password == "vidhi1234")
            //{
            //    return Json(new { Success = true });
            //}
            //else if (username == "admin" && password == "admin")
            //    return Json(new { Success = true });
            //}
            //else
            //    return Json(new { Success = false }); 
            #endregion

            #region
            string ConnectString = ConfigurationManager.ConnectionStrings["ConnectionPg"].ConnectionString;

            string result = string.Empty;
            string error = string.Empty;
            DataTable dt = new DataTable();
            try
            {

                dt = PostgreSQLHelper.GetProceduredata(ConnectString, " ", "public.checklogin", out result, out error, "'outjson'").Tables[0];

                //dt = PostgreSQLHelper.ExecuteDataset(connectionString, CommandType.Text, "SELECT * FROM public.\"Vidhi\" WHERE username = '"+ username + "' AND password = '"+ password + "'").Tables[0];

                if (dt != null && dt.Rows.Count > 0)
                {
                    return Json(new { Success = true });
                }
                else
                {
                    return Json(new { Success = false });
                }
            }
            catch (Exception ex)
            {
                // var error = ex.InnerException;
                var error1 = ex.StackTrace;
                return Json(new { Success = false });
            }
        }
        #endregion


        #region
        //        bool isValid = checkLogin(username, password);

        //            if (isValid) { return Json(new { Success = true }); }
        //            else { return Json(new { Success = false }); }

        //        }

        //        private bool checkLogin(string username, string password)
        //        { 
        //           bool isValid = false;

        //          using (NpgsqlConnection conn = new NpgsqlConnection("Server=192.168.61.227; Port=5432; Database=Trainee_Test; User Id=Trainee; Password=Trainee@123;"))
        //            {
        //        using (NpgsqlCommand cmd = new NpgsqlCommand("public.checkuser(:u_name, :pwd)", conn))
        //        {
        //            cmd.CommandType = CommandType.Text;

        //            cmd.Parameters.AddWithValue("u_name", DbType.String).Value = username;
        //            cmd.Parameters.AddWithValue("pwd", DbType.String).Value = password;

        //            cmd.Parameters.Add(new NpgsqlParameter
        //            {
        //                ParameterName = "result",
        //                Direction = ParameterDirection.Output,
        //                DbType = DbType.Boolean
        //            });

        //            conn.Open();
        //            cmd.ExecuteNonQuery();

        //            isValid = (bool)cmd.Parameters["result"].Value;
        //        }
        //    }
        //    return isValid;
        //}
        #endregion
    }
}
