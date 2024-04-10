//using Npgsql;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Connectivity.DAL.Helper
{
    public sealed class PostgreSQLHelper
    {
        public PostgreSQLHelper()
        {
        }
        #region private utility methods & constructors
        /// <summary>
        /// This method is used to attach array of NpgsqlParameters to a NpgsqlCommand.
        /// 
        /// This method will assign a value of DbNull to any parameter with a direction of
        /// InputOutput and a value of null.  
        /// 
        /// This behavior will prevent default values from being used, but
        /// this will be the less common case than an intended pure output parameter (derived as InputOutput)
        /// where the user provided no input value.
        /// </summary>
        /// <param name="command">The command to which the parameters will be added</param>
        /// <param name="commandParameters">an array of NpgsqlParameters tho be added to command</param>
        private static void AttachParameters(NpgsqlCommand command, NpgsqlParameter[] commandParameters)
        {

            foreach (NpgsqlParameter p in commandParameters)
            {
                //check for derived output value with no value assigned
                if ((p.Direction == ParameterDirection.InputOutput) && (p.Value == null))
                {
                    p.Value = DBNull.Value;
                }

                command.Parameters.Add(p);
            }
        }
        //public static string EncryptionConnectionPg()
        //{
        //    ErrorLogs error = new ErrorLogs();

        //    var ConnectionPg = "";

        //    try
        //    {
        //        error.WriteProcessLog("Start Password Getting");
        //        var section = System.Web.Configuration.WebConfigurationManager.GetSection("secureAppSettings") as NameValueCollection;

        //        if (section != null && section["ConnectionPg"] != null) ConnectionPg = section["ConnectionPg"];
        //    }
        //    catch (Exception ex)
        //    {
        //        error.WriteErrorLogs(ex);
        //        error.WriteProcessLog("Exception in EncryptionConnectionPg Config: " + ex.ToString());
        //    }
        //    return ConnectionPg;
        //}

        /// <summary>
        /// This method opens (if necessary) and assigns a connection, transaction, command type and parameters 
        /// to the provided command.
        /// </summary>
        /// <param name="command">the NpgsqlCommand to be prepared</param>
        /// <param name="connection">a valid NpgsqlConnection, on which to execute this command</param>
        /// <param name="transaction">a valid NpgsqlTransaction, or 'null'</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of NpgsqlParameters to be associated with the command or 'null' if no parameters are required</param>
        private static void PrepareCommand(NpgsqlCommand command, NpgsqlConnection connection, NpgsqlTransaction transaction, CommandType commandType, string commandText, NpgsqlParameter[] commandParameters)
        {
            //if the provided connection is not open, we will open it
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            //associate the connection with the command
            command.Connection = connection;

            //set the command text (stored procedure name or SQL statement)
            command.CommandText = commandText;

            //if we were provided a transaction, assign it.
            if (transaction != null)
            {
                command.Transaction = transaction;
            }

            //set the command type
            command.CommandType = commandType;

            //attach the command parameters if they are provided
            if (commandParameters != null)
            {
                AttachParameters(command, commandParameters);
            }

            return;
        }


        #endregion private utility methods & constructors

        #region ExecuteNonQuery

        /// <summary>
        /// Execute a NpgsqlCommand (that returns no resultset and takes no parameters) against the database specified in 
        /// the connection string. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(connString, CommandType.StoredProcedure, "PublishOrders");
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a NpgsqlConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(string connectionString, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of NpgsqlParameters
            return ExecuteNonQuery(connectionString, commandType, commandText, (NpgsqlParameter[])null);
        }

        /// <summary>
        /// Execute a NpgsqlCommand (that returns no resultset) against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(connString, CommandType.StoredProcedure, "PublishOrders", new NpgsqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a NpgsqlConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(string connectionString, CommandType commandType, string commandText, params NpgsqlParameter[] commandParameters)
        {
            //create & open a NpgsqlConnection, and dispose of it after we are done.
            using (NpgsqlConnection cn = new NpgsqlConnection(connectionString))
            {
                cn.Open();

                //call the overload that takes a connection in place of the connection string
                return ExecuteNonQuery(cn, commandType, commandText, commandParameters);
            }
        }

        /// <summary>
        /// Execute a NpgsqlCommand (that returns no resultset) against the specified NpgsqlConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(conn, CommandType.StoredProcedure, "PublishOrders", new NpgsqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connection">a valid NpgsqlConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(NpgsqlConnection connection, CommandType commandType, string commandText, params NpgsqlParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            NpgsqlCommand cmd = new NpgsqlCommand();
            int retval = 0;
            try
            {
                PrepareCommand(cmd, connection, (NpgsqlTransaction)null, commandType, commandText, commandParameters);

                //finally, execute the command.
                retval = cmd.ExecuteNonQuery();

                // detach the NpgsqlParameters from the command object, so they can be used again.
                cmd.Parameters.Clear();

                // Manually close the connection 
            }
            catch
            {
                throw;
            }
            finally
            {
                connection.Close();
                connection.Dispose();
            }
            return retval;
        }

        #endregion ExecuteNonQuery

        #region ExecuteDataSet

        /// <summary>
        /// Execute a NpgsqlCommand (that returns a resultset and takes no parameters) against the database specified in 
        /// the connection string. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(connString, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a NpgsqlConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(string connectionString, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of NpgsqlParameters
            return ExecuteDataset(connectionString, commandType, commandText, (NpgsqlParameter[])null);
        }

        /// <summary>
        /// Execute a NpgsqlCommand (that returns a resultset) against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(connString, CommandType.StoredProcedure, "GetOrders", new NpgsqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a NpgsqlConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(string connectionString, CommandType commandType, string commandText, params NpgsqlParameter[] commandParameters)
        {
            //create & open a NpgsqlConnection, and dispose of it after we are done.
            using (NpgsqlConnection cn = new NpgsqlConnection(connectionString))
            {
                try
                {
                    cn.Open();

                    //call the overload that takes a connection in place of the connection string
                    return ExecuteDataset(cn, commandType, commandText, commandParameters);
                }
                catch (Exception ex)
                {
                    //Log.WriteErrorLogs(ex);

                    //((Npgsql.NpgsqlException)ex).ErrorSql

                    //string message = "";
                    //message += Environment.NewLine;
                    //message += "-----------------------------------------------------------";
                    //message += string.Format("Message Title : {0}", (((Npgsql.NpgsqlException)ex)).Message);
                    //message += Environment.NewLine;
                    //message += "-----------------------------------------------------------";
                    //message += Environment.NewLine;
                    //message += string.Format("TotalMilliseconds : {0}", 0);
                    //message += Environment.NewLine;
                    //message += "-----------------------------------------------------------";
                    //message += Environment.NewLine;
                    //message += "-----------------------------------------------------------";
                    //message += Environment.NewLine;
                    ////message += string.Format("Message: {0}", ((Npgsql.NpgsqlException)ex).ErrorSql);Hardcoded
                    //message += Environment.NewLine;

                    //message += "-----------------------------------------------------------";
                    //message += Environment.NewLine;
                    ////message += string.Format("Where: {0}", ((Npgsql.NpgsqlException)ex).Where);Hardcoded
                    //message += Environment.NewLine;
                    //message += "-----------------------------------------------------------";
                    //message += Environment.NewLine;

                }
                finally
                {
                    cn.Close();
                    cn.Dispose();
                }
                return null;
            }
        }

        /// <summary>
        /// Execute a NpgsqlCommand (that returns a resultset) against the specified NpgsqlConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(conn, CommandType.StoredProcedure, "GetOrders", new NpgsqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connection">a valid NpgsqlConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(NpgsqlConnection connection, CommandType commandType, string commandText, params NpgsqlParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            NpgsqlCommand cmd = new NpgsqlCommand();
            DataSet ds = new DataSet();
            try
            {
                PrepareCommand(cmd, connection, (NpgsqlTransaction)null, commandType, commandText, commandParameters);

                //create the DataAdapter & DataSet
                NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);
                //cmd.CommandTimeout = 20;
                //fill the DataSet using default values for DataTable names, etc.
                da.Fill(ds);

                // detach the NpgsqlParameters from the command object, so they can be used again.			
                cmd.Parameters.Clear();
            }
            catch (Exception ex)
            {
            }
            finally
            {
                connection.Close();
                connection.Dispose();
                //return the dataset
            }
            return ds;
        }

        #endregion ExecuteDataSet

        #region ExecuteReader

        /// <summary>
        /// this enum is used to indicate whether the connection was provided by the caller, or created by SqlHelper, so that
        /// we can set the appropriate CommandBehavior when calling ExecuteReader()
        /// </summary>
        private enum NpgsqlConnectionOwnership
        {
            /// <summary>Connection is owned and managed by SqlHelper</summary>
            Internal,
            /// <summary>Connection is owned and managed by the caller</summary>
            External
        }

        /// <summary>
        /// Create and prepare a NpgsqlCommand, and call ExecuteReader with the appropriate CommandBehavior.
        /// </summary>
        /// <remarks>
        /// If we created and opened the connection, we want the connection to be closed when the DataReader is closed.
        /// 
        /// If the caller provided the connection, we want to leave it to them to manage.
        /// </remarks>
        /// <param name="connection">a valid NpgsqlConnection, on which to execute this command</param>
        /// <param name="transaction">a valid NpgsqlTransaction, or 'null'</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of NpgsqlParameters to be associated with the command or 'null' if no parameters are required</param>
        /// <param name="connectionOwnership">indicates whether the connection parameter was provided by the caller, or created by SqlHelper</param>
        /// <returns>NpgsqlDataReader containing the results of the command</returns>
        private static NpgsqlDataReader ExecuteReader(NpgsqlConnection connection, NpgsqlTransaction transaction, CommandType commandType, string commandText, NpgsqlParameter[] commandParameters, NpgsqlConnectionOwnership connectionOwnership)
        {
            //create a command and prepare it for execution
            NpgsqlCommand cmd = new NpgsqlCommand();
            NpgsqlDataReader dr = null;
            try
            {
                PrepareCommand(cmd, connection, transaction, commandType, commandText, commandParameters);

                //create a reader
                // call ExecuteReader with the appropriate CommandBehavior
                if (connectionOwnership == NpgsqlConnectionOwnership.External)
                {
                    dr = cmd.ExecuteReader();
                }
                else
                {
                    dr = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                }

                // detach the NpgsqlParameters from the command object, so they can be used again.
                cmd.Parameters.Clear();
            }
            catch { }
            finally
            {

                connection.Close();
                connection.Dispose();
            }
            return dr;
        }

        /// <summary>
        /// Execute a NpgsqlCommand (that returns a resultset) against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  NpgsqlDataReader dr = ExecuteReader(connString, CommandType.StoredProcedure, "GetOrders", new NpgsqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a NpgsqlConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>a NpgsqlDataReader containing the resultset generated by the command</returns>
        public static NpgsqlDataReader ExecuteReader(string connectionString, CommandType commandType, string commandText, params NpgsqlParameter[] commandParameters)
        {
            //create & open a NpgsqlConnection
            NpgsqlConnection cn = new NpgsqlConnection(connectionString);
            cn.Open();

            try
            {
                //call the private overload that takes an internally owned connection in place of the connection string
                return ExecuteReader(cn, null, commandType, commandText, commandParameters, NpgsqlConnectionOwnership.Internal);
            }
            catch
            {
                //if we fail to return the SqlDatReader, we need to close the connection ourselves
                cn.Close();
                throw;
            }
        }

        /// <summary>
        /// Execute a NpgsqlCommand (that returns a resultset and takes no parameters) against the provided NpgsqlConnection. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  NpgsqlDataReader dr = ExecuteReader(conn, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param name="connection">a valid NpgsqlConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <returns>a NpgsqlDataReader containing the resultset generated by the command</returns>
        public static NpgsqlDataReader ExecuteReader(NpgsqlConnection connection, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of NpgsqlParameters
            return ExecuteReader(connection, commandType, commandText, (NpgsqlParameter[])null);
        }

        /// <summary>
        /// Execute a NpgsqlCommand (that returns a resultset) against the specified NpgsqlConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  NpgsqlDataReader dr = ExecuteReader(conn, CommandType.StoredProcedure, "GetOrders", new NpgsqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connection">a valid NpgsqlConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>a NpgsqlDataReader containing the resultset generated by the command</returns>
        public static NpgsqlDataReader ExecuteReader(NpgsqlConnection connection, CommandType commandType, string commandText, params NpgsqlParameter[] commandParameters)
        {
            //pass through the call to the private overload using a null transaction value and an externally owned connection
            return ExecuteReader(connection, (NpgsqlTransaction)null, commandType, commandText, commandParameters, NpgsqlConnectionOwnership.External);
        }

        /// <summary>
        /// Execute a NpgsqlCommand (that returns a resultset and takes no parameters) against the provided NpgsqlTransaction. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  NpgsqlDataReader dr = ExecuteReader(trans, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param name="transaction">a valid NpgsqlTransaction</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <returns>a NpgsqlDataReader containing the resultset generated by the command</returns>
        public static NpgsqlDataReader ExecuteReader(NpgsqlTransaction transaction, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of NpgsqlParameters
            return ExecuteReader(transaction, commandType, commandText, (NpgsqlParameter[])null);
        }

        /// <summary>
        /// Execute a NpgsqlCommand (that returns a resultset) against the specified NpgsqlTransaction
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///   NpgsqlDataReader dr = ExecuteReader(trans, CommandType.StoredProcedure, "GetOrders", new NpgsqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="transaction">a valid NpgsqlTransaction</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>a NpgsqlDataReader containing the resultset generated by the command</returns>
        public static NpgsqlDataReader ExecuteReader(NpgsqlTransaction transaction, CommandType commandType, string commandText, params NpgsqlParameter[] commandParameters)
        {
            //pass through to private overload, indicating that the connection is owned by the caller
            return ExecuteReader(transaction.Connection, transaction, commandType, commandText, commandParameters, NpgsqlConnectionOwnership.External);
        }
        #endregion ExecuteReader

        #region ExecuteScalar
        /// <summary>
        /// Execute a NpgsqlCommand (that returns a 1x1 resultset and takes no parameters) against the database specified in 
        /// the connection string. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(connString, CommandType.StoredProcedure, "GetOrderCount");
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a NpgsqlConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(string connectionString, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of NpgsqlParameters
            return ExecuteScalar(connectionString, commandType, commandText, (NpgsqlParameter[])null);
        }
        /// <summary>
        /// Execute a NpgsqlCommand (that returns a 1x1 resultset) against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(connString, CommandType.StoredProcedure, "GetOrderCount", new NpgsqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a NpgsqlConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(string connectionString, CommandType commandType, string commandText, params NpgsqlParameter[] commandParameters)
        {
            //create & open a NpgsqlConnection, and dispose of it after we are done.
            using (NpgsqlConnection cn = new NpgsqlConnection(connectionString))
            {
                cn.Open();

                //call the overload that takes a connection in place of the connection string
                return ExecuteScalar(cn, commandType, commandText, commandParameters);
            }
        }
        /// <summary>
        /// Execute a NpgsqlCommand (that returns a 1x1 resultset) against the specified NpgsqlConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(conn, CommandType.StoredProcedure, "GetOrderCount", new NpgsqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connection">a valid NpgsqlConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(NpgsqlConnection connection, CommandType commandType, string commandText, params NpgsqlParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            NpgsqlCommand cmd = new NpgsqlCommand();
            object retval = null;
            try
            {
                PrepareCommand(cmd, connection, (NpgsqlTransaction)null, commandType, commandText, commandParameters);

                //execute the command & return the results
                retval = cmd.ExecuteScalar();

                // detach the NpgsqlParameters from the command object, so they can be used again.
                cmd.Parameters.Clear();
            }
            catch (Exception ex) { }
            finally
            {
                connection.Close();
                connection.Dispose();
            }
            return retval;

        }

        #endregion ExecuteScalar
        public static string GetConnectString(string connectionname = "")
        {
            string connectiostr = "";
            try


            {
                if (connectionname == "")
                    connectiostr = ConfigurationManager.AppSettings["ConnectPGQuickmed"].ToString(); //"Server=3.12.97.34;Port=5432;Database=QNRU74;User Id=botdba;Password=6hwY!gef>8;CommandTimeout=10000;Pooling=false";
                else
                {
                    connectiostr = ConfigurationManager.AppSettings[connectionname].ToString();
                }
            }
            catch (Exception ex)
            {
                //Comman.WriteErrorLogs(ex);
            }
            return connectiostr;
        }
        public static DataSet GetProceduredata(string ConnectString, string inputjson, string procedurename, out string result, out string Error, string outjson = "'outjson'")
        {

            DataSet ds = new DataSet();
            result = "";
            Error = "";


            //string ConnectString = Configuration["ConnectionPg"].ToString();
            string query = "call " + procedurename + "('" + inputjson.ToString() + "'," + outjson + ")";

            int outjson_count = outjson.Split(',').Length + 1;
            string[] outjson_obj = outjson.Split(',');

            NpgsqlParameter[] sqlParameters = new NpgsqlParameter[outjson_count];//2
            sqlParameters[0] = new NpgsqlParameter("inputjson", SqlDbType.VarChar);
            sqlParameters[0].Value = Convert.ToString(inputjson);

            for (int i = 0; i < outjson_obj.Length; i++)
            {
                sqlParameters[i + 1] = new NpgsqlParameter(outjson_obj[i], NpgsqlTypes.NpgsqlDbType.Refcursor);
                sqlParameters[i + 1].Value = Convert.ToString(outjson_obj[i]);
                sqlParameters[i + 1].Direction = ParameterDirection.InputOutput;
                sqlParameters[i + 1].NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Refcursor;
            }

            ds = PostgreSQLHelper.ExecuteDataset_Procedure(ConnectString, CommandType.Text, query, sqlParameters);
            for (int i = 0; i < ds.Tables.Count; i++)
            {
                if (i == 0)
                {
                    ds.Tables[i].TableName = "Table";
                }
                else
                {
                    ds.Tables[i].TableName = "Table" + i;
                }

            }


            return ds;
        }
        public static DataSet ExecuteDataset_procedure(string connectionString, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of NpgsqlParameters
            return ExecuteDataset_Procedure(connectionString, commandType, commandText, (NpgsqlParameter[])null);
        }
        public static DataSet ExecuteDataset_Procedure(string connectionString, CommandType commandType, string commandText, params NpgsqlParameter[] commandParameters)
        {
            DataSet ds = new DataSet();
            //create & open a NpgsqlConnection, and dispose of it after we are done.
            using (NpgsqlConnection cn = new NpgsqlConnection(connectionString))
            {
                cn.Open();
                NpgsqlTransaction tran = cn.BeginTransaction();

                DataTable dt = new DataTable();
                NpgsqlCommand command = new NpgsqlCommand(commandText, cn);
                command.CommandType = CommandType.Text;
                command.Parameters.AddRange(commandParameters);
                command.ExecuteNonQuery();
                NpgsqlDataAdapter da;
                int i = 0;
                foreach (NpgsqlParameter parm in commandParameters)
                {
                    if (parm.NpgsqlDbType == NpgsqlTypes.NpgsqlDbType.Refcursor)
                    {
                        string parm_val = string.Format("FETCH ALL IN \"{0}\"", parm.Value.ToString());
                        da = new NpgsqlDataAdapter(parm_val.Trim().ToString(), cn);
                        ds.Tables.Add(parm_val);
                        da.Fill(ds.Tables[i]);
                        i++;
                    }
                }
                tran.Commit();
                //call the overload that takes a connection in place of the connection string
                cn.Close();
                cn.Dispose();

            }
            return ds;
        }
    }
}