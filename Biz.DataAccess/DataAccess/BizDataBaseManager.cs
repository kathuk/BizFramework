using System;
using System.Data.SqlClient;
using System.Data;

namespace Biz.DataAccess
{
    public class BizDataBaseManager
    {
        
        private static BizDataBaseManager manager = null;
        internal SqlConnection sqlConnect = null; 


        public static void internalCreate(string userName, string password, string dataSource)
        {
            manager = new BizDataBaseManager();
            string strConnect =
                "Data Source=" + dataSource +
                ";User Id=" + userName +
                ";Password=" + password + ";";

            manager.sqlConnect = new SqlConnection(strConnect);
            
            manager.sqlConnect.Open();
            manager.sqlConnect.Close();
        }


        public static BizDataBaseManager getDefault()
        {
            if(manager==null){
                 throw new BizDataAccessException("BizDataBaseManager must be created properly!");
            }
            return manager;
        }


        public void AttacheDatabase(string path, string dbName)
        {
            try
            {
                sqlConnect.Open();
                SqlCommand cmd = new SqlCommand("sp_attach_db", sqlConnect);
                cmd.CommandType = CommandType.StoredProcedure;

                path.Replace("\\", "\\\\");
                cmd.Parameters.Add(new SqlParameter("@dbname", dbName));
                cmd.Parameters.Add(new SqlParameter("@filename1", path + "\\" + dbName + ".mdf"));
                cmd.Parameters.Add(new SqlParameter("@filename2", path + "\\" + dbName + "_log.LDF"));

                SqlDataReader myReader = cmd.ExecuteReader();
                myReader.Close();
                sqlConnect.Close();
            }
            catch (Exception objError)
            {
                sqlConnect.Close();
                throw objError;
            }
        }


        public void DetacheDatabase(string dbName)
        {
            try
            {
                sqlConnect.Open();
                SqlCommand cmd = new SqlCommand("sp_detach_db", sqlConnect);
                cmd.CommandType = CommandType.StoredProcedure;

                cmd.Parameters.Add(new SqlParameter("@dbname", dbName));

                SqlDataReader myReader = cmd.ExecuteReader();
                myReader.Close();
                sqlConnect.Close();
            }
            catch (Exception objError)
            {
                sqlConnect.Close();
                throw objError;
            }
        }
    }

}
