using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Npgsql_Helper_;
using System.Data;
using Npgsql;

namespace Test_examples
{
    class Program
    {
        static void Main(string[] args)
        {

            //#### Initialise #####
            string conn = @"Server=dbserver;Database=dellstore2;User Id=dellstore;Password=pa$$word;";//sslmode=disable;ssl=false;Protocol=3;Pooling=true;MaxPoolSize=30";
            Npgsql_Helper dellstoreDB = new Npgsql_Helper(conn);

            // #### a simple query ####
            {
                string sql = "select * from products where special = :special";

                NpgsqlParameter[] p = new NpgsqlParameter[1];
                p[0] = new NpgsqlParameter("special", DbType.Int16);
                p[0].Value = 1;

                DataTable result = dellstoreDB.ExecuteDataset(CommandType.Text, sql, p)[0];
                foreach (DataRow row in result.Rows)
                {
                    //Do something ....
                }
            }

            // #### a scalar query ####
            {
                string sql = "select count(*) from products where special = :special";

                NpgsqlParameter[] p = new NpgsqlParameter[1];
                p[0] = new NpgsqlParameter(":special", DbType.Int16);
                p[0].Value = 1;

                Object result = dellstoreDB.ExecuteScalar(CommandType.Text, sql, p);
            }

            // #### a simple update query ####
            {
                string sql = "update products  set special = 1  where special = :special";

                NpgsqlParameter[] p = new NpgsqlParameter[1];
                p[0] = new NpgsqlParameter("special", DbType.Int16);
                p[0].Value = 1;

                long l = dellstoreDB.ExecuteNonQuery(CommandType.Text, sql, p);
                
                
            }
        }
    }
}
