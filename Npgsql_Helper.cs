/* Npgsql helper library for C# Copyright (C) 2015 Kionier
For guides on how to use this library, please go to http://kionier.com/npgsql-helper/.
This software is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License (GPL) or GNU Lesser General Public License (LGPL) as published by the Free Software Foundation, either version 2 or later.
This software is distributed WITHOUT ANY WARRANTY; see the GNU General Public License for more details. http://www.gnu.org/licenses/.
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Npgsql;
using System.Data;

namespace Npgsql_Helper_
{
    public class Npgsql_Helper
    {
        public Npgsql_Helper(String connectionString)
        {
            _conn = connectionString;
        }

        //####  private methods ###
        private string _conn = null;

        private NpgsqlCommand GetCommand(string query, NpgsqlParameter[] npgsqlParameters, CommandType commandType)
        {
            NpgsqlConnection conn = new NpgsqlConnection(_conn);
            conn.UseSslStream = false;
            conn.Open();

            query = query.ToLower();

            NpgsqlCommand command = new NpgsqlCommand(query, conn);
            command.CommandType = commandType;
    
            if (npgsqlParameters is NpgsqlParameter[])
            {
                command.Parameters.AddRange(npgsqlParameters);
            }

            return command;
        }

        //#### public methods ####

        public  long ExecuteNonQuery(string query, NpgsqlParameter[] npgsqlParameters)
        {
            return ExecuteNonQuery(CommandType.StoredProcedure, query, npgsqlParameters);
        }

        public  long ExecuteNonQuery(CommandType commandType, string query, NpgsqlParameter[] npgsqlParameters)
        {

            using (NpgsqlCommand command = GetCommand(query, npgsqlParameters, commandType))
            {
                Int32 rowsaffected;

                try
                {
                    rowsaffected = command.ExecuteNonQuery();
                    return rowsaffected;
                }
                catch (Exception Ex)
                {
                    throw Ex;
                }

                finally
                {
                    command.Connection.Close();
                }
            }

        }

        public  long ExecuteNonQuery(CommandType commandType, string query)
        {
            return ExecuteNonQuery(commandType, query, null);
        }

        public  object ExecuteScalar(string query, NpgsqlParameter[] npgsqlParameters)
        {
            return ExecuteScalar(CommandType.StoredProcedure, query, npgsqlParameters);
        }

        public  object ExecuteScalar(CommandType commandType, string query, NpgsqlParameter[] npgsqlParameters)
        {
            using (NpgsqlCommand command = GetCommand(query, npgsqlParameters, commandType))
            {
                object result;

                try
                {
                    result = command.ExecuteScalar();
                    return result;
                }
                catch (Exception Ex)
                {
                    throw Ex;
                }
                finally
                {
                    command.Connection.Close();
                }
            }
        }

        public  object ExecuteScalar(CommandType commandType, string query)
        {
            return ExecuteScalar(commandType, query, null);
        }

        public  DataTable[] ExecuteDataset(string query, NpgsqlParameter[] npgsqlParameters)
        {
            return ExecuteDataset(CommandType.StoredProcedure, query, npgsqlParameters);

        }

        public  DataTable[] ExecuteDataset(CommandType commandType, string query, NpgsqlParameter[] npgsqlParameters)
        {
            using (NpgsqlCommand command = GetCommand(query, npgsqlParameters, commandType))
            {
                try
                {
                    DataSet myDS = new DataSet();

                    NpgsqlTransaction t = command.Connection.BeginTransaction();

                    NpgsqlDataAdapter da = new NpgsqlDataAdapter(command);
                    da.Fill(myDS);

                    t.Commit();

                    DataTable[] tables = new DataTable[myDS.Tables.Count];

                    myDS.Tables.CopyTo(tables, 0);

                    return tables;

                }
                catch (Exception Ex)
                {
                    throw Ex;
                }


                finally
                {
                    command.Connection.Close();
                }
            }
        }

        public  DataTable[] ExecuteDataset(CommandType commandType, string query)
        {
            return ExecuteDataset(commandType, query, null);
        }
    }

}
