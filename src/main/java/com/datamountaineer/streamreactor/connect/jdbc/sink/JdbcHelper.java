/**
 * Copyright 2015 Datamountaineer.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.datamountaineer.streamreactor.connect.jdbc.sink;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcHelper {

  private static final Logger logger = LoggerFactory.getLogger(JdbcHelper.class);

  /**
   * Returns the database for the current connection
   *
   * @param connection - The database URI
   * @param user       - The database user name
   * @param password   - The database password
   * @return The database name
   * @throws SQLException
   */
  public static String getDatabase(final String connection, final String user, final String password) throws SQLException {
    Connection con = null;

    try {
      if (user != null) {
        con = DriverManager.getConnection(connection, user, password);
      } else {
        con = DriverManager.getConnection(connection);
      }

      return con.getCatalog();
    } finally {
      if (con != null) {
        try {
          con.close();
        } catch (Throwable t) {
          logger.error(t.getMessage(), t);
        }
      }
    }
  }
}