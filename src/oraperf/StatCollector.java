/* 
 * Copyright (C) 2017 Tribein
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package oraperf;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.BlockingQueue;

public class StatCollector
        extends Thread {

    SL4JLogger lg;
    private final DateTimeFormatter DATEFORMAT = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss");
    private final int threadType;
    private final String dbUserName;
    private final String dbPassword;
    private final String dbConnectionString;
    private final String dbUniqueName;
    private final String dbHostName;
    private Connection con;
    private final BlockingQueue<OraCkhMsg> ckhQueue;
    private boolean shutdown = false;

    public StatCollector(String inputString, String dbUSN, String dbPWD, ComboPooledDataSource ckhDS, int runTType, BlockingQueue<OraCkhMsg> queue) {
        dbConnectionString = inputString;
        dbUniqueName = inputString.split("/")[1];
        dbHostName = inputString.split(":")[0];
        dbUserName = dbUSN;
        dbPassword = dbPWD;
        threadType = runTType;
        ckhQueue = queue;
    }

    private void cleanup() {
        try {
            if ((con != null) && (!con.isClosed())) {
                con.close();
            }
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                    + dbConnectionString + "\t" + "Error durring ORADB resource cleanups!"
            );

            e.printStackTrace();
        }
    }

    private void openConnection() {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            con = DriverManager.getConnection("jdbc:oracle:thin:@" + dbConnectionString, dbUserName, dbPassword);
            con.setAutoCommit(false);
        } catch (ClassNotFoundException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                    + "Cannot load Oracle driver!"
            );
            shutdown = true;
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                    + "Cannot initiate connection to target Oracle database: " + dbConnectionString
            );

            shutdown = true;
        }
    }

    public void run() {
        lg = new SL4JLogger();

        openConnection();
        if (!shutdown) {
            try {
                switch (threadType) {
                    case 0:
                        WaitsCollector waits = new WaitsCollector(con, ckhQueue, dbUniqueName, dbHostName, dbConnectionString);
                        waits.RunCollection();
                        break;
                    case 1:
                        SesCollector ses = new SesCollector(con, ckhQueue, dbUniqueName, dbHostName, dbConnectionString);
                        ses.RunCollection();
                        break;
                    case 2:
                        SysCollector sys = new SysCollector(con, ckhQueue, dbUniqueName, dbHostName, dbConnectionString);
                        sys.RunCollection();
                        break;
                    case 3:
                        SQLCollector sql = new SQLCollector(con, ckhQueue, dbUniqueName, dbHostName, dbConnectionString);
                        sql.RunCollection();
                        break;
                    default:
                        lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                                + dbConnectionString + "\t" + "Unknown thread type provided!"
                        );
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            cleanup();
        }
    }
}
