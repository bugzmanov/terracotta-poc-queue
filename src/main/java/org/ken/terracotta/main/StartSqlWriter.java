package org.ken.terracotta.main;

import org.ken.terracotta.writer.SqlWriter;
import org.ken.terracotta.queue.QueueManager;
import org.ken.terracotta.config.Config;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverManager;

/**
 * Date: 31.05.2009
 *
 * @author: bagmanov
 */
public class StartSqlWriter {
    static {
        try{
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] argz){
        try {
            new SqlWriter(getConnection(), new QueueManager(), Config.BATCH_SIZE).run();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://localhost/test?user=root&password=pRi4uDa24rb");
    }
}
