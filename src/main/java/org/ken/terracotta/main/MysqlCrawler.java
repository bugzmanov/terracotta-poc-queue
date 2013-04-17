package org.ken.terracotta.main;

import java.sql.*;
import java.util.Date;

/**
 * This is just crawler for mysql test table. It shows how amount of new records in table every 10 secs 
 *
 * @author: Bagmanov
 * Date: 02.06.2009
 */
public class MysqlCrawler {
    static {
        try{
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] argz) {
        try {
            Connection connection = DriverManager.getConnection("jdbc:mysql://localhost/test?user=root&password=pRi4uDa24rb");

            int previous = 0;
	        Statement statement = connection.createStatement();
	        statement.executeUpdate("DROP TABLE IF EXISTS terra_test;");
	        statement.executeUpdate("create table terra_test (id bigint(20) NOT NULL auto_increment, test varchar(100) default NULL, PRIMARY KEY  (id)) ENGINE=MyISAM");
            while(true) {
                statement = connection.createStatement();
                final ResultSet resultSet = statement.executeQuery("select count(*) from terra_test");
                resultSet.next();
                int count = resultSet.getInt(1);
                System.out.println("" + new Date() + " - " + (count - previous));
                Thread.sleep(10000L);
                previous = count;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
