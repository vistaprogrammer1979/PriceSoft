/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.accumed.pricing;

/**
 *
 * @author Basel
 */
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DBConnectionManager {
    private static final Logger LOGGER = Logger.getLogger(DBConnectionManager.class.getName());
    private static DataSource dataSource;

    // Initialize the DataSource only once in a static block
    static {
        try {
            Context initCtx = new InitialContext();
//            Context envCtx = (Context) initCtx.lookup("java:comp/env");
//            dataSource = (DataSource) envCtx.lookup("jdbc/pricingDB");
             dataSource = (DataSource) initCtx.lookup("java:comp/env/jdbc/pricingDB");
        } catch (NamingException ex) {
            LOGGER.log(Level.SEVERE, "JNDI lookup failed for pricingDB DataSource.", ex);
        }
    }

    /**
     * Retrieves a connection from the connection pool.
     * @return a Connection object or null if the connection cannot be established.
     * @throws java.sql.SQLException
     */
    public static Connection getPriceDB()  {
        if (dataSource == null) {
            LOGGER.log(Level.SEVERE, "DataSource is not initialized.");
            return null;
        }

        try {
            Connection con = dataSource.getConnection();

            // Verify the transaction isolation level
            if (con.getTransactionIsolation() != Connection.TRANSACTION_READ_UNCOMMITTED) {
                LOGGER.log(Level.SEVERE, "DB connection is NOT READ_UNCOMMITTED.");
            } else {
                LOGGER.log(Level.INFO, "DB connection is READ_UNCOMMITTED.");
            }

            return con;
            
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Failed to establish a database connection.", ex);
            return null;
        }
    }
}
