/*
 * Copyright (c) 2024 CUBRID Corporation.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors
 *   may be used to endorse or promote products derived from this software without
 *   specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package turbograph.jdbc.test;

import java.io.File;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

import java.sql.DriverManager;

import turbograph.jdbc.driver.TURBOGRAPHDriver;
import turbograph.jdbc.driver.TURBOGRAPHStatement;

public class TurboGraphTest {

    private final boolean USE_INTERNAL_CLASS = false;
    private final String CLASS_PATH = this.getClass().getResource("").getPath();
    private final String jdbcDriverPath = CLASS_PATH + "JDBC-1.0.0.0008-turbograph.jar";

    private final String driverClassName = "turbograph.jdbc.driver.TURBOGRAPHDriver";
    private final String URL = "jdbc:turbograph:192.168.2.54:30000:demodb:::";
    private Driver driver = null;
    private static Connection connection = null;

    private static ArrayList<String> testQuery = new ArrayList<>();

    private static void addQuery() {
        String query;
        query =
                "MATCH (n:CUSTOMER) WITH MIN(n.C_ACCTBAL) as minval, MAX(n.C_ACCTBAL) as maxval return minval, maxval";
        testQuery.add(query);

        query =
                "MATCH (n:CUSTOMER) WHERE n.C_ACCTBAL >= 0-9.98 AND n.C_ACCTBAL <= 1.017 RETURN COUNT(*) AS node_count";
        testQuery.add(query);

        query =
                "MATCH (c:CUSTOMER)-[r:CUST_BELONG_TO]->(n:NATION) WHERE n.N_NATIONKEY > 14 AND c.C_CUSTKEY < 100 RETURN c, r, n";
        testQuery.add(query);

        query =
                "MATCH (c:CUSTOMER)-[r:CUST_BELONG_TO]->(n:NATION) WHERE n.N_NATIONKEY > 14 AND c.C_CUSTKEY < 100 RETURN n, r, c";
        testQuery.add(query);

        String tpch_q3 =
                "MATCH (l:LINEITEM)-[:IS_PART_OF]->(ord:ORDERS)-[:MADE_BY]->(c:CUSTOMER)"
                        + " WHERE c.C_MKTSEGMENT = 'FURNITURE'"
                        + " AND ord.O_ORDERDATE < date('1995-03-07')"
                        + " AND l.L_SHIPDATE > date('1995-03-07')"
                        + " RETURN ord.O_ORDERKEY,"
                        + " sum(l.L_EXTENDEDPRICE*(1-l.L_DISCOUNT)) AS revenue,"
                        + " ord.O_ORDERDATE AS ord_date,"
                        + " ord.O_SHIPPRIORITY"
                        + " ORDER BY revenue DESC, ord_date limit 50";

        testQuery.add(tpch_q3);

        String tpch_q3_add_node_edge =
                "MATCH (l:LINEITEM)-[r:IS_PART_OF]->(ord:ORDERS)-[r1:MADE_BY]->(c:CUSTOMER)"
                        + " WHERE c.C_MKTSEGMENT = 'FURNITURE'"
                        + " AND ord.O_ORDERDATE < date('1995-03-07')"
                        + " AND l.L_SHIPDATE > date('1995-03-07')"
                        + " RETURN l, r, ord, r1, c, ord.O_ORDERKEY,"
                        + " sum(l.L_EXTENDEDPRICE*(1-l.L_DISCOUNT)) AS revenue,"
                        + " ord.O_ORDERDATE AS ord_date, "
                        + " ord.O_SHIPPRIORITY"
                        + " ORDER BY revenue DESC, ord_date limit 50";

        testQuery.add(tpch_q3_add_node_edge);

        query =
                "MATCH (item:LINEITEM)"
                        + " WHERE item.L_EXTENDEDPRICE <= 100010.00"
                        + " RETURN item.L_RETURNFLAG AS ret_flag,"
                        + " item.L_LINESTATUS AS line_stat,"
                        + " sum(item.L_QUANTITY) AS sum_qty,"
                        + " sum(item.L_EXTENDEDPRICE) AS sum_base_price,"
                        + " sum(item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT)) AS sum_disc_price,"
                        + " sum(item.L_EXTENDEDPRICE*(1 - item.L_DISCOUNT)*(1 + item.L_TAX)) AS sum_charge,"
                        + " avg(item.L_QUANTITY) AS avg_qty,"
                        + " avg(item.L_EXTENDEDPRICE) AS avg_price,"
                        + " avg(item.L_DISCOUNT) AS avg_disc,"
                        + " COUNT(*) AS count_order"
                        + " ORDER BY ret_flag, line_stat";

        testQuery.add(query);

        query =
                "MATCH (n:CUSTOMER)-[r:CUST_BELONG_TO]-(n1:NATION)"
                        + " WHERE n.C_CUSTKEY < 100 and n.C_NATIONKEY > 10"
                        + " RETURN n,r,n1 limit 1";

        testQuery.add(query);

        String tpch_q1 =
                "MATCH (item:LINEITEM)"
                        + " WHERE item.L_SHIPDATE <= date('1998-08-25')"
                        + " RETURN"
                        + " item.L_RETURNFLAG AS ret_flag,"
                        + " item.L_LINESTATUS AS line_stat,"
                        + " sum(item.L_QUANTITY) AS sum_qty,"
                        + " sum(item.L_EXTENDEDPRICE) AS sum_base_price,"
                        + " sum(item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT)) AS sum_disc_price,"
                        + " sum(item.L_EXTENDEDPRICE*(1 - item.L_DISCOUNT)*(1 + item.L_TAX)) AS sum_charge,"
                        + " avg(item.L_QUANTITY) AS avg_qty,"
                        + " avg(item.L_EXTENDEDPRICE) AS avg_price,"
                        + " avg(item.L_DISCOUNT) AS avg_disc,"
                        + " COUNT(*) AS count_order"
                        + " ORDER BY ret_flag, line_stat";
        testQuery.add(tpch_q1);
    }

    private void connection() {

        if (!USE_INTERNAL_CLASS) {
            try {
                System.out.println("jdbcDriverPath : " + jdbcDriverPath);
                ClassLoader cl = getClassLoader(jdbcDriverPath);
                Class<Driver> result = (Class<Driver>) cl.loadClass(driverClassName);
                driver = (Driver) result.newInstance();
            } catch (ClassNotFoundException e1) {
                e1.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }

            Properties properties = new Properties();
            properties.put("user", "dba");
            properties.put("password", "");
            try {
                connection = driver.connect(URL, properties);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            connection = getConnection();
        }
    }

    private void meta() {
        try {

            boolean autoCommit = true;
            connection.setAutoCommit(autoCommit);

            DatabaseMetaData metadata = connection.getMetaData();
            readMetaInfo(metadata);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void close(boolean doRollback) {
        if (this.connection != null) {
            if (doRollback) {
                try {
                    if (!connection.isClosed() && !connection.getAutoCommit()) {
                        connection.rollback();
                    }
                } catch (Throwable e) {
                    System.out.println("Error closing active transaction");
                }
            }
            try {
                connection.close();
            } catch (Throwable ex) {
                System.out.println("Error closing connection");
            }
        }
        this.connection = null;
    }

    private void readMetaInfo(DatabaseMetaData metaData) {

        if (metaData == null) {
            return;
        }

        boolean readOnly;
        String databaseProductName;
        String databaseProductVersion;
        String driverName;
        String driverVersion;
        int databaseMajorVersion;
        int databaseMinorVersion;

        String schemaTerm;
        String procedureTerm;
        String catalogTerm;

        boolean supportsTransactions;
        boolean supportsBatchUpdates;

        try {
            databaseMajorVersion = metaData.getDatabaseMajorVersion();
            databaseMinorVersion = metaData.getDatabaseMinorVersion();
        } catch (Throwable e) {
            databaseMajorVersion = 0;
            databaseMinorVersion = 0;
        }

        System.out.println("databaseMajorVersion : " + databaseMajorVersion);
        System.out.println("databaseMinorVersion : " + databaseMinorVersion);

        try {
            readOnly = metaData.isReadOnly();
        } catch (Throwable e) {
            readOnly = false;
        }

        System.out.println("readOnly : " + readOnly);

        try {
            databaseProductName = metaData.getDatabaseProductName();
        } catch (Throwable e) {
            databaseProductName = "?"; // $NON-NLS-1$
        }

        System.out.println("databaseProductName : " + databaseProductName);

        try {
            databaseProductVersion = metaData.getDatabaseProductVersion();
        } catch (Throwable e) {
            databaseProductVersion = "?"; // $NON-NLS-1$
        }

        System.out.println("databaseProductVersion : " + databaseProductVersion);

        try {
            driverName = metaData.getDriverName();
        } catch (Throwable e) {
            driverName = "?"; // $NON-NLS-1$
        }

        System.out.println("driverName : " + driverName);

        try {
            driverVersion = metaData.getDriverVersion();
        } catch (Throwable e) {
            driverVersion = "?"; // $NON-NLS-1$
        }

        System.out.println("driverVersion : " + driverVersion);

        try {
            schemaTerm = metaData.getSchemaTerm();
        } catch (Throwable e) {
            schemaTerm = "Schema";
        }

        System.out.println("schemaTerm : " + schemaTerm);

        try {
            procedureTerm = metaData.getProcedureTerm();
        } catch (Throwable e) {
            procedureTerm = "Procedure";
        }

        System.out.println("procedureTerm : " + procedureTerm);

        try {
            catalogTerm = metaData.getCatalogTerm();
        } catch (Throwable e) {
            catalogTerm = "Database";
        }

        System.out.println("catalogTerm : " + catalogTerm);

        try {
            supportsBatchUpdates = metaData.supportsBatchUpdates();
        } catch (Throwable e) {
            supportsBatchUpdates = false;
        }

        System.out.println("supportsBatchUpdates : " + supportsBatchUpdates);

        try {
            supportsTransactions = metaData.supportsTransactions();
        } catch (Throwable e) {
            supportsTransactions = true;
        }

        System.out.println("supportsTransactions : " + supportsTransactions);
    }

    private void readNodeFromMeta() {
        String vertexLabel;
        String vertexProperty;
        String getType;
        DatabaseMetaData meta;
        try {
            meta = connection.getMetaData();
            ResultSet tableResultSet = meta.getTables(null, null, null, new String[] {"NODE"});
            // ResultSet tableResultSet = meta.getTables(null, null, null, null);
            while (tableResultSet.next()) {
                vertexLabel = tableResultSet.getString("TABLE_NAME");
                String type = tableResultSet.getString("TABLE_TYPE");

                System.out.println("-------------------------------------------");
                System.out.println("vertex label : " + vertexLabel);
                System.out.println("vertex type : " + type);
                ResultSet columsResultSet = meta.getColumns(null, null, vertexLabel, type);
                while (columsResultSet.next()) {
                    vertexProperty = columsResultSet.getString("COLUMN_NAME");
                    System.out.println("vertexProperty : " + vertexProperty);
                    System.out.println(
                            "vertexProperty DataType : " + columsResultSet.getInt("DATA_TYPE"));
                    System.out.println(
                            "vertexProperty typeName : " + columsResultSet.getString("TYPE_NAME"));
                }
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void readEdgeFromMeta() {
        String vertexLabel;
        String vertexProperty;

        DatabaseMetaData meta;

        try {
            meta = connection.getMetaData();
            ResultSet tableResultSet = meta.getTables(null, null, null, new String[] {"EDGE"});
            while (tableResultSet.next()) {
                vertexLabel = tableResultSet.getString("TABLE_NAME");
                String type = tableResultSet.getString("TABLE_TYPE");

                System.out.println("---------------------------------------");
                System.out.println("Edge Type : " + vertexLabel);
                System.out.println("Edge TableType : " + type);
                ResultSet columsResultSet = meta.getColumns(null, null, vertexLabel, type);
                while (columsResultSet.next()) {
                    vertexProperty = columsResultSet.getString("COLUMN_NAME");
                    System.out.println("Edge Property : " + vertexProperty);
                    System.out.println("DataType : " + columsResultSet.getInt("DATA_TYPE"));
                    System.out.println("DataType : " + columsResultSet.getString("TYPE_NAME"));
                }
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void ResultSetMetaforQuery() {
        System.out.println("ResultSetMetaforQuery");
        for (int i = 0; i < testQuery.size(); i++) {
            System.out.println("-----------TEST_QUERYS " + i + "-----------");
            System.out.println(testQuery.get(i));
            try (PreparedStatement dbStat = connection.prepareStatement(testQuery.get(i))) {
                try (ResultSet rs = dbStat.executeQuery()) {
                    ResultSetMetaData meta = rs.getMetaData();
                    int count = meta.getColumnCount();
                    for (int j = 1; j < count + 1; j++) {
                        System.out.println(
                                "getColumnType : " + meta.getColumnType(j)); // java.sql.Types
                        System.out.println("getColumnTypeName : " + meta.getColumnTypeName(j));
                    }
                    while (rs.next()) {
                        for (int j = 1; j < count + 1; j++) {
                            Object m = rs.getObject(j);
                            if (m != null) {
                                System.out.println("Values : " + m.toString());
                                System.out.println(
                                        "TypeName : " + rs.getObject(j).getClass().getSimpleName());
                            }
                        }
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void useParameterBind() {
        String query =
                "MATCH (item:LINEITEM)"
                        + " WHERE item.L_EXTENDEDPRICE >= ? AND item.L_EXTENDEDPRICE <= ?"
                        + " AND item.L_SHIPMODE = ? AND item.L_SHIPDATE < ?"
                        + " RETURN item.L_RETURNFLAG AS ret_flag, item.L_SHIPDATE LIMIT 10";

        PreparedStatement pstmt = null;

        try {
            pstmt = connection.prepareStatement(query);
            System.out.println("query : " + query);
            BigDecimal value1 = new BigDecimal("28733.64");
            pstmt.setBigDecimal(1, value1);
            BigDecimal value2 = new BigDecimal("145983.16");
            pstmt.setBigDecimal(2, value2);
            pstmt.setString(3, "TRUCK");
            pstmt.setDate(4, Date.valueOf("1995-03-01"));

            try (ResultSet rs = pstmt.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                int count = meta.getColumnCount();
                for (int j = 1; j < count + 1; j++) {
                    System.out.println(
                            "getColumnType : " + meta.getColumnType(j)); // java.sql.Types
                    System.out.println("getColumnTypeName : " + meta.getColumnTypeName(j));
                }
                while (rs.next()) {
                    for (int j = 1; j < count + 1; j++) {
                        Object m = rs.getObject(j);
                        System.out.println("Values : " + m.toString());
                        System.out.println(
                                "TypeName : " + rs.getObject(j).getClass().getSimpleName());
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void getPlanTest() {
        String query =
                "MATCH (item:LINEITEM)"
                        + " WHERE item.L_EXTENDEDPRICE >= 28733.64 AND item.L_EXTENDEDPRICE <= 145983.16"
                        + " AND item.L_SHIPMODE = 'TRUCK' "
                        + " RETURN item.L_RETURNFLAG AS ret_flag LIMIT 10";
        PreparedStatement pstmt = null;
        System.out.println("query : " + query);
        try {
            pstmt = connection.prepareStatement(query);
            try (ResultSet rs = pstmt.executeQuery()) {
                String plan = ((TURBOGRAPHStatement) pstmt).getQueryplan(query);
                System.out.println("plan : " + plan);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public ClassLoader getClassLoader(String filepath) {
        synchronized (this) {
            try {
                final URL[] us;
                File file = new File(filepath);
                us = new URL[] {file.toURI().toURL()};
                ClassLoader result =
                        AccessController.doPrivileged(
                                new PrivilegedAction<URLClassLoader>() {
                                    public URLClassLoader run() {
                                        return new URLClassLoader(us);
                                    }
                                });
                return result;
            } catch (Exception e) {
                return null;
            }
        }
    }

    public Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName(driverClassName);

            Properties prop = new Properties();
            prop.put("logOnException", true);

            conn = DriverManager.getConnection(URL, prop);
            
//            TURBOGRAPHDriver turoboDriver = new TURBOGRAPHDriver();
//            conn = turoboDriver.connect(URL, prop);
        } catch (Exception e) {
            // TODO: handle exception
        }

        return conn;
    }

    public static void main(String[] args) {
        // make table, input want table count create.
        TurboGraphTest step = new TurboGraphTest();
        System.out.println("connection");

        step.connection();

        if (connection == null) {
            System.out.println("connection is null");
            return;
        }
        System.out.println("===========meta===========");
        step.meta(); // meta
        System.out.println("===========readNodeFromMeta===========");
        step.readNodeFromMeta();
        System.out.println("===========readEdgeFromMeta===========");
        step.readEdgeFromMeta();
        System.out.println("===========ResultSetMetaforQuery===========");
        addQuery();
        step.ResultSetMetaforQuery();
        System.out.println("===========useParameterBind===========");
        step.useParameterBind();
        System.out.println("===========getPlan===========");
        step.getPlanTest();
        System.out.println("close");
        step.close(false);
    }
}
