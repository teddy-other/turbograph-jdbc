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

package turbograph.jdbc.driver;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import turbograph.jdbc.jci.BrokerHealthCheck;
import turbograph.jdbc.jci.UClientSideConnection;
import turbograph.jdbc.jci.UJCIManager;
import turbograph.jdbc.jci.UJCIUtil;

/**
 * Title: CUBRID JDBC Driver Description:
 *
 * @version 2.0
 */
public class TURBOGRAPHDriver implements Driver {
    // version
    public static final String version_string = "1.0.0.0001";
    public static final int major_version;
    public static final int minor_version;
    public static final int patch_version;

    static {
        StringTokenizer st = new StringTokenizer(version_string, ".");
        if (st.countTokens() != 4) {
            throw new RuntimeException("Could not parse version_string: " + version_string);
        }
        major_version = Integer.parseInt(st.nextToken());
        minor_version = Integer.parseInt(st.nextToken());
        patch_version = Integer.parseInt(st.nextToken());
    }

    // default connection informations
    public static final String default_hostname = "localhost";
    public static final int default_port = 30000;
    public static final String default_user = "public";
    public static final String default_password = "";

    private static final String URL_PATTERN =
            "jdbc:turbograph:([a-zA-Z_0-9\\.-]*):([0-9]*):([^:]+):([^:]*):([^:]*):(\\?[a-zA-Z_0-9]+=[^&=?]+(&[a-zA-Z_0-9]+=[^&=?]+)*)?";
    private static final String CUBRID_JDBC_URL_HEADER = "jdbc:turbograph";
    private static final String ENV_JDBC_PROP_NAME = "TURBOGRAPH_JDBC_PROP";

    static {
        try {
            DriverManager.registerDriver(new TURBOGRAPHDriver());
        } catch (SQLException e) {
        }
    }

    private static PrintStream debugOutput;

    static {
        if (UJCIUtil.isConsoleDebug()) {
            try {
                debugOutput = new PrintStream(new File("turbograph.log"));
            } catch (FileNotFoundException e) {
                debugOutput = System.out;
            }
        }
        Thread brokerHealthCheck = new Thread(new BrokerHealthCheck());
        brokerHealthCheck.setDaemon(true);
        brokerHealthCheck.setContextClassLoader(null);
        brokerHealthCheck.start();
    }

    public static void printDebug(String msg) {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        SimpleDateFormat fmt = new SimpleDateFormat("MM-dd hh:mm:ss.SSS");

        String line = String.format("%s %s", fmt.format(timestamp), msg);
        debugOutput.println(line);
    }

    /*
     * java.sql.Driver interface
     */

    public Connection connect(String url, Properties info) throws SQLException {
        Connection conn = null;
        String holdability = null;
        String dummy = null;
        String host = null;
        String portString = null;
        String db = null;
        String user = null;
        String pass = null;
        String prop = null;
        int port = default_port;

        if (!acceptsURL(url)) {
            return null;
        }

        Pattern pattern = Pattern.compile(URL_PATTERN, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(url);
        if (!matcher.find()) {
            throw new TURBOGRAPHException(TURBOGRAPHJDBCErrorCode.invalid_url, url, null);
        }

        String match = matcher.group();
        if (!match.equals(url)) {
            throw new TURBOGRAPHException(TURBOGRAPHJDBCErrorCode.invalid_url, url, null);
        }

        host = matcher.group(1);
        portString = matcher.group(2);
        db = matcher.group(3);
        prop = matcher.group(6);

        UClientSideConnection u_con;
        String resolvedUrl;
        ConnectionProperties connProperties;

        if (host == null || host.length() == 0) {
            host = default_hostname;
        }

        if (portString == null || portString.length() == 0) {
            port = default_port;
        } else {
            port = Integer.parseInt(portString);
        }

        user = info.getProperty("user");
        if (user == null) {
            user = matcher.group(4);
        }

        pass = info.getProperty("password");
        if (pass == null) {
            pass = matcher.group(5);
        }

        String filePath = System.getenv(ENV_JDBC_PROP_NAME);

        if (filePath != null) {
            if (existPropertiesFile(filePath)) {
                Properties temp_prop = new Properties();
                FileInputStream in;
                try {
                    in = new FileInputStream(filePath);
                    temp_prop.load(in);
                    in.close();

                    StringBuilder sbProp = new StringBuilder();
                    String value = null;
                    for (String key : temp_prop.stringPropertyNames()) {
                        value = temp_prop.getProperty(key);
                        if (sbProp.length() == 0) {
                            sbProp.append("?");
                        } else {
                            sbProp.append("&");
                        }
                        sbProp.append(key + "=" + value);
                    }

                    if (sbProp == null || sbProp.length() <= 0) {
                        throw new TURBOGRAPHException(
                                TURBOGRAPHJDBCErrorCode.invalid_prop_file, filePath, null);
                    }

                    prop = sbProp.toString();
                    url =
                            "jdbc:turbograph:"
                                    + host
                                    + ":"
                                    + port
                                    + ":"
                                    + db
                                    + ":"
                                    + user
                                    + ":"
                                    + pass
                                    + ":"
                                    + prop;

                    Matcher propMatcher = pattern.matcher(url);
                    if (!propMatcher.find()) {
                        throw new TURBOGRAPHException(
                                TURBOGRAPHJDBCErrorCode.invalid_prop_file, filePath, null);
                    }

                    String propMatch = propMatcher.group();
                    if (!propMatch.equals(url)) {
                        throw new TURBOGRAPHException(
                                TURBOGRAPHJDBCErrorCode.invalid_prop_file, filePath, null);
                    }

                } catch (FileNotFoundException e) {
                    throw new TURBOGRAPHException(
                            TURBOGRAPHJDBCErrorCode.file_not_found_prop, filePath, null);
                } catch (IOException e) {
                    throw new TURBOGRAPHException(
                            TURBOGRAPHJDBCErrorCode.invalid_prop_file, filePath, null);
                }
            } else {
                throw new TURBOGRAPHException(TURBOGRAPHJDBCErrorCode.file_not_found_prop, filePath, null);
            }
        }

        resolvedUrl = "jdbc:turbograph:" + host + ":" + port + ":" + db + ":" + user + ":********:";
        if (prop != null) {
            resolvedUrl += prop;
        }

        connProperties = new ConnectionProperties();
        connProperties.setProperties(prop);
        connProperties.setProperties(info);

        dummy = connProperties.getAltHosts();
        if (dummy != null) {
            ArrayList<String> altHostList = new ArrayList<String>();
            altHostList.add(host + ":" + port);

            StringTokenizer st = new StringTokenizer(dummy, ",", false);
            while (st.hasMoreTokens()) {
                altHostList.add(st.nextToken());
            }

            if (connProperties.getConnLoadBal()) {
                Collections.shuffle(altHostList);
            }
            try {
                u_con =
                        (UClientSideConnection)
                                UJCIManager.connect(altHostList, db, user, pass, resolvedUrl);
            } catch (TURBOGRAPHException e) {
                throw e;
            }
        } else {
            try {
                u_con =
                        (UClientSideConnection)
                                UJCIManager.connect(host, port, db, user, pass, resolvedUrl);
            } catch (TURBOGRAPHException e) {
                throw e;
            }
        }

        u_con.setCharset(connProperties.getCharSet());
        u_con.setZeroDateTimeBehavior(connProperties.getZeroDateTimeBehavior());
        u_con.setResultWithCUBRIDTypes(connProperties.getResultWithCUBRIDTypes());

        u_con.setConnectionProperties(connProperties);
        u_con.tryConnect();

        conn = new TURBOGRAPHConnection(u_con, url, user);
        if (conn != null) {
            conn.setHoldability(connProperties.getHoldCursor());
        }
        return conn;
    }

    public boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            return false;
        }

        String urlHeader = CUBRID_JDBC_URL_HEADER;
        String className = TURBOGRAPHDriver.class.getName();
        if (className.matches(".*mysql.*")) {
            urlHeader += "-mysql:";
        } else if (className.matches(".*oracle.*")) {
            urlHeader += "-oracle:";
        } else {
            urlHeader += ":";
        }

        if (url.toLowerCase().startsWith(urlHeader)) {
            return true;
        }

        return false;
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    public int getMajorVersion() {
        return major_version;
    }

    public int getMinorVersion() {
        return minor_version;
    }

    public boolean jdbcCompliant() {
        return true;
    }

    /* JDK 1.7 */
    public Logger getParentLogger() {
        throw new java.lang.UnsupportedOperationException();
    }

    private boolean existPropertiesFile(String filePath) {
        File file = new File(filePath);
        return file.exists();
    }
}
