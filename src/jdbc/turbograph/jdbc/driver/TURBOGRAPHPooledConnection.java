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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Vector;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;

import turbograph.jdbc.jci.UConnection;

public class TURBOGRAPHPooledConnection implements PooledConnection {
    protected UConnection u_con;
    protected boolean isClosed;
    protected TURBOGRAPHConnection storedConnection;
    protected TURBOGRAPHConnection cubConnection;
    private Vector<ConnectionEventListener> eventListeners;

    protected TURBOGRAPHPooledConnection() {
        initConnection();
    }

    protected TURBOGRAPHPooledConnection(UConnection uCon) {
        initConnection();
        u_con = uCon;
    }

    protected TURBOGRAPHPooledConnection(TURBOGRAPHConnection cCon) {
        initConnection();
        storedConnection = cCon;
    }

    private void initConnection() {
        cubConnection = null;
        storedConnection = null;
        eventListeners = new Vector<ConnectionEventListener>();
        isClosed = false;
        u_con = null;
    }

    /*
     * javax.sql.PooledConnection interface
     */

    public synchronized Connection getConnection() throws SQLException {
        if (isClosed) {
            throw new TURBOGRAPHException(TURBOGRAPHJDBCErrorCode.pooled_connection_closed);
        }

        if (cubConnection != null) {
            cubConnection.closeConnection();
        }

        if (u_con == null && storedConnection != null) {
            u_con = storedConnection.getUConnection();
        }

        if (u_con.check_cas() == false) {
            u_con.resetConnection();
        }

        cubConnection = new TURBOGRAPHConnectionWrapperPooling(u_con, null, null, this);
        return cubConnection;
    }

    public synchronized void close() throws SQLException {
        if (isClosed) {
            return;
        }
        isClosed = true;
        if (cubConnection != null) {
            cubConnection.closeConnection();
        }
        if (storedConnection != null) {
            storedConnection.closeConnection();
        }
        if (u_con != null) {
            u_con.close();
        }
        eventListeners.clear();
    }

    public synchronized void addConnectionEventListener(ConnectionEventListener listener) {
        if (isClosed) {
            return;
        }

        eventListeners.addElement(listener);
    }

    public synchronized void removeConnectionEventListener(ConnectionEventListener listener) {
        if (isClosed) {
            return;
        }

        eventListeners.removeElement(listener);
    }

    synchronized void notifyConnectionClosed() {
        cubConnection = null;
        ConnectionEvent e = new ConnectionEvent(this);

        for (int i = 0; i < eventListeners.size(); i++) {
            eventListeners.elementAt(i).connectionClosed(e);
        }
    }

    synchronized void notifyConnectionErrorOccurred(SQLException ex) {
        cubConnection = null;
        ConnectionEvent e = new ConnectionEvent(this, ex);

        for (int i = 0; i < eventListeners.size(); i++) {
            eventListeners.elementAt(i).connectionErrorOccurred(e);
        }
    }

    /* JDK 1.6 */
    public void addStatementEventListener(StatementEventListener listener) {
        throw new java.lang.UnsupportedOperationException();
    }

    /* JDK 1.6 */
    public void removeStatementEventListener(StatementEventListener listener) {
        throw new java.lang.UnsupportedOperationException();
    }
}
