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

/**
 * Title: CUBRID Java Client Interface
 *
 * <p>Description: CUBRID Java Client Interface
 *
 * <p>
 *
 * @version 2.0
 */
package turbograph.jdbc.jci;

import turbograph.jdbc.driver.TURBOGRAPHConnection;
import turbograph.jdbc.driver.TURBOGRAPHDriver;
import turbograph.jdbc.driver.TURBOGRAPHXid;
import turbograph.jdbc.driver.ConnectionProperties;
import turbograph.jdbc.log.BasicLogger;
import turbograph.jdbc.log.Log;
import turbograph.jdbc.net.BrokerHandler;
import turbograph.sql.TURBOGRAPHOID;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;
import javax.transaction.xa.Xid;

public abstract class UConnection {
    /* DBMS types */
    public static final byte DBMS_CUBRID = 1;
    public static final byte DBMS_MYSQL = 2;
    public static final byte DBMS_ORACLE = 3;
    public static final byte DBMS_PROXY_CUBRID = 4;
    public static final byte DBMS_PROXY_MYSQL = 5;
    public static final byte DBMS_PROXY_ORACLE = 6;

    /* prepare flags */
    public static final byte PREPARE_INCLUDE_OID = 0x01;
    public static final byte PREPARE_UPDATABLE = 0x02;
    public static final byte PREPARE_QUERY_INFO = 0x04;
    public static final byte PREPARE_HOLDABLE = 0x08;
    public static final byte PREPARE_XASL_CACHE_PINNED = 0x10;
    public static final byte PREPARE_CALL = 0x40;

    public static final byte DROP_BY_OID = 1,
            IS_INSTANCE = 2,
            GET_READ_LOCK_BY_OID = 3,
            GET_WRITE_LOCK_BY_OID = 4,
            GET_CLASS_NAME_BY_OID = 5;

    public static final int OID_BYTE_SIZE = 8;

    // this value is defined in broker/cas_protocol.h
    private static final String magicString = "CUBRK";
    private static final String magicStringSSL = "CUBRS";
    private static final byte CAS_CLIENT_JDBC = 3;

    public static final int PROTOCOL_V0 = 0;
    public static final int PROTOCOL_V1 = 1;
    public static final int PROTOCOL_V2 = 2;
    public static final int PROTOCOL_V3 = 3;
    public static final int PROTOCOL_V4 = 4;
    public static final int PROTOCOL_V5 = 5;
    public static final int PROTOCOL_V6 = 6;
    public static final int PROTOCOL_V7 = 7;
    public static final int PROTOCOL_V8 = 8;
    public static final int PROTOCOL_V9 = 9;

    public static final int PROTOCOL_V11 = 11;
    public static final int PROTOCOL_V12 = 12;

    /* Current protocol version */
    protected static final byte CAS_PROTOCOL_VERSION = PROTOCOL_V12;
    protected static final byte CAS_PROTO_INDICATOR = 0x40;
    protected static final byte CAS_PROTO_VER_MASK = 0x3F;
    protected static final byte CAS_RENEWED_ERROR_CODE = (byte) 0x80;
    protected static final byte CAS_SUPPORT_HOLDABLE_RESULT = (byte) 0x40;
    /* Do not remove and rename CAS_RECONNECT_WHEN_SERVER_DOWN */
    protected static final byte CAS_RECONNECT_WHEN_SERVER_DOWN = (byte) 0x20;

    protected static final byte CAS_ORACLE_COMPAT_NUMBER_BEHAVIOR = (byte) 0x01;

    @SuppressWarnings("unused")
    private static final byte GET_COLLECTION_VALUE = 1,
            GET_SIZE_OF_COLLECTION = 2,
            DROP_ELEMENT_IN_SET = 3,
            ADD_ELEMENT_TO_SET = 4,
            DROP_ELEMENT_IN_SEQUENCE = 5,
            INSERT_ELEMENT_INTO_SEQUENCE = 6,
            PUT_ELEMENT_ON_SEQUENCE = 7;
    @SuppressWarnings("unused")
    private static final int DB_PARAM_ISOLATION_LEVEL = 1,
            DB_PARAM_LOCK_TIMEOUT = 2,
            DB_PARAM_AUTO_COMMIT = 4;

    /* end_tran constants */
    protected static final byte END_TRAN_COMMIT = 1;
    protected static final byte END_TRAN_ROLLBACK = 2;

    protected static final int LOCK_TIMEOUT_NOT_USED = -2;
    protected static final int LOCK_TIMEOUT_INFINITE = -1;

    protected static final int SOCKET_TIMEOUT = 5000;

    /* driver version */
    private static final int DRIVER_VERSION_MAX_SIZE = 20;

    /* cas info */
    protected static final byte CAS_INFO_STATUS_INACTIVE = 0;
    protected static final byte CAS_INFO_STATUS_ACTIVE = 1;

    protected static final int CAS_INFO_SIZE = 4;

    /* cas info field def */
    protected static final int CAS_INFO_STATUS = 0;
    protected static final int CAS_INFO_RESERVED_1 = 1;
    protected static final int CAS_INFO_RESERVED_2 = 2;
    protected static final int CAS_INFO_ADDITIONAL_FLAG = 3;

    protected static final byte CAS_INFO_FLAG_MASK_AUTOCOMMIT = 0x01;
    protected static final byte CAS_INFO_FLAG_MASK_FORCE_OUT_TRAN = 0x02;
    protected static final byte CAS_INFO_FLAG_MASK_NEW_SESSION_ID = 0x04;

    /* broker info */
    protected static final int BROKER_INFO_SIZE = 8;
    protected static final int BROKER_INFO_DBMS_TYPE = 0;
    protected static final int BROKER_INFO_RESERVED4 = 1;
    protected static final int BROKER_INFO_STATEMENT_POOLING = 2;
    protected static final int BROKER_INFO_CCI_PCONNECT = 3;
    protected static final int BROKER_INFO_PROTO_VERSION = 4;
    protected static final int BROKER_INFO_FUNCTION_FLAG = 5;
    protected static final int BROKER_INFO_SYSTEM_PARAM = 6;
    protected static final int BROKER_INFO_RESERVED3 = 7;

    /* For backward compatibility */
    protected static final int BROKER_INFO_MAJOR_VERSION = BROKER_INFO_PROTO_VERSION;
    protected static final int BROKER_INFO_MINOR_VERSION = BROKER_INFO_FUNCTION_FLAG;
    protected static final int BROKER_INFO_PATCH_VERSION = BROKER_INFO_SYSTEM_PARAM;

    public static final String ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL = "convertToNull";
    public static final String ZERO_DATETIME_BEHAVIOR_EXCEPTION = "exception";
    public static final String ZERO_DATETIME_BEHAVIOR_ROUND = "round";

    public static final String RESULT_WITH_CUBRID_TYPES_YES = "yes";
    public static final String RESULT_WITH_CUBRID_TYPES_NO = "no";

    public static final int SESSION_ID_SIZE = 20;

    public static final int MAX_QUERY_TIMEOUT = 2000000;
    public static final int MAX_CONNECT_TIMEOUT = 2000000;

    public static final int READ_TIMEOUT = 10000;

    public static final int FN_STATUS_NONE = -2;
    public static final int FN_STATUS_IDLE = -1;
    public static final int FN_STATUS_CONN = 0;
    public static final int FN_STATUS_BUSY = 1;
    public static final int FN_STATUS_DONE = 2;

    public static byte[] driverInfo;
    public static byte[] driverInfossl;

    static {
        driverInfo = new byte[10];
        UJCIUtil.copy_bytes(driverInfo, 0, 5, magicString);
        driverInfo[5] = CAS_CLIENT_JDBC;
        driverInfo[6] = CAS_PROTO_INDICATOR | CAS_PROTOCOL_VERSION;
        driverInfo[7] = CAS_RENEWED_ERROR_CODE | CAS_SUPPORT_HOLDABLE_RESULT;
        driverInfo[8] = 0; // reserved
        driverInfo[9] = 0; // reserved
    }

    static {
        driverInfossl = new byte[10];
        UJCIUtil.copy_bytes(driverInfossl, 0, 5, magicStringSSL);
        driverInfossl[5] = CAS_CLIENT_JDBC;
        driverInfossl[6] = CAS_PROTO_INDICATOR | CAS_PROTOCOL_VERSION;
        driverInfossl[7] = CAS_RENEWED_ERROR_CODE | CAS_SUPPORT_HOLDABLE_RESULT;
        driverInfossl[8] = 0; // reserved
        driverInfossl[9] = 0; // reserved
    }

    protected UError errorHandler;
    protected Log log;

    protected ConnectionProperties connectionProperties = new ConnectionProperties();
    protected TURBOGRAPHConnection cubridcon;

    protected Socket client;
    protected UTimedDataInputStream input;
    protected DataOutputStream output;
    protected UOutputBuffer outBuffer;

    // jci 3.0

    protected byte[] brokerInfo = null;
    protected byte[] casInfo = null;
    protected int brokerVersion = 0;
    protected static int protocolVersion = 0;

    public String casIp = "";
    public int casPort;
    public int casProcessId;
    public int casId;

    protected boolean needReconnection;

    protected boolean lastAutoCommit = true;
    private boolean isAutoCommitBySelf = false;

    protected boolean isClosed = false;

    private int lastIsolationLevel;
    private int lastLockTimeout = LOCK_TIMEOUT_NOT_USED;

    protected byte[] dbInfo;
    protected String dbname = "";
    protected String user = "";
    protected String passwd = "";
    protected String url = null;

    protected byte sessionId[] = createNullSession();
    protected int oldSessionId = 0;

    boolean skip_checkcas = false;
    Vector<UStatement> pooled_ustmts;
    Vector<Integer> deferred_close_handle;

    private long beginTime;

    /* for result cache */
    boolean update_executed;
    private UUrlCache url_cache = null;

    /* shard */
    private int lastShardId = UShardInfo.SHARD_ID_INVALID;
    private int numShard = 0;
    UShardInfo[] shardInfo = null;

    private static int isolationLevelMin = TURBOGRAPHIsolationLevel.TRAN_READ_COMMITTED;
    private static int isolationLevelMax = TURBOGRAPHIsolationLevel.TRAN_SERIALIZABLE;

    /*
     * main methods
     */

    // UFunctionCode.CHECK_CAS
    public synchronized boolean check_cas() {
        if (isClosed == true) return true;
        if (client == null || needReconnection == true) return true;

        if (skip_checkcas) {
            return true;
        }

        try {
            outBuffer.newRequest(output, UFunctionCode.CHECK_CAS);
            send_recv_msg();
        } catch (IOException e) {
            logException(e);
            return false;
        } catch (UJciException e) {
            logException(e);
            return false;
        }

        return true;
    }

    public synchronized boolean check_cas(String msg) {
        try {
            outBuffer.newRequest(output, UFunctionCode.CHECK_CAS);
            outBuffer.addStringWithNull(msg);
            send_recv_msg();
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    // UFunctionCode.CON_CLOSE
    public synchronized void close() {
        errorHandler = new UError(this);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return;
        }

        closeInternal();
    }

    // UFunctionCode.END_TRANSACTION
    public abstract void endTransaction(boolean type);

    // UFunctionCode.END_SESSION
    public synchronized void closeSession() {
        try {
            setBeginTime();
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return;
            outBuffer.newRequest(output, UFunctionCode.END_SESSION);
            send_recv_msg();
            sessionId = createNullSession();
            oldSessionId = 0;
        } catch (Exception e) {
        }
    }

    // UFunctionCode.GET_BY_OID
    public synchronized UStatement getByOID(TURBOGRAPHOID oid, String[] attributeName) {
        UStatement returnValue = null;

        errorHandler = new UError(this);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return null;
        }
        try {
            setBeginTime();
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return null;

            outBuffer.newRequest(output, UFunctionCode.GET_BY_OID);
            outBuffer.addOID(oid);
            for (int i = 0; attributeName != null && i < attributeName.length; i++) {
                if (attributeName[i] != null) outBuffer.addStringWithNull(attributeName[i]);
                else outBuffer.addNull();
            }

            UInputBuffer inBuffer;
            inBuffer = send_recv_msg();

            returnValue = new UStatement(this, oid, attributeName, inBuffer);
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
            return null;
        } catch (IOException e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
            return null;
        }
        if (returnValue.getRecentError().getErrorCode() != UErrorCode.ER_NO_ERROR) {
            errorHandler.copyValue(returnValue.getRecentError());
            return null;
        }
        return returnValue;
    }

    // UFunctionCode.GET_DB_VERSION
    public synchronized String getDatabaseProductVersion() {
        errorHandler = new UError(this);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return null;
        }
        try {
            setBeginTime();
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return null;

            outBuffer.newRequest(output, UFunctionCode.GET_DB_VERSION);
            outBuffer.addByte(getAutoCommit() ? (byte) 1 : (byte) 0);

            UInputBuffer inBuffer;
            inBuffer = send_recv_msg();

            return inBuffer.readString(inBuffer.remainedCapacity(), UJCIManager.sysCharsetName);
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
        } catch (IOException e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
        }
        return null;
    }

    // UFunctionCode.GET_DB_PARAMETER
    public synchronized int getIsolationLevel() {
        errorHandler = new UError(this);

        if (lastIsolationLevel != TURBOGRAPHIsolationLevel.TRAN_UNKNOWN_ISOLATION) {
            return lastIsolationLevel;
        }

        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return TURBOGRAPHIsolationLevel.TRAN_UNKNOWN_ISOLATION;
        }
        try {
            setBeginTime();
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR)
                return TURBOGRAPHIsolationLevel.TRAN_UNKNOWN_ISOLATION;

            outBuffer.newRequest(output, UFunctionCode.GET_DB_PARAMETER);
            outBuffer.addInt(DB_PARAM_ISOLATION_LEVEL);

            UInputBuffer inBuffer;
            inBuffer = send_recv_msg();

            lastIsolationLevel = inBuffer.readInt();
            return lastIsolationLevel;
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
        } catch (IOException e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
        }
        return TURBOGRAPHIsolationLevel.TRAN_UNKNOWN_ISOLATION;
    }

    // UFunctionCode.GET_QUERY_INFO
    public synchronized String getQueryplanOnly(String sql) {
        String ret_val;

        if (sql == null) return null;

        errorHandler = new UError(this);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return null;
        }

        try {
            setBeginTime();
            checkReconnect();
            outBuffer.newRequest(UFunctionCode.GET_QUERY_INFO);
            outBuffer.addInt(0);
            outBuffer.addByte(UStatement.QUERY_INFO_PLAN);
            outBuffer.addStringWithNull(sql);

            UInputBuffer inBuffer;
            inBuffer = send_recv_msg();

            ret_val =
                    inBuffer.readString(
                            inBuffer.remainedCapacity(), connectionProperties.getCharSet());
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
            return null;
        } catch (IOException e) {
            logException(e);
            if (errorHandler.getErrorCode() != UErrorCode.ER_CONNECTION)
                errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
            return null;
        }

        if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) {
            return null;
        }

        return ret_val;
    }

    // UFunctionCode.GET_SCHEMA_INFO
    public synchronized UStatement getSchemaInfo(
            int type, String arg1, String arg2, byte flag, int shard_id) {
        UStatement returnValue = null;

        errorHandler = new UError(this);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return null;
        }
        if (type < USchType.SCH_MIN || type > USchType.SCH_MAX) {
            errorHandler.setErrorCode(UErrorCode.ER_SCHEMA_TYPE);
            return null;
        }
        if (flag < 0 || flag > 3) {
            errorHandler.setErrorCode(UErrorCode.ER_ILLEGAL_FLAG);
            return null;
        }
        try {
            setBeginTime();
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return null;

            outBuffer.newRequest(output, UFunctionCode.GET_SCHEMA_INFO);
            outBuffer.addInt(type);
            if (arg1 == null) outBuffer.addNull();
            else outBuffer.addStringWithNull(arg1);
            if (arg2 == null) outBuffer.addNull();
            else outBuffer.addStringWithNull(arg2);
            outBuffer.addByte(flag);

            if (protoVersionIsAbove(PROTOCOL_V5)) {
                outBuffer.addInt(shard_id);
            }

            UInputBuffer inBuffer;
            inBuffer = send_recv_msg();

            returnValue = new UStatement(this, arg1, arg2, type, inBuffer);
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
            return null;
        } catch (IOException e) {
            logException(e);
            if (errorHandler.getErrorCode() != UErrorCode.ER_CONNECTION)
                errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
            return null;
        }
        if (returnValue.getRecentError().getErrorCode() != UErrorCode.ER_NO_ERROR) {
            errorHandler.copyValue(returnValue.getRecentError());
            return null;
        }
        // transactionList.add(returnValue);
        return returnValue;
    }

    // UFunctionCode.EXECUTE_BATCH_STATEMENT
    public synchronized UBatchResult batchExecute(String batchSqlStmt[], int queryTimeout) {
        errorHandler = new UError(this);
        setShardId(UShardInfo.SHARD_ID_INVALID);

        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return null;
        }
        if (batchSqlStmt == null) {
            errorHandler.setErrorCode(UErrorCode.ER_INVALID_ARGUMENT);
            return null;
        }
        try {
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return null;

            outBuffer.newRequest(output, UFunctionCode.EXECUTE_BATCH_STATEMENT);
            outBuffer.addByte(getAutoCommit() ? (byte) 1 : (byte) 0);
            if (protoVersionIsAbove(UConnection.PROTOCOL_V4)) {
                long remainingTime = getRemainingTime(queryTimeout * 1000);
                if (queryTimeout > 0 && remainingTime <= 0) {
                    throw createJciException(UErrorCode.ER_TIMEOUT);
                }
                outBuffer.addInt((int) remainingTime);
            }

            for (int i = 0; i < batchSqlStmt.length; i++) {
                if (batchSqlStmt[i] != null) outBuffer.addStringWithNull(batchSqlStmt[i]);
                else outBuffer.addNull();
            }

            UInputBuffer inBuffer;
            inBuffer = send_recv_msg(queryTimeout);

            int result;
            UBatchResult batchResult = new UBatchResult(inBuffer.readInt());
            for (int i = 0; i < batchResult.getResultNumber(); i++) {
                batchResult.setStatementType(i, inBuffer.readByte());
                result = inBuffer.readInt();
                if (result < 0) {
                    int err_code = inBuffer.readInt();
                    batchResult.setResultError(
                            i,
                            err_code,
                            inBuffer.readString(inBuffer.readInt(), UJCIManager.sysCharsetName));
                } else {
                    batchResult.setResult(i, result);
                    // jci 3.0
                    inBuffer.readInt();
                    inBuffer.readShort();
                    inBuffer.readShort();
                }
            }

            if (protoVersionIsAbove(UConnection.PROTOCOL_V5)) {
                setShardId(inBuffer.readInt());
            }

            update_executed = true;
            return batchResult;
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
        } catch (IOException e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
        }
        return null;
    }

    // UFunctionCode.RELATED_TO_COLLECTION
    public synchronized void addElementToSet(TURBOGRAPHOID oid, String attributeName, Object value) {
        errorHandler = new UError(this);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return;
        }
        try {
            manageElementOfSet(oid, attributeName, value, UConnection.ADD_ELEMENT_TO_SET);
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
            return;
        } catch (IOException e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
            return;
        }
    }

    public synchronized void dropElementInSet(TURBOGRAPHOID oid, String attributeName, Object value) {
        errorHandler = new UError(this);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return;
        }
        try {
            manageElementOfSet(oid, attributeName, value, UConnection.DROP_ELEMENT_IN_SET);
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
            return;
        } catch (IOException e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
            return;
        }
    }

    public synchronized void putElementInSequence(
            TURBOGRAPHOID oid, String attributeName, int index, Object value) {
        errorHandler = new UError(this);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return;
        }
        try {
            manageElementOfSequence(
                    oid, attributeName, index, value, UConnection.PUT_ELEMENT_ON_SEQUENCE);
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
            return;
        } catch (IOException e) {
            logException(e);
            if (errorHandler.getErrorCode() != UErrorCode.ER_CONNECTION)
                errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
            return;
        }
    }

    public synchronized void insertElementIntoSequence(
            TURBOGRAPHOID oid, String attributeName, int index, Object value) {
        errorHandler = new UError(this);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return;
        }
        try {
            manageElementOfSequence(
                    oid, attributeName, index, value, UConnection.INSERT_ELEMENT_INTO_SEQUENCE);
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
            return;
        } catch (IOException e) {
            logException(e);
            if (errorHandler.getErrorCode() != UErrorCode.ER_CONNECTION)
                errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
            return;
        }
    }

    public synchronized void dropElementInSequence(TURBOGRAPHOID oid, String attributeName, int index) {
        errorHandler = new UError(this);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return;
        }
        try {
            setBeginTime();
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return;

            outBuffer.newRequest(output, UFunctionCode.RELATED_TO_COLLECTION);
            outBuffer.addByte(UConnection.DROP_ELEMENT_IN_SEQUENCE);
            outBuffer.addOID(oid);
            outBuffer.addInt(index);
            if (attributeName == null) outBuffer.addNull();
            else outBuffer.addStringWithNull(attributeName);

            send_recv_msg();
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
        } catch (IOException e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
        }
    }

    public synchronized int getSizeOfCollection(TURBOGRAPHOID oid, String attributeName) {
        errorHandler = new UError(this);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return 0;
        }
        try {
            setBeginTime();
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return 0;

            outBuffer.newRequest(output, UFunctionCode.RELATED_TO_COLLECTION);
            outBuffer.addByte(UConnection.GET_SIZE_OF_COLLECTION);
            outBuffer.addOID(oid);
            if (attributeName == null) outBuffer.addNull();
            else outBuffer.addStringWithNull(attributeName);

            UInputBuffer inBuffer;
            inBuffer = send_recv_msg();

            return inBuffer.readInt();
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
        } catch (IOException e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
        }
        return 0;
    }

    // UFunctionCode.RELATED_TO_OID
    public synchronized Object oidCmd(TURBOGRAPHOID oid, byte cmd) {
        errorHandler = new UError(this);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return null;
        }
        try {
            setBeginTime();
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return null;

            outBuffer.newRequest(output, UFunctionCode.RELATED_TO_OID);
            outBuffer.addByte(cmd);
            outBuffer.addOID(oid);

            UInputBuffer inBuffer;
            inBuffer = send_recv_msg();

            int res_code;
            res_code = inBuffer.getResCode();

            if (cmd == IS_INSTANCE) {
                if (res_code == 1) return oid;
            } else if (cmd == GET_CLASS_NAME_BY_OID) {
                return inBuffer.readString(
                        inBuffer.remainedCapacity(), connectionProperties.getCharSet());
            }
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
        } catch (IOException e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
        }

        return null;
    }

    // UFunctionCode.PREPARE
    public synchronized UStatement prepare(String sql, byte flag) {
        return prepare(sql, flag, false);
    }

    public synchronized UStatement prepare(String sql, byte flag, boolean recompile) {
        errorHandler = new UError(this);
        if (isClosed) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return null;
        }

        UStatement stmt = null;
        boolean isFirstPrepareInTran = !isActive();

        skip_checkcas = true;

        // first
        try {
            checkReconnect();
            stmt = prepareInternal(sql, flag, recompile);
            return stmt;
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
        } catch (IOException e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
            errorHandler.setStackTrace(e.getStackTrace());
        } finally {
            skip_checkcas = false;
        }

        if (isActive() && !isFirstPrepareInTran) {
            return null;
        }

        // second loop
        while (isErrorToReconnect(errorHandler.getJdbcErrorCode())) {
            if (!brokerInfoReconnectWhenServerDown()
                    || isErrorCommunication(errorHandler.getJdbcErrorCode())) {
                clientSocketClose();
            }

            try {
                errorHandler.clear();
                checkReconnect();
                if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) {
                    return null;
                }
            } catch (UJciException e) {
                logException(e);
                e.toUError(errorHandler);
                return null;
            } catch (IOException e) {
                logException(e);
                errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
                errorHandler.setStackTrace(e.getStackTrace());
                return null;
            }

            try {
                stmt = prepareInternal(sql, flag, recompile);
                return stmt;
            } catch (UJciException e) {
                logException(e);
                e.toUError(errorHandler);
            } catch (IOException e) {
                logException(e);
                errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
                errorHandler.setStackTrace(e.getStackTrace());
            }
        }

        return null;
    }

    // UFunctionCode.PUT_BY_OID
    public synchronized void putByOID(TURBOGRAPHOID oid, String attributeName[], Object values[]) {
        errorHandler = new UError(this);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return;
        }
        if (attributeName == null && values == null) {
            errorHandler.setErrorCode(UErrorCode.ER_INVALID_ARGUMENT);
            return;
        }
        try {
            UPutByOIDParameter putParameter = null;

            if (values != null) putParameter = new UPutByOIDParameter(attributeName, values);

            setBeginTime();
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return;

            outBuffer.newRequest(output, UFunctionCode.PUT_BY_OID);
            outBuffer.addOID(oid);
            if (putParameter != null) putParameter.writeParameter(outBuffer);

            send_recv_msg();
            if (getAutoCommit()) {
                turnOnAutoCommitBySelf();
            }
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
        } catch (IOException e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
        }
    }

    // UFunctionCode.SET_DB_PARAMETER
    public synchronized void setIsolationLevel(int level) {
        errorHandler = new UError(this);

        if (lastIsolationLevel != TURBOGRAPHIsolationLevel.TRAN_UNKNOWN_ISOLATION
                && lastIsolationLevel == level) {
            return;
        }

        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return;
        }
        if (level < getIsolationLevelMin() || level > getIsolationLevelMax()) {
            errorHandler.setErrorCode(UErrorCode.ER_ISO_TYPE);
            return;
        }
        try {
            setBeginTime();
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return;

            outBuffer.newRequest(output, UFunctionCode.SET_DB_PARAMETER);
            outBuffer.addInt(DB_PARAM_ISOLATION_LEVEL);
            outBuffer.addInt(level);

            send_recv_msg();

            lastIsolationLevel = level;
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
        } catch (IOException e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
        }
    }

    public synchronized void setLockTimeout(int timeout) {
        errorHandler = new UError(this);

        if (lastLockTimeout != LOCK_TIMEOUT_NOT_USED && lastLockTimeout == timeout) {
            return;
        }

        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return;
        }

        try {
            setBeginTime();
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return;

            outBuffer.newRequest(output, UFunctionCode.SET_DB_PARAMETER);
            outBuffer.addInt(DB_PARAM_LOCK_TIMEOUT);
            outBuffer.addInt(timeout);

            send_recv_msg();

            if (timeout < 0) lastLockTimeout = LOCK_TIMEOUT_INFINITE;
            else lastLockTimeout = timeout;
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
        } catch (IOException e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
        }
    }

    // UFunctionCode.SET_CAS_CHANGE_MODE
    public synchronized int setCASChangeMode(int mode) {
        errorHandler = new UError(this);

        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return errorHandler.getJdbcErrorCode();
        }

        try {
            setBeginTime();
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) {
                return errorHandler.getJdbcErrorCode();
            }

            outBuffer.newRequest(output, UFunctionCode.SET_CAS_CHANGE_MODE);
            outBuffer.addInt(mode);

            UInputBuffer inBuffer;
            inBuffer = send_recv_msg();

            return inBuffer.readInt();
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
        } catch (IOException e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
        }

        return errorHandler.getJdbcErrorCode();
    }

    /* LOB protocols */
    // UFunctionCode.NEW_LOB
    public synchronized byte[] lobNew(int lob_type) {
        errorHandler = new UError(this);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return null;
        }
        try {
            setBeginTime();
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return null;

            outBuffer.newRequest(output, UFunctionCode.NEW_LOB);
            outBuffer.addInt(lob_type);

            UInputBuffer inBuffer;
            inBuffer = send_recv_msg();

            int res_code;
            res_code = inBuffer.getResCode();
            if (res_code < 0) {
                errorHandler.setErrorCode(UErrorCode.ER_UNKNOWN);
                return null;
            }

            byte[] packedLobHandle = new byte[res_code];
            inBuffer.readBytes(packedLobHandle);
            return packedLobHandle;
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
        } catch (IOException e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
        } catch (Exception e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_UNKNOWN);
        }
        return null;
    }

    // UFunctionCode.WRITE_LOB
    public synchronized int lobWrite(
            byte[] packedLobHandle, long offset, byte[] buf, int start, int len) {
        errorHandler = new UError(this);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return -1;
        }
        try {
            setBeginTime();
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return -1;

            outBuffer.newRequest(output, UFunctionCode.WRITE_LOB);
            outBuffer.addBytes(packedLobHandle);
            outBuffer.addLong(offset);
            outBuffer.addBytes(buf, start, len);

            UInputBuffer inBuffer;
            inBuffer = send_recv_msg();

            int res_code;
            res_code = inBuffer.getResCode();
            if (res_code < 0) {
                errorHandler.setErrorCode(UErrorCode.ER_UNKNOWN);
            }
            return res_code;
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
        } catch (IOException e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
        } catch (Exception e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_UNKNOWN);
        }
        return -1;
    }

    // UFunctionCode.READ_LOB
    public synchronized int lobRead(
            byte[] packedLobHandle, long offset, byte[] buf, int start, int len) {
        errorHandler = new UError(this);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return -1;
        }
        try {
            setBeginTime();
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return -1;

            outBuffer.newRequest(output, UFunctionCode.READ_LOB);
            outBuffer.addBytes(packedLobHandle);
            outBuffer.addLong(offset);
            outBuffer.addInt(len);

            UInputBuffer inBuffer;
            inBuffer = send_recv_msg();

            int res_code;
            res_code = inBuffer.getResCode();
            if (res_code < 0) {
                errorHandler.setErrorCode(UErrorCode.ER_UNKNOWN);
            } else {
                inBuffer.readBytes(buf, start, res_code);
            }

            return res_code;
        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
        } catch (IOException e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
        } catch (Exception e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_UNKNOWN);
        }
        return -1;
    }

    /* XA protocols */
    // UFunctionCode.XA_END_TRAN
    public synchronized void xa_endTransaction(Xid xid, boolean type) {
        errorHandler = new UError(this);

        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return;
        }

        try {
            setBeginTime();
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return;

            outBuffer.newRequest(output, UFunctionCode.XA_END_TRAN);
            outBuffer.addXid(xid);
            outBuffer.addByte((type == true) ? END_TRAN_COMMIT : END_TRAN_ROLLBACK);

            send_recv_msg();
        } catch (Exception e) {
            errorHandler.setErrorCode(UErrorCode.ER_UNKNOWN);
        } finally {
            clientSocketClose();
            needReconnection = true;
        }
    }

    // UFunctionCode.XA_PREPARE
    public synchronized void xa_prepare(Xid xid) {
        errorHandler = new UError(this);

        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return;
        }

        try {
            setBeginTime();
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return;

            outBuffer.newRequest(output, UFunctionCode.XA_PREPARE);
            outBuffer.addXid(xid);

            send_recv_msg();
        } catch (Exception e) {
            errorHandler.setErrorCode(UErrorCode.ER_UNKNOWN);
        }
    }

    // UFunctionCode.XA_RECOVER
    public synchronized Xid[] xa_recover() {
        errorHandler = new UError(this);

        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return null;
        }

        try {
            setBeginTime();
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return null;

            outBuffer.newRequest(output, UFunctionCode.XA_RECOVER);

            UInputBuffer inBuffer;
            inBuffer = send_recv_msg();

            int num_xid = inBuffer.getResCode();

            TURBOGRAPHXid[] xid;
            xid = new TURBOGRAPHXid[num_xid];
            for (int i = 0; i < num_xid; i++) {
                xid[i] = inBuffer.readXid();
            }
            return xid;
        } catch (Exception e) {
            errorHandler.setErrorCode(UErrorCode.ER_UNKNOWN);
            return null;
        }
    }

    // UFunctionCode.GET_SHARD_INFO
    public synchronized int shardInfo() {
        errorHandler = new UError(this);

        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return 0;
        }

        if (isConnectedToProxy() == false) {
            errorHandler.setErrorCode(UErrorCode.ER_NO_SHARD_AVAILABLE);
            return 0;
        }

        if (numShard > 0) {
            return numShard; // return cached shard info
        }

        try {
            setBeginTime();
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) {
                return 0;
            }

            outBuffer.newRequest(output, UFunctionCode.GET_SHARD_INFO);

            UInputBuffer inBuffer;
            inBuffer = send_recv_msg();

            int num_shard = inBuffer.getResCode();
            if (num_shard > 0) {
                shardInfo = new UShardInfo[num_shard];

                for (int i = 0; i < num_shard; i++) {
                    shardInfo[i] = new UShardInfo(inBuffer.readInt());

                    shardInfo[i].setDBName(
                            inBuffer.readString(inBuffer.readInt(), UJCIManager.sysCharsetName));
                    shardInfo[i].setDBServer(
                            inBuffer.readString(inBuffer.readInt(), UJCIManager.sysCharsetName));
                }

                numShard = num_shard;
            }

        } catch (UJciException e) {
            logException(e);
            e.toUError(errorHandler);
        } catch (IOException e) {
            logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
        }

        return numShard;
    }

    /*
     * internal implementation of main methods
     */
    protected abstract void closeInternal();

    // jci 3.0
    protected void disconnect() {
        try {
            setBeginTime();
            checkReconnect();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return;

            outBuffer.newRequest(output, UFunctionCode.CON_CLOSE);
            send_recv_msg();
        } catch (Exception e) {
        }
    }

    // UFunctionCode.PREPARE
    protected UStatement prepareInternal(String sql, byte flag, boolean recompile)
            throws IOException, UJciException {
        errorHandler.clear();

        outBuffer.newRequest(output, UFunctionCode.PREPARE);
        outBuffer.addStringWithNull(sql);
        outBuffer.addByte(flag);
        outBuffer.addByte(getAutoCommit() ? (byte) 1 : (byte) 0);

        while (deferred_close_handle.isEmpty() != true) {
            Integer close_handle = (Integer) deferred_close_handle.remove(0);
            outBuffer.addInt(close_handle.intValue());
        }

        UInputBuffer inBuffer = send_recv_msg();
        UStatement stmt;
        if (recompile) {
            stmt = new UStatement(this, inBuffer, true, sql, flag);
        } else {
            stmt = new UStatement(this, inBuffer, false, sql, flag);
        }

        if (stmt.getRecentError().getErrorCode() != UErrorCode.ER_NO_ERROR) {
            errorHandler.copyValue(stmt.getRecentError());
            return null;
        }

        pooled_ustmts.add(stmt);

        return stmt;
    }

    // UFunctionCode.RELATED_TO_COLLECTION
    protected void manageElementOfSequence(
            TURBOGRAPHOID oid, String attributeName, int index, Object value, byte flag)
            throws UJciException, IOException {
        UAParameter aParameter;
        aParameter = new UAParameter(attributeName, value);

        setBeginTime();
        checkReconnect();
        if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return;

        outBuffer.newRequest(output, UFunctionCode.RELATED_TO_COLLECTION);
        outBuffer.addByte(flag);
        outBuffer.addOID(oid);
        outBuffer.addInt(index);
        aParameter.writeParameter(outBuffer);

        send_recv_msg();
    }

    protected void manageElementOfSet(TURBOGRAPHOID oid, String attributeName, Object value, byte flag)
            throws UJciException, IOException {
        UAParameter aParameter;
        aParameter = new UAParameter(attributeName, value);

        setBeginTime();
        checkReconnect();
        if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return;

        outBuffer.newRequest(output, UFunctionCode.RELATED_TO_COLLECTION);
        outBuffer.addByte(flag);
        outBuffer.addOID(oid);
        aParameter.writeParameter(outBuffer);

        send_recv_msg();
    }

    /*
     * requests via broker handler
     */
    public boolean isValid(int timeout) throws SQLException {
        if (protoVersionIsUnder(PROTOCOL_V9)) {
            return !isClosed;
        }
        try {
            byte[] session = new byte[4];
            for (int i = 0; i < 4; i++) session[i] = sessionId[i + 8];

            int status = BrokerHandler.statusBroker(casIp, casPort, casProcessId, session, timeout);
            if (status == UConnection.FN_STATUS_NONE) {
                return false;
            }
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    void cancel() throws UJciException, IOException {
        if (protoVersionIsAbove(PROTOCOL_V4)) {
            BrokerHandler.cancelBrokerEx(casIp, casPort, casProcessId, READ_TIMEOUT);
        } else {
            BrokerHandler.cancelBroker(casIp, casPort, casProcessId, READ_TIMEOUT);
        }
    }

    /*
     * logger
     */
    protected Log getLogger() {
        if (log == null) {
            log = new BasicLogger(connectionProperties.getLogFile());
        }
        return log;
    }

    protected void initLogger() {
        if (connectionProperties.getLogOnException() || connectionProperties.getLogSlowQueries()) {
            log = getLogger();
        }
    }

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public void logSlowQuery(long begin, long end, String sql, UBindParameter p) {
        if (connectionProperties == null || connectionProperties.getLogSlowQueries() != true) {
            return;
        }

        long elapsed = end - begin;
        if (connectionProperties.getSlowQueryThresholdMillis() > elapsed) {
            return;
        }

        StringBuffer b = new StringBuffer();
        b.append("SLOW QUERY\n");
        b.append(String.format("[CAS INFO]\n%s:%d, %d, %d\n", casIp, casPort, casId, casProcessId));
        b.append(
                String.format(
                        "[TIME]\nSTART: %s, ELAPSED: %d\n",
                        dateFormat.format(new Date(begin)), elapsed));
        b.append("[SQL]\n").append(sql).append('\n');
        if (p != null) {
            b.append("[BIND]\n");
            for (int i = 0; i < p.values.length; i++) {
                if (i != 0) b.append(", ");
                b.append(p.values[i].toString());
            }
            b.append('\n');
        }

        synchronized (this) {
            getLogger().logInfo(b.toString());
        }
    }

    /*
     * methods related to Connection Properties
     */
    public int getQueryTimeout() {
        return connectionProperties.getQueryTimeout();
    }

    public void setCharset(String newCharsetName) {}

    public String getCharset() {
        return connectionProperties.getCharSet();
    }

    public void setZeroDateTimeBehavior(String behavior) {}

    public String getZeroDateTimeBehavior() {
        return connectionProperties.getZeroDateTimeBehavior();
    }

    public void setResultWithCUBRIDTypes(String support) {}

    public String getResultWithCUBRIDTypes() {
        return connectionProperties.getResultWithCUBRIDTypes();
    }

    public boolean getLogSlowQuery() {
        return connectionProperties.getLogSlowQueries();
    }

    public boolean getUseOldBooleanValue() {
        return connectionProperties.getUseOldBooleanValue();
    }

    public boolean getOracleStyleEmpltyString() {
        return connectionProperties.getOracleStyleEmptyString();
    }

    public int getClientCacheSize() {
        /* unit = MByte */
        return connectionProperties.getClientCacheSize() * 1024 * 1024;
    }

    public boolean getPrepStmtCache() {
        return connectionProperties.getPrepStmtCache();
    }

    public int getPrepStmtCacheSize() {
        return connectionProperties.getPrepStmtCacheSize();
    }

    public int getPrepStmtCacheSqlLimit() {
        return connectionProperties.getPrepStmtCacheSqlLimit();
    }

    public boolean isPrepStmtCache(String sql) {
        boolean isCacheable = false;
        if (connectionProperties.getPrepStmtCache()
                && sql.length() <= connectionProperties.getPrepStmtCacheSqlLimit()) {
            isCacheable = true;
        }
        return isCacheable;
    }

    public void setCasIp(String casIp) {
        this.casIp = casIp;
    }

    public String getCasIp() {
        return this.casIp;
    }

    public void setCasPort(int casPort) {
        this.casPort = casPort;
    }

    public int getCasPort() {
        return this.casPort;
    }

    public void setCasProcessId(int processId) {
        this.casProcessId = processId;
    }

    public int getCasProcessId() {
        return this.casProcessId;
    }

    public void setCasId(int casId) {
        this.casId = casId;
    }

    public int getCasId() {
        return this.casId;
    }

    public abstract void setAutoCommit(boolean autoCommit);

    public abstract boolean getAutoCommit();

    public synchronized void turnOnAutoCommitBySelf() {
        isAutoCommitBySelf = true;
    }

    public synchronized void turnOffAutoCommitBySelf() {
        isAutoCommitBySelf = false;
    }

    public boolean getAutoCommitBySelf() {
        return isAutoCommitBySelf;
    }

    public synchronized OutputStream getOutputStream() {
        return output;
    }

    public UError getRecentError() {
        return errorHandler;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public boolean isErrorCommunication(int error) {
        switch (error) {
            case UErrorCode.ER_COMMUNICATION:
            case UErrorCode.ER_ILLEGAL_DATA_SIZE:
            case UErrorCode.CAS_ER_COMMUNICATION:
                return true;
            default:
                return false;
        }
    }

    public boolean isErrorToReconnect(int error) {
        if (isErrorCommunication(error)) {
            return true;
        }

        switch (error) {
            case -111: // ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED
            case -199: // ER_NET_SERVER_CRASHED
            case -224: // ER_OBJ_NO_CONNECT
            case -677: // ER_BO_CONNECT_FAILED
                return true;
            default:
                return false;
        }
    }

    public int getLockTimeout() {
        return lastLockTimeout;
    }

    /*
     * 3.0 synchronized public void savepoint(int mode, String name) {
     * errorHandler = new UError(); if (isClosed == true) {
     * errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED); return; }
     *
     * try { checkReconnect(); if (errorHandler.getErrorCode() !=
     * UErrorCode.ER_NO_ERROR) return;
     *
     * outBuffer.newRequest(out, UFunctionCode.SAVEPOINT);
     * outBuffer.addByte(mode); outBuffer.addStringWithNull(name);
     *
     * UInputBuffer inBuffer; inBuffer = send_recv_msg(); } catch (UJciException
     * e) { e.toUError(errorHandler); } catch (IOException e) {
     * errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION); } }
     */

    public byte getCASInfoStatus() {
        if (casInfo == null) {
            return (byte) CAS_INFO_STATUS_INACTIVE;
        }
        return casInfo[CAS_INFO_STATUS];
    }

    public byte[] getCASInfo() {
        return casInfo;
    }

    public void setCASInfo(byte[] casinfo) {
        this.casInfo = casinfo;
    }

    public byte getDbmsType() {
        // jci 3.0
        if (brokerInfo == null) return DBMS_CUBRID;
        return brokerInfo[BROKER_INFO_DBMS_TYPE];

        /*
         * jci 2.x return DBMS_CUBRID;
         */
    }

    public boolean isConnectedToCubrid() {
        byte dbms_type = getDbmsType();
        if (dbms_type == DBMS_CUBRID || dbms_type == DBMS_PROXY_CUBRID) {
            return true;
        }
        return false;
    }

    public boolean isConnectedToOracle() {
        byte dbms_type = getDbmsType();
        if (dbms_type == DBMS_ORACLE || dbms_type == DBMS_PROXY_ORACLE) {
            return true;
        }
        return false;
    }

    public boolean isConnectedToProxy() {
        byte dbms_type = getDbmsType();
        if (dbms_type == DBMS_PROXY_CUBRID
                || dbms_type == DBMS_PROXY_MYSQL
                || dbms_type == DBMS_PROXY_ORACLE) {
            return true;
        }
        return false;
    }

    public boolean brokerInfoStatementPooling() {
        if (brokerInfo == null) return false;

        if (brokerInfo[BROKER_INFO_STATEMENT_POOLING] == (byte) 1) return true;
        else return false;
    }

    public boolean brokerInfoRenewedErrorCode() {
        if ((brokerInfo[BROKER_INFO_PROTO_VERSION] & CAS_PROTO_INDICATOR) != CAS_PROTO_INDICATOR) {
            return false;
        }

        return (brokerInfo[BROKER_INFO_FUNCTION_FLAG] & CAS_RENEWED_ERROR_CODE)
                == CAS_RENEWED_ERROR_CODE;
    }

    public boolean brokerInfoSupportHoldableResult() {
        if (brokerInfo == null) return false;

        return (brokerInfo[BROKER_INFO_FUNCTION_FLAG] & CAS_SUPPORT_HOLDABLE_RESULT)
                == CAS_SUPPORT_HOLDABLE_RESULT;
    }

    public boolean brokerInfoReconnectWhenServerDown() {
        if (brokerInfo == null) return false;

        return (brokerInfo[BROKER_INFO_FUNCTION_FLAG] & CAS_RECONNECT_WHEN_SERVER_DOWN)
                == CAS_RECONNECT_WHEN_SERVER_DOWN;
    }

    public boolean supportHoldableResult() {
        if (brokerInfoSupportHoldableResult() || protoVersionIsSame(UConnection.PROTOCOL_V2)) {
            return true;
        }

        return false;
    }

    public boolean isOracleCompatNumberBehavior() {
        if (protoVersionIsAbove(PROTOCOL_V12)) {
            if (brokerInfo == null) return false;
            return (brokerInfo[BROKER_INFO_SYSTEM_PARAM] & CAS_ORACLE_COMPAT_NUMBER_BEHAVIOR)
                    == CAS_ORACLE_COMPAT_NUMBER_BEHAVIOR;
        } else {
            return false;
        }
    }

    public void setCUBRIDConnection(TURBOGRAPHConnection con) {
        cubridcon = con;
        lastIsolationLevel = TURBOGRAPHIsolationLevel.TRAN_UNKNOWN_ISOLATION;
        lastLockTimeout = LOCK_TIMEOUT_NOT_USED;
    }

    public TURBOGRAPHConnection getCUBRIDConnection() {
        return cubridcon;
    }

    private static void printCasInfo(byte[] prev, byte[] curr) {
        if (prev != null) {
            String fmt =
                    "[PREV : %d, RECV : %d], [preffunc : %d, recvfunc : %d], [REQ: %d], [JID: %d]";
            String msg = String.format(fmt, prev[0], curr[0], prev[1], curr[1], prev[2], curr[3]);
            TURBOGRAPHDriver.printDebug(msg);
        }
    }

    public synchronized void resetConnection() {
        try {
            if (client != null) client.close();
        } catch (Exception e) {
        }

        client = null;
        needReconnection = true;
    }

    public int currentIsolationLevel() {
        return lastIsolationLevel;
    }

    void setIsolationLevelMin(int level) {
        isolationLevelMin = level;
    }

    int getIsolationLevelMin() {
        return isolationLevelMin;
    }

    void setIsolationLevelMax(int level) {
        isolationLevelMax = level;
    }

    int getIsolationLevelMax() {
        return isolationLevelMax;
    }

    public static byte[] createDBInfo(String dbname, String user, String passwd, String url) {
        // see broker/cas_protocol.h
        // #define SRV_CON_DBNAME_SIZE 32
        // #define SRV_CON_DBUSER_SIZE 32
        // #define SRV_CON_DBPASSWD_SIZE 32
        // #define SRV_CON_DBSESS_ID_SIZE 20
        // #define SRV_CON_URL_SIZE 512
        // #define SRV_CON_DB_INFO_SIZE \
        // (SRV_CON_DBNAME_SIZE + SRV_CON_DBUSER_SIZE +
        // SRV_CON_DBPASSWD_SIZE + \
        // SRV_CON_URL_SIZE + SRV_CON_DBSESS_ID_SIZE)
        byte[] info = new byte[32 + 32 + 32 + 512 + 20];
        UJCIUtil.copy_bytes(info, 0, 32, dbname);
        UJCIUtil.copy_bytes(info, 32, 32, user);
        UJCIUtil.copy_bytes(info, 64, 32, passwd);
        UJCIUtil.copy_bytes(info, 96, 511, url);

        if (url == null) {
            UJCIUtil.copy_byte(info, 96, (byte) 0); // null
            UJCIUtil.copy_byte(info, 97, (byte) 0); // length
        } else {
            String version = TURBOGRAPHDriver.version_string;
            int index = 96 + url.getBytes().length + 1;
            if ((version.getBytes().length <= DRIVER_VERSION_MAX_SIZE)
                    && (url.getBytes().length + version.getBytes().length + 3 <= 512)) {

                // url = ( url string + length (1byte) + version string )
                byte len = (byte) version.getBytes().length;
                UJCIUtil.copy_byte(info, index, len);
                UJCIUtil.copy_bytes(info, index + 1, version.getBytes().length + 1, version);
            } else {
                UJCIUtil.copy_byte(info, index, (byte) 0); // length
            }
        }
        return info;
    }

    void clientSocketClose() {
        try {
            needReconnection = true;
            if (client != null) {
                client.setSoLinger(true, 0);
                client.close();
            }
            client = null;
        } catch (IOException e) {
            logException(e);
        }
        clearPooledUStatements();
        deferred_close_handle.clear();
    }

    UInputBuffer send_recv_msg(boolean recv_result, int timeout) throws UJciException, IOException {
        byte prev_casinfo[] = casInfo;
        UInputBuffer inputBuffer;
        outBuffer.sendData();
        /* set cas info to UConnection member variable and return InputBuffer */
        if (timeout > 0) {
            inputBuffer = new UInputBuffer(input, this, timeout * 1000 + READ_TIMEOUT);
        } else {
            inputBuffer = new UInputBuffer(input, this, 0);
        }

        if (UJCIUtil.isConsoleDebug()) {
            printCasInfo(prev_casinfo, casInfo);
        }
        return inputBuffer;
    }

    UInputBuffer send_recv_msg(int timeout) throws UJciException, IOException {
        if (client == null) {
            createJciException(UErrorCode.ER_COMMUNICATION);
        }
        return send_recv_msg(true, timeout);
    }

    UInputBuffer send_recv_msg(boolean recv_result) throws UJciException, IOException {
        byte prev_casinfo[] = casInfo;
        outBuffer.sendData();
        /* set cas info to UConnection member variable and return InputBuffer */
        UInputBuffer inputBuffer = new UInputBuffer(input, this, 0);

        if (UJCIUtil.isConsoleDebug()) {
            printCasInfo(prev_casinfo, casInfo);
        }
        return inputBuffer;
    }

    UInputBuffer send_recv_msg() throws UJciException, IOException {
        if (client == null) {
            createJciException(UErrorCode.ER_COMMUNICATION);
        }
        return send_recv_msg(true);
    }

    UUrlCache getUrlCache() {
        if (url_cache == null) {
            UUrlHostKey key = new UUrlHostKey(casIp, casPort, dbname, user);
            url_cache = UJCIManager.getUrlCache(key);
            url_cache.setLimit(getClientCacheSize());
        }
        return url_cache;
    }

    protected int makeBrokerVersion(int major, int minor, int patch) {
        int version = 0;
        if ((major < 0 || major > Byte.MAX_VALUE)
                || (minor < 0 || minor > Byte.MAX_VALUE)
                || (patch < 0 || patch > Byte.MAX_VALUE)) {
            return 0;
        }

        version = ((int) major << 24) | ((int) minor << 16) | ((int) patch << 8);
        return version;
    }

    protected int makeProtoVersion(int ver) {
        return ((int) CAS_PROTO_INDICATOR << 24) | ver;
    }

    public int brokerInfoVersion() {
        return brokerVersion;
    }

    public boolean protoVersionIsSame(int ver) {
        if (brokerInfoVersion() == makeProtoVersion(ver)) {
            return true;
        }
        return false;
    }

    public boolean protoVersionIsUnder(int ver) {
        if (brokerInfoVersion() < makeProtoVersion(ver)) {
            return true;
        }
        return false;
    }

    public boolean protoVersionIsAbove(int ver) {
        if (brokerInfoVersion() >= makeProtoVersion(ver)) {
            return true;
        }
        return false;
    }

    public static boolean protoVersionIsLower(int ver) {
        if (protocolVersion < ver) {
            return true;
        }
        return false;
    }

    protected void checkReconnect() throws IOException, UJciException {
        if (dbInfo == null) {
            dbInfo = createDBInfo(dbname, user, passwd, url);
        }
        // set the session id
        if (brokerInfoVersion() == 0) {
            /* Interpretable session information supporting version
             *   later than PROTOCOL_V3 as well as version earlier
             *   than PROTOCOL_V3 should be delivered since no broker information
             *   is provided at the time of initial connection.
             */
            String id = "0";
            UJCIUtil.copy_bytes(dbInfo, 608, 20, id);
        } else if (protoVersionIsAbove(PROTOCOL_V3)) {
            System.arraycopy(sessionId, 0, dbInfo, 608, 20);
        } else {
            UJCIUtil.copy_bytes(dbInfo, 608, 20, new Integer(oldSessionId).toString());
        }

        if (outBuffer == null) {
            outBuffer = new UOutputBuffer(this);
        }

        if (pooled_ustmts == null) {
            pooled_ustmts = new Vector<UStatement>();
        }

        if (deferred_close_handle == null) {
            deferred_close_handle = new Vector<Integer>();
        }
    }

    private byte[] createNullSession() {
        return new byte[SESSION_ID_SIZE];
    }

    private void clearPooledUStatements() {
        if (pooled_ustmts == null) return;

        while (pooled_ustmts.isEmpty() != true) {
            UStatement tmp_ustmt = (UStatement) pooled_ustmts.remove(0);
            if (tmp_ustmt != null) tmp_ustmt.close(false);
        }
    }

    public void setConnectionProperties(ConnectionProperties connProperties) {
        this.connectionProperties = connProperties;
    }

    public UJciException createJciException(int err) {
        UJciException e = new UJciException(err);
        if (connectionProperties == null || !connectionProperties.getLogOnException()) {
            return e;
        }

        StringBuffer b = new StringBuffer();
        b.append("DUMP EXCEPTION\n");
        b.append("[JCI EXCEPTION]");

        synchronized (this) {
            getLogger().logInfo(b.toString(), e);
        }
        return e;
    }

    public UJciException createJciException(int err, int indicator, int srv_err, String msg) {
        UJciException e = new UJciException(err, indicator, srv_err, msg);
        logException(e);
        return e;
    }

    public void logException(Throwable t) {
        if (connectionProperties == null || !connectionProperties.getLogOnException()) {
            return;
        }

        StringBuffer b = new StringBuffer();
        b.append("DUMP EXCEPTION\n");
        b.append("[" + t.getClass().getName() + "]");

        synchronized (this) {
            getLogger().logInfo(b.toString(), t);
        }
    }

    public boolean isActive() {
        return getCASInfoStatus() == CAS_INFO_STATUS_ACTIVE;
    }

    public void setBeginTime() {
        beginTime = System.currentTimeMillis();
    }

    public long getBeginTime() {
        return beginTime;
    }

    public long getRemainingTime(long timeout) {
        if (beginTime == 0 || timeout == 0) {
            return timeout;
        }

        long now = System.currentTimeMillis();
        return timeout - (now - beginTime);
    }

    public void resetBeginTime() {
        beginTime = 0;
    }

    public boolean isRenewedSessionId() {
        return (brokerInfoReconnectWhenServerDown()
                && ((casInfo[CAS_INFO_ADDITIONAL_FLAG] & CAS_INFO_FLAG_MASK_NEW_SESSION_ID)
                        == CAS_INFO_FLAG_MASK_NEW_SESSION_ID));
    }

    public void setNewSessionId(byte[] newSessionId) {
        sessionId = newSessionId;
    }

    public void setShardId(int shardId) {
        lastShardId = shardId;
    }

    public int getShardId() {
        return lastShardId;
    }

    public int getShardCount() {
        if (isConnectedToProxy() == false) {
            return 0;
        }

        if (numShard == 0) {
            int num_shard = shardInfo();
            if (num_shard == 0 || errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) {
                return 0;
            }
        }

        return numShard;
    }

    public synchronized UShardInfo getShardInfo(int shard_id) {
        errorHandler = new UError(this);

        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return null;
        }

        if (isConnectedToProxy() == false) {
            errorHandler.setErrorCode(UErrorCode.ER_NO_SHARD_AVAILABLE);
            return null;
        }

        if (numShard == 0) {
            int num_shard = shardInfo();
            if (num_shard == 0 || errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) {
                return null;
            }
        }

        if (shard_id < 0 || shard_id >= numShard) {
            errorHandler.setErrorCode(UErrorCode.ER_INVALID_SHARD);
            return null;
        }

        return shardInfo[shard_id];
    }
}
