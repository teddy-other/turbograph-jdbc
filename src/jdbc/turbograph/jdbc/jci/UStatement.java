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

import turbograph.jdbc.driver.TURBOGRAPHBlob;
import turbograph.jdbc.driver.TURBOGRAPHClob;
import turbograph.jdbc.driver.TURBOGRAPHHugeInt;
import turbograph.jdbc.driver.TURBOGRAPHOutResultSet;
import turbograph.sql.TURBOGRAPHOID;
import turbograph.sql.TURBOGRAPHTimestamptz;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;

public class UStatement {
    public static final int CURSOR_SET = 0, CURSOR_CUR = 1, CURSOR_END = 2;

    public static final byte QUERY_INFO_PLAN = 0x01;

    private static final byte ASYNC_EXECUTE = 1;
    private static final byte NORMAL = 0,
            GET_BY_OID = 1,
            GET_SCHEMA_INFO = 2,
            GET_AUTOINCREMENT_KEYS = 3;
    private static final byte OLD_TRUE = -128, TRUE = 1, FALSE = 0;
    private static final int DEFAULT_FETCH_SIZE = 100;

    private static final byte EXEC_FLAG_ASYNC = 0x01,
            EXEC_FLAG_QUERY_ALL = 0x02,
            EXEC_FLAG_QUERY_INFO = 0x04,
            EXEC_FLAG_ONLY_QUERY_PLAN = 0x08,
            EXEC_FLAG_HOLDABLE_RESULT = 0x20,
            EXEC_FLAG_GET_GENERATED_KEYS = 0x40;

    private static final byte MASK_COLLECTION_FROM_TYPE = (byte) 0140;
    private static final byte MASK_TYPE_HAS_2_BYTES = (byte) 0200;
    private static final byte MASK_TYPE_FROM_SINGLE_BYTE = (byte) 0037;
    private static final byte MASK_CHARSET_FROM_TYPE = (byte) 0007;
    private static final byte DEFAULT_CHARSET = (byte) 0377;

    private byte statementType;

    private UConnection relatedConnection;
    private boolean isClosed;
    private boolean realFetched;
    private boolean isUpdatable;
    private boolean isSensitive;

    private int serverHandler;
    private int parameterNumber;
    private int columnNumber;
    private UBindParameter bindParameter;
    private ArrayList<UBindParameter> batchParameter;
    private UColumnInfo columnInfo[];
    private HashMap<String, Integer> colNameToIndex;
    private UResultInfo resultInfo[];
    private byte commandTypeIs;
    private byte firstStmtType;
    private byte executeFlag;

    private int fetchDirection;
    private int fetchSize;
    private int maxFetchSize;
    private int fetchedTupleNumber;
    private int fetchedSize;
    private boolean isFetchCompleted;
    private int totalTupleNumber;
    private int currentFirstCursor;
    private int cursorPosition;
    private int executeResult;
    private UResultTuple tuples[];
    private int numQueriesExecuted;

    private UError errorHandler;

    private UOutputBuffer outBuffer;

    private int schemaType;
    private String sql_stmt;
    private byte prepare_flag;
    private UInputBuffer tmp_inbuffer;
    private boolean isAutoCommit = false;
    private boolean isGeneratedKeys = false;

    /*
     * 3.0 private int resultset_index; private int resultset_index_flag;
     * private UParameterInfo parameterInfo[]; private int pramNumber;
     */

    public int result_cache_lifetime;
    private boolean result_cacheable = false;
    private UStmtCache stmt_cache;
    private UStatementCacheData stmt_cache_data = null;

    UStatement(
            UConnection relatedC,
            UInputBuffer inBuffer,
            boolean assign_only,
            String sql,
            byte _prepare_flag)
            throws UJciException {
        errorHandler = new UError(relatedC);
        if (assign_only) {
            relatedConnection = relatedC;
            tmp_inbuffer = inBuffer;
            sql_stmt = sql;
            prepare_flag = _prepare_flag;
        } else {
            init(relatedC, inBuffer, sql, _prepare_flag, true);
        }

        if (result_cacheable
                && (prepare_flag & UConnection.PREPARE_INCLUDE_OID) == 0
                && (prepare_flag & UConnection.PREPARE_UPDATABLE) == 0) {
            UUrlCache url_cache = relatedC.getUrlCache();
            stmt_cache = url_cache.getStmtCache(sql);
        } else result_cacheable = false;
    }

    private void init(
            UConnection relatedC,
            UInputBuffer inBuffer,
            String sql,
            byte _prepare_flag,
            boolean clear_bind_info)
            throws UJciException {
        sql_stmt = sql;
        prepare_flag = _prepare_flag;
        outBuffer = relatedC.outBuffer;
        statementType = NORMAL;
        relatedConnection = relatedC;

        serverHandler = inBuffer.getResCode();
        result_cache_lifetime = inBuffer.readInt();
        if (result_cache_lifetime >= 0 && UJCIManager.result_cache_enable) result_cacheable = true;
        commandTypeIs = inBuffer.readByte();
        firstStmtType = commandTypeIs;
        parameterNumber = inBuffer.readInt();
        isUpdatable = (inBuffer.readByte() == 1) ? true : false;
        columnNumber = inBuffer.readInt();
        readColumnInfo(inBuffer);

        if (clear_bind_info) {
            if (parameterNumber > 0) {
                bindParameter =
                        new UBindParameter(parameterNumber, relatedConnection.getDbmsType());
            } else {
                bindParameter = null;
            }
            batchParameter = null;
        }
        fetchSize = DEFAULT_FETCH_SIZE;
        isFetchCompleted = false;
        currentFirstCursor = cursorPosition = totalTupleNumber = fetchedTupleNumber = 0;
        maxFetchSize = 0;
        realFetched = false;
        isClosed = false;

        if (commandTypeIs == TURBOGRAPHCommandType.CUBRID_STMT_CALL_SP)
            columnNumber = parameterNumber + 1;

        /*
         * 3.0 resultset_index = 0; resultset_index_flag =
         * java.sql.Statement.CLOSE_CURRENT_RESULT;
         */
    }

    public void initToReuse() throws UJciException {
        commandTypeIs = firstStmtType;
        isFetchCompleted = false;

        currentFirstCursor = cursorPosition = totalTupleNumber = fetchedTupleNumber = 0;
        maxFetchSize = 0;
        realFetched = false;
        isClosed = false;

        if (commandTypeIs == TURBOGRAPHCommandType.CUBRID_STMT_CALL_SP)
            columnNumber = parameterNumber + 1;
    }

    UStatement(UConnection relatedC, TURBOGRAPHOID oid, String attributeName[], UInputBuffer inBuffer)
            throws UJciException {
        outBuffer = relatedC.outBuffer;
        statementType = GET_BY_OID;
        relatedConnection = relatedC;
        // oidString = oString;

        errorHandler = new UError(relatedConnection);

        serverHandler = -1;

        inBuffer.readString(inBuffer.readInt(), relatedConnection.getCharset());
        columnNumber = inBuffer.readInt();
        readColumnInfo(inBuffer);
        fetchSize = 1;
        tuples = new UResultTuple[fetchSize];

        readATupleByOid(oid, inBuffer);

        bindParameter = null;
        batchParameter = null;
        executeResult = totalTupleNumber = fetchedTupleNumber = 1;
        currentFirstCursor = cursorPosition = 0;
        maxFetchSize = 0;
        realFetched = false;
        isUpdatable = false;
        isClosed = false;

        /*
         * 3.0 resultset_index = 0; resultset_index_flag =
         * java.sql.Statement.CLOSE_CURRENT_RESULT;
         */
    }

    UStatement(
            UConnection relatedC,
            String cName,
            String attributePattern,
            int type,
            UInputBuffer inBuffer)
            throws UJciException {
        outBuffer = relatedC.outBuffer;
        statementType = GET_SCHEMA_INFO;
        relatedConnection = relatedC;
        schemaType = type;

        errorHandler = new UError(relatedConnection);

        serverHandler = inBuffer.getResCode();
        totalTupleNumber = inBuffer.readInt();
        columnNumber = inBuffer.readInt();
        readColumnInfo(inBuffer);

        fetchSize = DEFAULT_FETCH_SIZE;
        currentFirstCursor = cursorPosition = fetchedTupleNumber = 0;
        bindParameter = null;
        batchParameter = null;
        maxFetchSize = 0;
        realFetched = false;
        isUpdatable = false;
        isClosed = false;

        /*
         * 3.0 resultset_index = 0; resultset_index_flag =
         * java.sql.Statement.CLOSE_CURRENT_RESULT;
         */
    }

    public UStatement(UConnection u_con, long id) throws UJciException, IOException {
        relatedConnection = u_con;
        outBuffer = u_con.outBuffer;
        statementType = NORMAL;
        errorHandler = new UError(relatedConnection);
        bindParameter = null;
        fetchSize = DEFAULT_FETCH_SIZE;
        currentFirstCursor = cursorPosition = totalTupleNumber = fetchedTupleNumber = 0;
        maxFetchSize = 0;
        realFetched = false;
        isClosed = false;
        // executeFlag = ASYNC_EXECUTE;

        UInputBuffer inBuffer;
        synchronized (u_con) {
            outBuffer.newRequest(UFunctionCode.MAKE_OUT_RS);
            if (UConnection.protoVersionIsLower(UConnection.PROTOCOL_V11)) {
                outBuffer.addInt((int) id);
            } else {
                outBuffer.addLong(id);
            }
            inBuffer = u_con.send_recv_msg();
        }

        serverHandler = inBuffer.readInt();
        commandTypeIs = inBuffer.readByte();
        totalTupleNumber = inBuffer.readInt();
        isUpdatable = (inBuffer.readByte() == 1) ? true : false;
        columnNumber = inBuffer.readInt();
        readColumnInfo(inBuffer);

        executeResult = totalTupleNumber;
    }

    public UStatement(UStatement u_stmt) {
        serverHandler = u_stmt.serverHandler;
        relatedConnection = u_stmt.relatedConnection;
        outBuffer = u_stmt.outBuffer;
        statementType = NORMAL;
        errorHandler = new UError(relatedConnection);
        bindParameter = null;
        fetchSize = DEFAULT_FETCH_SIZE;
        currentFirstCursor = cursorPosition = totalTupleNumber = fetchedTupleNumber = 0;
        maxFetchSize = 0;
        realFetched = false;
        isClosed = false;
    }

    public UResCache getResCache() {
        UBindKey key;

        if (bindParameter == null) key = new UBindKey(null);
        else if (bindParameter.checkAllBinded() == false) return null;
        else key = new UBindKey(bindParameter.values);

        return ((UResCache) stmt_cache.get(key));
    }

    public int getParameterCount() {
        return parameterNumber;
    }

    public void registerOutParameter(int index, int sqlType) {
        errorHandler = new UError(relatedConnection);
        if (index < 0 || index >= parameterNumber) {
            errorHandler.setErrorCode(UErrorCode.ER_BIND_INDEX);
            return;
        }
        synchronized (bindParameter) {
            try {
                bindParameter.setOutParam(index, sqlType);
            } catch (UJciException e) {
                e.toUError(errorHandler);
            }
        }
    }

    public void bind(int index, boolean value) {
        Byte data =
                new Byte(
                        (value == true)
                                ? ((relatedConnection.getUseOldBooleanValue()) ? OLD_TRUE : TRUE)
                                : FALSE);

        bindValue(index, UUType.U_TYPE_SHORT, data);
    }

    public void bind(int index, byte value) {
        Short data = new Short(value);

        bindValue(index, UUType.U_TYPE_SHORT, data);
    }

    public void bind(int index, short value) {
        Short data = new Short(value);

        bindValue(index, UUType.U_TYPE_SHORT, data);
    }

    public void bind(int index, int value) {
        Integer data = new Integer(value);

        bindValue(index, UUType.U_TYPE_INT, data);
    }

    public void bind(int index, long value) {
        Long data = new Long(value);

        bindValue(index, UUType.U_TYPE_BIGINT, data);
    }

    public void bind(int index, float value) {
        Float data = new Float(value);

        bindValue(index, UUType.U_TYPE_FLOAT, data);
    }

    public void bind(int index, double value) {
        Double data = new Double(value);

        bindValue(index, UUType.U_TYPE_DOUBLE, data);
    }

    public void bind(int index, BigDecimal value) {
        bindValue(index, UUType.U_TYPE_NUMERIC, value);
    }

    public void bind(int index, String value) {
        if (relatedConnection.getOracleStyleEmpltyString()) {
            if ("".equals(value)) value = null;
        }
        bindValue(index, UUType.U_TYPE_STRING, value);
    }

    public void bind(int index, byte[] value) {
        byte[] data;

        if (value == null) data = null;
        else data = (byte[]) value.clone();

        bindValue(index, UUType.U_TYPE_VARBIT, data);
    }

    public void bind(int index, Date value) {
        bindValue(index, UUType.U_TYPE_DATE, value);
    }

    public void bind(int index, Time value) {
        bindValue(index, UUType.U_TYPE_TIME, value);
    }

    public void bind(int index, Timestamp value) {
        byte type = UUType.getObjectDBtype(value);

        bindValue(index, type, value);
    }

    public void bind(int index, TURBOGRAPHTimestamptz value) {
        byte type = UUType.getObjectDBtype(value);

        bindValue(index, type, value);
    }

    public void bind(int index, Object value) {
        byte type = UUType.getObjectDBtype(value);

        if (type == UUType.U_TYPE_SEQUENCE) {
            bindCollection(index, (Object[]) value);
            return;
        }
        if (type == UUType.U_TYPE_NULL && value != null) {
            errorHandler = new UError(relatedConnection);
            errorHandler.setErrorCode(UErrorCode.ER_INVALID_ARGUMENT);
            return;
        }

        if (type == UUType.U_TYPE_STRING) {
            if (relatedConnection.getOracleStyleEmpltyString()) {
                if ("".equals(value)) value = null;
            }
        }

        if (type == UUType.U_TYPE_HUGEINT) {
            value = ((TURBOGRAPHHugeInt)value).getValue();
        }

        bindValue(index, type, value);
    }

    public void bindCollection(int index, Object values[]) {
        TURBOGRAPHArray collectionData;

        if (values == null) {
            collectionData = null;
        } else {
            try {
                if (relatedConnection.getOracleStyleEmpltyString() && values[0] instanceof String) {
                    int length = values.length;
                    for (int i = 0; i < length; i++) {
                        if ("".equals(values[i])) {
                            values[i] = null;
                        }
                    }
                }
                collectionData = new TURBOGRAPHArray(values);
            } catch (UJciException e) {
                errorHandler = new UError(relatedConnection);
                e.toUError(errorHandler);
                return;
            }
        }

        bindValue(index, UUType.U_TYPE_SEQUENCE, collectionData);
    }

    public void bindOID(int index, TURBOGRAPHOID oid) {
        bindValue(index, UUType.U_TYPE_OBJECT, oid);
    }

    public void bindBlob(int index, Blob blob) {
        bindValue(index, UUType.U_TYPE_BLOB, blob);
    }

    public void bindClob(int index, Clob clob) {
        try {
            if (relatedConnection.getOracleStyleEmpltyString()
                    && clob != null
                    && clob.length() == 0) {
                clob = null;
            }
        } catch (SQLException e) {
            relatedConnection.logException(e);
        }
        bindValue(index, UUType.U_TYPE_CLOB, clob);
    }

    public void addBatch() {
        errorHandler = new UError(relatedConnection);

        if (bindParameter == null) return;

        if (bindParameter.checkAllBinded() == false) {
            errorHandler.setErrorCode(UErrorCode.ER_NOT_BIND);
            return;
        }

        if (batchParameter == null) {
            batchParameter = new ArrayList<UBindParameter>();
        }
        batchParameter.add(bindParameter);

        bindParameter = new UBindParameter(parameterNumber, relatedConnection.getDbmsType());
    }

    public void bindNull(int index) {
        bindValue(index, UUType.U_TYPE_NULL, null);
    }

    public UError cancel() {
        UError localError = new UError(relatedConnection);
        if (isClosed == true) {
            localError.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return localError;
        }
        if (statementType == GET_BY_OID) return localError;

        try {
            relatedConnection.cancel();
        } catch (UJciException e) {
            relatedConnection.logException(e);
            e.toUError(localError);
        } catch (IOException e) {
            relatedConnection.logException(e);
            localError.setErrorCode(UErrorCode.ER_COMMUNICATION);
        }
        return localError;
    }

    public void clearBatch() {
        errorHandler = new UError(relatedConnection);
        if (batchParameter == null) return;
        synchronized (batchParameter) {
            batchParameter.clear();
        }
    }

    public synchronized void clearBind() {
        errorHandler = new UError(relatedConnection);
        if (bindParameter == null) return;
        synchronized (bindParameter) {
            bindParameter.clear();
        }
    }

    public synchronized void close(boolean close_srv_handle) {
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return;
        }
        relatedConnection.pooled_ustmts.remove(this);
        currentFirstCursor = cursorPosition = totalTupleNumber = fetchedTupleNumber = 0;
        isClosed = true;
        if (stmt_cache != null) {
            stmt_cache.decr_ref_count();
        }

        try {
            if (close_srv_handle
                    && (relatedConnection.getAutoCommit() == false
                            || relatedConnection.brokerInfoStatementPooling() == true
                            || ((prepare_flag & UConnection.PREPARE_HOLDABLE) != 0))) {
                if (getSqlType()) {
                    synchronized (relatedConnection) {
                        outBuffer.newRequest(UFunctionCode.CLOSE_USTATEMENT);
                        outBuffer.addInt(serverHandler);
                        outBuffer.addByte(relatedConnection.getAutoCommit() ? (byte) 1 : (byte) 0);
                        relatedConnection.send_recv_msg();
                    }
                } else {
                    relatedConnection.deferred_close_handle.add(new Integer(serverHandler));
                }
            }
        } catch (UJciException e) {
            if (relatedConnection.isActive()) {
                e.setStackTrace(e.getStackTrace());
                e.toUError(errorHandler);
            }
        } catch (IOException e) {
            if (relatedConnection.isActive()) {
                e.setStackTrace(e.getStackTrace());
                errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
            }
        } finally {
            currentFirstCursor = cursorPosition = totalTupleNumber = fetchedTupleNumber = 0;
            isClosed = true;
            if (stmt_cache != null) stmt_cache.decr_ref_count();
        }
    }

    public synchronized void close() {
        close(true);
    }

    public synchronized boolean cursorIsInstance(int cursor) {
        errorHandler = new UError(relatedConnection);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return false;
        }
        if ((currentFirstCursor < 0)
                || (cursor < 0)
                || (cursor >= 0
                        && ((cursor < currentFirstCursor)
                                || (cursor > currentFirstCursor + fetchedTupleNumber - 1)))) {
            errorHandler.setErrorCode(UErrorCode.ER_INVALID_ARGUMENT);
            return false;
        }
        if (tuples[cursorPosition - currentFirstCursor].oidIsIncluded() == false) {
            errorHandler.setErrorCode(UErrorCode.ER_OID_IS_NOT_INCLUDED);
            return false;
        }

        Object instance_obj;

        synchronized (relatedConnection) {
            instance_obj =
                    relatedConnection.oidCmd(
                            tuples[cursorPosition - currentFirstCursor].getOid(),
                            UConnection.IS_INSTANCE);
        }
        errorHandler.copyValue(relatedConnection.getRecentError());
        if (instance_obj == null) return false;
        else return true;
    }

    public synchronized void closeCursor() {
        if (relatedConnection.isConnectedToCubrid() == false) {
            return;
        }

        try {
            UFunctionCode code = UFunctionCode.CURSOR_CLOSE;
            if (relatedConnection.protoVersionIsSame(UConnection.PROTOCOL_V2)) {
                code = UFunctionCode.CURSOR_CLOSE_FOR_PROTOCOL_V2;
            }
            outBuffer.newRequest(code);
            outBuffer.addInt(serverHandler);
            relatedConnection.send_recv_msg();
        } catch (UJciException e) {
            relatedConnection.logException(e);
            e.toUError(errorHandler);
        } catch (IOException e) {
            relatedConnection.logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
        }
    }

    public synchronized void deleteCursor(int cursor) {
        errorHandler = new UError(relatedConnection);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return;
        }
        if ((currentFirstCursor < 0)
                || (cursor < 0)
                || (cursor >= 0
                        && ((cursor < currentFirstCursor)
                                || (cursor > currentFirstCursor + fetchedTupleNumber - 1)))) {
            errorHandler.setErrorCode(UErrorCode.ER_INVALID_ARGUMENT);
            return;
        }
        if (tuples[cursorPosition - currentFirstCursor].oidIsIncluded() == false) {
            errorHandler.setErrorCode(UErrorCode.ER_OID_IS_NOT_INCLUDED);
            return;
        }

        synchronized (relatedConnection) {
            relatedConnection.oidCmd(
                    tuples[cursorPosition - currentFirstCursor].getOid(), UConnection.DROP_BY_OID);
        }
        errorHandler.copyValue(relatedConnection.getRecentError());
    }

    private void setExecuteOptions(
            int maxRow,
            boolean isAsync,
            boolean isExecuteAll,
            boolean isQueryPlan,
            boolean isOnlyPlan,
            boolean isHoldable,
            boolean isSensitive) {
        executeFlag = 0;
        if (isAsync) {
            executeFlag |= EXEC_FLAG_ASYNC;
        }
        if (isExecuteAll) {
            executeFlag |= EXEC_FLAG_QUERY_ALL;
        }
        if (isQueryPlan) {
            executeFlag |= EXEC_FLAG_QUERY_INFO;
        }
        if (isOnlyPlan) {
            executeFlag |= EXEC_FLAG_QUERY_INFO;
            executeFlag |= EXEC_FLAG_ONLY_QUERY_PLAN;
        }
        if (isHoldable && relatedConnection.supportHoldableResult()) {
            executeFlag |= EXEC_FLAG_HOLDABLE_RESULT;
        }

        if (isGeneratedKeys) {
            executeFlag |= EXEC_FLAG_GET_GENERATED_KEYS;
        }

        this.isSensitive = isSensitive;
        this.maxFetchSize = maxRow;
    }

    private void writeExecuteRequest(
            int maxField, boolean isScrollable, int queryTimeout, UStatementCacheData cacheData)
            throws IOException, UJciException {
        byte is_auto_commit = (byte) 0, is_forward_only = (byte) 0;
        long remainingTime = 0;

        outBuffer.newRequest(UFunctionCode.EXECUTE);
        outBuffer.addInt(serverHandler);
        outBuffer.addByte(executeFlag);
        outBuffer.addInt(maxField < 0 ? 0 : maxField);
        outBuffer.addInt(0);

        if (firstStmtType == TURBOGRAPHCommandType.CUBRID_STMT_CALL_SP && bindParameter != null) {
            outBuffer.addBytes(bindParameter.paramMode);
        } else {
            outBuffer.addNull();
        }

        /* fetch flag */
        if (firstStmtType == TURBOGRAPHCommandType.CUBRID_STMT_SELECT) {
            outBuffer.addByte((byte) 1);
        } else {
            outBuffer.addByte((byte) 0);
        }

        if (relatedConnection.getAutoCommit() && !isGeneratedKeys) {
            is_auto_commit = (byte) 1;
        }
        outBuffer.addByte(is_auto_commit);

        if (isScrollable == false) {
            is_forward_only = (byte) 1;
        }
        outBuffer.addByte(is_forward_only);
        outBuffer.addCacheTime(cacheData);

        // query timeout support only if protocol version 1 or above
        if (relatedConnection.protoVersionIsAbove(UConnection.PROTOCOL_V2)) {
            // send queryTimeout in milliseconds
            remainingTime = relatedConnection.getRemainingTime(queryTimeout * 1000);
        } else if (relatedConnection.protoVersionIsAbove(UConnection.PROTOCOL_V1)) {
            // send queryTimeout in seconds
            remainingTime = relatedConnection.getRemainingTime(queryTimeout * 1000);
            remainingTime = (long) Math.ceil(remainingTime / 1000.0);
        }
        if (queryTimeout > 0 && remainingTime <= 0) {
            throw relatedConnection.createJciException(UErrorCode.ER_TIMEOUT);
        }
        outBuffer.addInt((int) remainingTime);

        if (bindParameter != null) {
            bindParameter.writeParameter(outBuffer);
        }
    }

    private void readResultMeta(UInputBuffer inBuffer) throws UJciException {
        if (relatedConnection.protoVersionIsAbove(UConnection.PROTOCOL_V2)) {
            // include_column_info
            if (inBuffer.readByte() == 1) {
                inBuffer.readInt(); // result_cache_lifetime
                commandTypeIs = inBuffer.readByte();
                inBuffer.readInt(); // num_markers
                isUpdatable = (inBuffer.readByte() == 1) ? true : false;
                columnNumber = inBuffer.readInt();
                readColumnInfo(inBuffer);
                if (commandTypeIs == TURBOGRAPHCommandType.CUBRID_STMT_CALL_SP) {
                    columnNumber = parameterNumber + 1;
                }
            }
        }
    }

    private void fetchResultData(UInputBuffer inBuffer, UStatementCacheData cacheData)
            throws UJciException {
        executeResult = inBuffer.getResCode();
        if (maxFetchSize > 0) {
            executeResult = Math.min(maxFetchSize, executeResult);
        }
        totalTupleNumber = executeResult;
        batchParameter = null;

        // jdbc cache feature
        if (cacheData != null && resultInfo.length == 1) {
            stmt_cache_data = cacheData;
            stmt_cache_data.setCacheData(totalTupleNumber, resultInfo);
        } else if (resultInfo.length > 1) {
            result_cacheable = false;
        }
        // --

        if (commandTypeIs == TURBOGRAPHCommandType.CUBRID_STMT_SELECT && totalTupleNumber > 0) {
            inBuffer.readInt(); // fetch_rescode
            read_fetch_data(inBuffer, UFunctionCode.FETCH);
        }

        // jdbc cache feature
        if (stmt_cache_data != null) {
            int total_size = fetchedSize + relatedConnection.getUrlCache().getCacheSize();
            if (total_size < relatedConnection.getUrlCache().getLimit()) {
                stmt_cache_data.addCacheData(
                        tuples, currentFirstCursor, fetchedTupleNumber, fetchedSize);
                relatedConnection.getUrlCache().addCacheSize(fetchedSize);
            }
        }
        // --
    }

    private void executeInternal(
            int maxRow,
            int maxField,
            boolean isScrollable,
            int queryTimeout,
            UStatementCacheData cacheData)
            throws UJciException, IOException {
        UInputBuffer inBuffer = null;
        errorHandler.clear();
        relatedConnection.setShardId(UShardInfo.SHARD_ID_INVALID);

        synchronized (relatedConnection) {
            writeExecuteRequest(maxField, isScrollable, queryTimeout, cacheData);
            inBuffer = relatedConnection.send_recv_msg();
        }

        // cache reusable
        byte cache_reusable = inBuffer.readByte();
        if (cacheData != null && cache_reusable == (byte) 1) {
            /* get data from cache */
            getCacheData(cacheData);
            stmt_cache_data = cacheData;
            tuples = cacheData.getTuples(0);
            return;
        }
        // --

        readResultInfo(inBuffer);
        readResultMeta(inBuffer);

        if (relatedConnection.protoVersionIsAbove(UConnection.PROTOCOL_V5)) {
            relatedConnection.setShardId(inBuffer.readInt());
        }

        fetchResultData(inBuffer, cacheData);

        for (int i = 0; i < resultInfo.length; i++) {
            if (resultInfo[i].statementType != TURBOGRAPHCommandType.CUBRID_STMT_SELECT) {
                relatedConnection.update_executed = true;
                break;
            }
        }
    }

    public synchronized void execute(
            boolean isAsync,
            int maxRow,
            int maxField,
            boolean isExecuteAll,
            boolean isSensitive,
            boolean isScrollable,
            boolean isQueryPlan,
            boolean isOnlyPlan,
            boolean isHoldable,
            UStatementCacheData cacheData,
            int queryTimeout) {
        boolean loop;
        byte additional_prepare_flag;

        isFetchCompleted = false;
        flushLobStreams();
        errorHandler = new UError(relatedConnection);

        if (isClosed) {
            if (relatedConnection.brokerInfoStatementPooling()) {
                try {
                    reset((byte) 0);
                } catch (UJciException e) {
                    e.toUError(errorHandler);
                    return;
                }
            } else {
                errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
                return;
            }
        }

        if (statementType == GET_SCHEMA_INFO) {
            return;
        }

        if (bindParameter != null && !bindParameter.checkAllBinded()) {
            errorHandler.setErrorCode(UErrorCode.ER_NOT_BIND);
            return;
        }

        setExecuteOptions(
                maxRow, isAsync, isExecuteAll, isQueryPlan, isOnlyPlan, isHoldable, isSensitive);
        currentFirstCursor = -1;
        fetchedTupleNumber = 0;
        if (firstStmtType == TURBOGRAPHCommandType.CUBRID_STMT_CALL_SP) {
            cursorPosition = 0;
        } else {
            cursorPosition = -1;
        }

        boolean isFirstExecInTran = !relatedConnection.isActive();

        try {
            executeInternal(maxRow, maxField, isScrollable, queryTimeout, cacheData);
            return;
        } catch (UJciException e) {
            relatedConnection.logException(e);
            e.toUError(errorHandler);
        } catch (IOException e) {
            relatedConnection.logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
        }

        if (relatedConnection.isErrorToReconnect(errorHandler.getJdbcErrorCode())) {
            if (!relatedConnection.isActive() || isFirstExecInTran) {
                if (!relatedConnection.brokerInfoReconnectWhenServerDown()
                        || relatedConnection.isErrorCommunication(
                                errorHandler.getJdbcErrorCode())) {
                    relatedConnection.clientSocketClose();
                }

                try {
                    reset((byte) 0);
                    executeInternal(maxRow, maxField, isScrollable, queryTimeout, cacheData);
                    return;
                } catch (UJciException e) {
                    relatedConnection.logException(e);
                    e.toUError(errorHandler);
                } catch (IOException e) {
                    relatedConnection.logException(e);
                    errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
                }
            }
        }

        if (relatedConnection.protoVersionIsAbove(UConnection.PROTOCOL_V7)) {
            loop = false; /* retry once more */
            additional_prepare_flag = UConnection.PREPARE_XASL_CACHE_PINNED;
        } else {
            loop = true; /* infinitely */
            additional_prepare_flag = (byte) 0;
        }

        do {
            if (relatedConnection.brokerInfoStatementPooling()
                    && errorHandler.getJdbcErrorCode() == UErrorCode.CAS_ER_STMT_POOLING) {
                try {
                    reset(additional_prepare_flag);
                    executeInternal(maxRow, maxField, isScrollable, queryTimeout, cacheData);
                    return;
                } catch (UJciException e) {
                    relatedConnection.logException(e);
                    e.toUError(errorHandler);
                } catch (IOException e) {
                    relatedConnection.logException(e);
                    errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
                }
            }
        } while (loop
                && relatedConnection.brokerInfoStatementPooling()
                && errorHandler.getJdbcErrorCode() == UErrorCode.CAS_ER_STMT_POOLING);
    }

    private void reset(byte flag) throws UJciException {
        close();

        UStatement tmp = relatedConnection.prepare(sql_stmt, (byte) (prepare_flag | flag), true);
        UError err = relatedConnection.getRecentError();
        if (err.getErrorCode() != UErrorCode.ER_NO_ERROR) {
            int indicator =
                    (err.getErrorCode() == UErrorCode.ER_DBMS)
                            ? UErrorCode.DBMS_ERROR_INDICATOR
                            : UErrorCode.CAS_ERROR_INDICATOR;

            throw new UJciException(
                    err.getErrorCode(), indicator, err.getJdbcErrorCode(), err.getErrorMsg(false));
        }

        relatedConnection.pooled_ustmts.remove(tmp);
        relatedConnection.pooled_ustmts.add(this);

        init(relatedConnection, tmp.tmp_inbuffer, sql_stmt, prepare_flag, false);
    }

    public synchronized TURBOGRAPHOID executeInsert(boolean isAsync) {
        errorHandler = new UError(relatedConnection);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return null;
        }
        if (commandTypeIs != TURBOGRAPHCommandType.CUBRID_STMT_INSERT) {
            errorHandler.setErrorCode(UErrorCode.ER_CMD_IS_NOT_INSERT);
            return null;
        }
        execute(isAsync, 0, 0, false, false, false, false, false, false, null, 0);
        if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return null;
        if (resultInfo != null && resultInfo[0] != null) return resultInfo[0].getCUBRIDOID();
        errorHandler.setErrorCode(UErrorCode.ER_OID_IS_NOT_INCLUDED);
        return null;
    }

    public synchronized void setAutoCommit(boolean autoCommit) {
        isAutoCommit = autoCommit;
    }

    private void writeExecuteBatchRequest(int queryTimeout) throws IOException, UJciException {
        outBuffer.newRequest(
                relatedConnection.getOutputStream(), UFunctionCode.EXECUTE_BATCH_PREPAREDSTATEMENT);
        outBuffer.addInt(serverHandler);
        if (relatedConnection.protoVersionIsAbove(UConnection.PROTOCOL_V4)) {
            long remainingTime = relatedConnection.getRemainingTime(queryTimeout * 1000);
            if (queryTimeout > 0 && remainingTime <= 0) {
                throw relatedConnection.createJciException(UErrorCode.ER_TIMEOUT);
            }
            outBuffer.addInt((int) remainingTime);
        }
        outBuffer.addByte(isAutoCommit ? (byte) 1 : (byte) 0);

        if (batchParameter != null) {
            synchronized (batchParameter) {
                for (int i = 0; i < batchParameter.size(); i++) {
                    UBindParameter b = (UBindParameter) batchParameter.get(i);
                    b.writeParameter(outBuffer);
                }
            }
        }
    }

    private UBatchResult executeBatchInternal(int queryTimeout) throws IOException, UJciException {
        UInputBuffer inBuffer = null;
        errorHandler.clear();
        relatedConnection.setShardId(UShardInfo.SHARD_ID_INVALID);

        synchronized (relatedConnection) {
            writeExecuteBatchRequest(queryTimeout);
            inBuffer = relatedConnection.send_recv_msg();
        }

        batchParameter = null;
        UBatchResult batchResult;
        int result;

        batchResult = new UBatchResult(inBuffer.readInt());
        for (int i = 0; i < batchResult.getResultNumber(); i++) {
            batchResult.setStatementType(i, statementType);
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

        if (relatedConnection.protoVersionIsAbove(UConnection.PROTOCOL_V5)) {
            relatedConnection.setShardId(inBuffer.readInt());
        }

        return batchResult;
    }

    public synchronized UBatchResult executeBatch(int queryTimeout) {
        UBatchResult batchResult;
        boolean loop;
        byte additional_prepare_flag;

        errorHandler = new UError(relatedConnection);
        if (isClosed) {
            if (relatedConnection.brokerInfoStatementPooling()) {
                try {
                    reset((byte) 0);
                } catch (UJciException e) {
                    e.toUError(errorHandler);
                    return null;
                }
            } else {
                errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
                return null;
            }
        }

        boolean isFirstExecInTran = !relatedConnection.isActive();

        try {
            batchResult = executeBatchInternal(queryTimeout);
            return batchResult;
        } catch (UJciException e) {
            relatedConnection.logException(e);
            e.toUError(errorHandler);
        } catch (IOException e) {
            relatedConnection.logException(e);
            if (errorHandler.getErrorCode() != UErrorCode.ER_CONNECTION)
                errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
        }

        if (relatedConnection.isErrorToReconnect(errorHandler.getJdbcErrorCode())) {
            if (!relatedConnection.isActive() || isFirstExecInTran) {
                if (!relatedConnection.brokerInfoReconnectWhenServerDown()
                        || relatedConnection.isErrorCommunication(
                                errorHandler.getJdbcErrorCode())) {
                    relatedConnection.clientSocketClose();
                }

                try {
                    reset((byte) 0);
                    batchResult = executeBatchInternal(queryTimeout);
                    return batchResult;
                } catch (UJciException e) {
                    relatedConnection.logException(e);
                    e.toUError(errorHandler);
                } catch (IOException e) {
                    relatedConnection.logException(e);
                    errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
                }
            }
        }

        if (relatedConnection.protoVersionIsAbove(UConnection.PROTOCOL_V7)) {
            loop = false; /* retry once more */
            additional_prepare_flag = UConnection.PREPARE_XASL_CACHE_PINNED;
        } else {
            loop = true; /* infinitely */
            additional_prepare_flag = (byte) 0;
        }

        do {
            if (relatedConnection.brokerInfoStatementPooling()
                    && errorHandler.getJdbcErrorCode() == UErrorCode.CAS_ER_STMT_POOLING) {
                try {
                    reset(additional_prepare_flag);
                    batchResult = executeBatchInternal(queryTimeout);
                    return batchResult;
                } catch (UJciException e) {
                    relatedConnection.logException(e);
                    e.toUError(errorHandler);
                } catch (IOException e) {
                    relatedConnection.logException(e);
                    errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
                }
            }
        } while (loop
                && relatedConnection.brokerInfoStatementPooling()
                && errorHandler.getJdbcErrorCode() == UErrorCode.CAS_ER_STMT_POOLING);
        return null;
    }

    public synchronized void fetch() {
        errorHandler = new UError(relatedConnection);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return;
        }
        realFetched = false;

        /* need not to fetch */
        if (statementType == GET_BY_OID) return;

        if (cursorPosition < 0 || (executeFlag != ASYNC_EXECUTE && totalTupleNumber <= 0)) {
            errorHandler.setErrorCode(UErrorCode.ER_NO_MORE_DATA);
            return;
        }

        /* need not to fetch really */
        if (currentFirstCursor >= 0
                && currentFirstCursor <= cursorPosition
                && cursorPosition <= currentFirstCursor + fetchedTupleNumber - 1) {
            return;
        }

        reFetch();
    }

    public synchronized BigDecimal getBigDecimal(int index) {
        errorHandler = new UError(relatedConnection);

        Object obj = beforeGetXXX(index);
        if (obj == null) return null;

        try {
            return (UGetTypeConvertedValue.getBigDecimal(obj));
        } catch (UJciException e) {
            e.toUError(errorHandler);
        }
        return null;
    }

    public synchronized boolean getBoolean(int index) {
        errorHandler = new UError(relatedConnection);

        Object obj = beforeGetXXX(index);
        if (obj == null) return false;

        try {
            return (UGetTypeConvertedValue.getBoolean(obj));
        } catch (UJciException e) {
            e.toUError(errorHandler);
        }
        return false;
    }

    public synchronized byte getByte(int index) {
        errorHandler = new UError(relatedConnection);

        Object obj = beforeGetXXX(index);
        if (obj == null) return ((byte) 0);

        try {
            return (UGetTypeConvertedValue.getByte(obj));
        } catch (UJciException e) {
            e.toUError(errorHandler);
        }
        return ((byte) 0);
    }

    public synchronized byte[] getBytes(int index) {
        errorHandler = new UError(relatedConnection);

        Object obj = beforeGetXXX(index);
        if (obj == null) return null;

        try {
            return (UGetTypeConvertedValue.getBytes(obj));
        } catch (UJciException e) {
            e.toUError(errorHandler);
        }
        return null;
    }

    public synchronized Object getCollection(int index) {
        errorHandler = new UError(relatedConnection);

        Object obj = beforeGetXXX(index);
        if (obj == null) return null;

        if (!(obj instanceof TURBOGRAPHArray)) {
            errorHandler.setErrorCode(UErrorCode.ER_TYPE_CONVERSION);
            return null;
        }

        return (((TURBOGRAPHArray) obj).getArrayClone());
    }

    public UColumnInfo[] getColumnInfo() {
        UError localError;

        localError = new UError(relatedConnection);
        if (isClosed == true) {
            localError.setErrorCode(UErrorCode.ER_IS_CLOSED);
            errorHandler = localError;
            return null;
        }
        errorHandler = localError;
        return columnInfo;
    }

    public HashMap<String, Integer> getColumnNameToIndexMap() {
        UError localError;

        localError = new UError(relatedConnection);
        if (isClosed == true) {
            localError.setErrorCode(UErrorCode.ER_IS_CLOSED);
            errorHandler = localError;
            return null;
        }
        errorHandler = localError;
        return colNameToIndex;
    }

    public synchronized TURBOGRAPHOID getColumnOID(int index) {
        errorHandler = new UError(relatedConnection);

        Object obj = beforeGetXXX(index);
        if (obj == null) return null;

        if (!(obj instanceof TURBOGRAPHOID)) {
            errorHandler.setErrorCode(UErrorCode.ER_NOT_OBJECT);
            return null;
        }

        return ((TURBOGRAPHOID) obj);
    }

    public synchronized TURBOGRAPHOID getCursorOID() {
        errorHandler = new UError(relatedConnection);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return null;
        }

        if (checkReFetch() != true) return null;

        return (tuples[cursorPosition - currentFirstCursor].getOid());
    }

    public synchronized TURBOGRAPHBlob getBlob(int index) {
        errorHandler = new UError(relatedConnection);

        Object obj = beforeGetXXX(index);
        if (obj == null) return null;

        try {
            return (UGetTypeConvertedValue.getBlob(obj, relatedConnection.getCUBRIDConnection()));
        } catch (UJciException e) {
            e.toUError(errorHandler);
        }
        return null;
    }

    public synchronized TURBOGRAPHClob getClob(int index) {
        errorHandler = new UError(relatedConnection);

        Object obj = beforeGetXXX(index);
        if (obj == null) return null;

        try {
            return (UGetTypeConvertedValue.getClob(obj, relatedConnection.getCUBRIDConnection()));
        } catch (UJciException e) {
            e.toUError(errorHandler);
        }
        return null;
    }

    public synchronized Date getDate(int index) {
        errorHandler = new UError(relatedConnection);

        Object obj = beforeGetXXX(index);
        if (obj == null) return null;

        try {
            return (UGetTypeConvertedValue.getDate(obj));
        } catch (UJciException e) {
            e.toUError(errorHandler);
        }
        return null;
    }

    public synchronized double getDouble(int index) {
        errorHandler = new UError(relatedConnection);

        Object obj = beforeGetXXX(index);
        if (obj == null) return ((double) 0);

        try {
            return (UGetTypeConvertedValue.getDouble(obj));
        } catch (UJciException e) {
            e.toUError(errorHandler);
        }
        return ((double) 0);
    }

    public int getExecuteResult() {
        UError localError = new UError(relatedConnection);
        if (isClosed == true) {
            localError.setErrorCode(UErrorCode.ER_IS_CLOSED);
            errorHandler = localError;
            return 0;
        }
        errorHandler = localError;
        return executeResult;
    }

    public int getFetchDirection() {
        UError localError = new UError(relatedConnection);
        if (isClosed == true) {
            localError.setErrorCode(UErrorCode.ER_IS_CLOSED);
            errorHandler = localError;
            return 0;
        }
        errorHandler = localError;
        return fetchDirection;
    }

    public int getFetchSize() {
        UError localError = new UError(relatedConnection);
        if (isClosed == true) {
            localError.setErrorCode(UErrorCode.ER_IS_CLOSED);
            errorHandler = localError;
            return 0;
        }
        errorHandler = localError;
        return fetchSize;
    }

    public synchronized float getFloat(int index) {
        errorHandler = new UError(relatedConnection);

        Object obj = beforeGetXXX(index);
        if (obj == null) return ((float) 0);

        try {
            return (UGetTypeConvertedValue.getFloat(obj));
        } catch (UJciException e) {
            e.toUError(errorHandler);
        }
        return ((float) 0);
    }

    public synchronized int getInt(int index) {
        errorHandler = new UError(relatedConnection);

        Object obj = beforeGetXXX(index);
        if (obj == null) return 0;

        try {
            return (UGetTypeConvertedValue.getInt(obj));
        } catch (UJciException e) {
            e.toUError(errorHandler);
        }

        return 0;
    }

    public synchronized long getLong(int index) {
        errorHandler = new UError(relatedConnection);

        Object obj = beforeGetXXX(index);
        if (obj == null) return ((long) 0);

        try {
            return (UGetTypeConvertedValue.getLong(obj));
        } catch (UJciException e) {
            e.toUError(errorHandler);
        }

        return ((long) 0);
    }

    public synchronized Object getObject(int index) {
        errorHandler = new UError(relatedConnection);

        Object obj = beforeGetXXX(index);
        if (obj == null) return null;

        Object retValue;
        try {
            if ((commandTypeIs != TURBOGRAPHCommandType.CUBRID_STMT_CALL_SP)
                    && (columnInfo[index].getColumnType() == UUType.U_TYPE_BIT)
                    && (columnInfo[index].getColumnPrecision() == 8)) {
                retValue = new Boolean(UGetTypeConvertedValue.getBoolean(obj));
            } else if (obj instanceof TURBOGRAPHArray) retValue = ((TURBOGRAPHArray) obj).getArrayClone();
            else if (obj instanceof byte[]) retValue = ((byte[]) obj).clone();
            else if (obj instanceof Date) retValue = ((Date) obj).clone();
            else if (obj instanceof Time) retValue = ((Time) obj).clone();
            else if (obj instanceof Timestamp) {
                if (relatedConnection
                        .getResultWithCUBRIDTypes()
                        .equals(UConnection.RESULT_WITH_CUBRID_TYPES_NO))
                    retValue = new Timestamp(((Timestamp) obj).getTime());
                else retValue = ((Timestamp) obj).clone();
            } else if (obj instanceof TURBOGRAPHOutResultSet) {
                try {
                    ((TURBOGRAPHOutResultSet) obj).createInstance();
                    retValue = obj;
                } catch (Exception e) {
                    retValue = null;
                }
            } else if (relatedConnection.isOracleCompatNumberBehavior()
                    && (obj instanceof Double)) {
                retValue =
                        new BigDecimal(((Number) obj).doubleValue(), MathContext.DECIMAL64)
                                .stripTrailingZeros()
                                .toPlainString();
            } else if (relatedConnection.isOracleCompatNumberBehavior() && (obj instanceof Float)) {
                retValue =
                        new BigDecimal(((Number) obj).floatValue(), MathContext.DECIMAL32)
                                .stripTrailingZeros()
                                .toPlainString();
            } else retValue = obj;
        } catch (UJciException e) {
            e.toUError(errorHandler);
            return null;
        }

        return retValue;
    }

    public UError getRecentError() {
        return errorHandler;
    }

    public UResultInfo[] getResultInfo() {
        UError localError;

        localError = new UError(relatedConnection);
        if (isClosed == true) {
            localError.setErrorCode(UErrorCode.ER_IS_CLOSED);
            errorHandler = localError;
            return null;
        }
        errorHandler = localError;
        return resultInfo;
    }

    public synchronized short getShort(int index) {
        errorHandler = new UError(relatedConnection);

        Object obj = beforeGetXXX(index);
        if (obj == null) return ((short) 0);

        try {
            return (UGetTypeConvertedValue.getShort(obj));
        } catch (UJciException e) {
            e.toUError(errorHandler);
        }
        return ((short) 0);
    }

    public boolean getSqlType() {
        UError localError = new UError(relatedConnection);
        if (isClosed == true) {
            localError.setErrorCode(UErrorCode.ER_IS_CLOSED);
            errorHandler = localError;
            return false;
        }
        if ((commandTypeIs == TURBOGRAPHCommandType.CUBRID_STMT_SELECT)
                || (commandTypeIs == TURBOGRAPHCommandType.CUBRID_STMT_CALL)
                || (commandTypeIs == TURBOGRAPHCommandType.CUBRID_STMT_GET_STATS)
                || (commandTypeIs == TURBOGRAPHCommandType.CUBRID_STMT_EVALUATE)) {
            errorHandler = localError;
            return true;
        } else {
            errorHandler = localError;
            return false;
        }
    }

    public synchronized String getString(int index) {
        String numString = null;

        errorHandler = new UError(relatedConnection);

        Object obj = beforeGetXXX(index);
        if (obj == null) return null;

        try {
            if (relatedConnection.isOracleCompatNumberBehavior() && (obj instanceof Double)) {
                return new BigDecimal(((Number) obj).doubleValue(), MathContext.DECIMAL64)
                        .stripTrailingZeros()
                        .toPlainString();
            } else if (relatedConnection.isOracleCompatNumberBehavior() && (obj instanceof Float)) {
                return new BigDecimal(((Number) obj).floatValue(), MathContext.DECIMAL32)
                        .stripTrailingZeros()
                        .toPlainString();
            } else {
                return (UGetTypeConvertedValue.getString(obj));
            }
        } catch (UJciException e) {
            e.toUError(errorHandler);
        }
        return null;
    }

    public synchronized Time getTime(int index) {
        errorHandler = new UError(relatedConnection);

        Object obj = beforeGetXXX(index);
        if (obj == null) return null;

        try {
            return (UGetTypeConvertedValue.getTime(obj));
        } catch (UJciException e) {
            e.toUError(errorHandler);
        }
        return null;
    }

    public synchronized Timestamp getTimestamp(int index) {
        errorHandler = new UError(relatedConnection);

        Object obj = beforeGetXXX(index);
        if (obj == null) return null;

        try {
            return (UGetTypeConvertedValue.getTimestamp(obj));
        } catch (UJciException e) {
            e.toUError(errorHandler);
        }
        return null;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public boolean isOIDIncluded() {
        UError localError = new UError(relatedConnection);
        if (isClosed == true) {
            localError.setErrorCode(UErrorCode.ER_IS_CLOSED);
            errorHandler = localError;
            return false;
        }
        errorHandler = localError;
        return isUpdatable;
    }

    public synchronized void moveCursor(int offset, int origin) {
        UInputBuffer inBuffer;

        errorHandler = new UError(relatedConnection);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return;
        }
        if ((origin != CURSOR_SET && origin != CURSOR_CUR && origin != CURSOR_END)
                || (executeFlag != ASYNC_EXECUTE && totalTupleNumber == 0)) {
            errorHandler.setErrorCode(UErrorCode.ER_NO_MORE_DATA);
            return;
        }

        int currentCursor = cursorPosition;

        if (origin == CURSOR_SET) cursorPosition = offset;
        else if (origin == CURSOR_CUR) cursorPosition += offset;
        if (origin == CURSOR_SET || origin == CURSOR_CUR) {
            if (executeFlag == ASYNC_EXECUTE) {
                if ((cursorPosition <= currentFirstCursor + fetchedTupleNumber - 1)
                        || (totalTupleNumber != 0 && cursorPosition < totalTupleNumber)) return;
            } else if (cursorPosition < totalTupleNumber) return;
            else {
                errorHandler.setErrorCode(UErrorCode.ER_NO_MORE_DATA);
                cursorPosition = currentCursor;
                return;
            }
        }
        if (origin == CURSOR_END && totalTupleNumber != 0) {
            cursorPosition = totalTupleNumber - offset - 1;
            if (cursorPosition >= 0) return;
            else {
                errorHandler.setErrorCode(UErrorCode.ER_NO_MORE_DATA);
                cursorPosition = currentCursor;
                return;
            }
        }
        if (origin == CURSOR_CUR) {
            origin = CURSOR_SET;
            offset += currentCursor;
        }
        try {
            synchronized (relatedConnection) {
                outBuffer.newRequest(UFunctionCode.CURSOR);
                outBuffer.addInt(serverHandler);
                outBuffer.addInt(offset);
                outBuffer.addInt(origin);

                inBuffer = relatedConnection.send_recv_msg();
            }

            totalTupleNumber = inBuffer.readInt();
        } catch (UJciException e) {
            relatedConnection.logException(e);
            cursorPosition = currentCursor;
            e.toUError(errorHandler);
            return;
        } catch (IOException e) {
            relatedConnection.logException(e);
            cursorPosition = currentCursor;
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
            return;
        }

        if (totalTupleNumber < 0) {
            totalTupleNumber = 0;
        } else if (totalTupleNumber <= cursorPosition) {
            errorHandler.setErrorCode(UErrorCode.ER_NO_MORE_DATA);
            cursorPosition = currentCursor;
        }
    }

    public synchronized boolean nextResult() {
        UInputBuffer inBuffer;

        errorHandler = new UError(relatedConnection);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return false;
        }
        try {
            synchronized (relatedConnection) {
                outBuffer.newRequest(UFunctionCode.NEXT_RESULT);
                outBuffer.addInt(serverHandler);
                // jci 3.0
                outBuffer.addInt(0);

                inBuffer = relatedConnection.send_recv_msg();
            }

            executeResult = inBuffer.readInt();
            commandTypeIs = inBuffer.readByte();
            isUpdatable = (inBuffer.readByte() == 1) ? true : false;
            columnNumber = inBuffer.readInt();
            readColumnInfo(inBuffer);
        } catch (UJciException e) {
            relatedConnection.logException(e);
            closeInternal();
            e.toUError(errorHandler);
            return false;
        } catch (IOException e) {
            relatedConnection.logException(e);
            closeInternal();
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
            return false;
        }

        fetchedTupleNumber = 0;
        currentFirstCursor = cursorPosition = -1;
        /* set max resultset row size */
        totalTupleNumber =
                executeResult =
                        ((maxFetchSize > 0) && (executeResult > maxFetchSize))
                                ? maxFetchSize
                                : executeResult;
        realFetched = false;
        return true;
    }

    /*
     * 3.0 synchronized public boolean nextResult (int rs_mode) { UInputBuffer
     * inBuffer;
     *
     * errorHandler = new UError(); if (isClosed == true) {
     * errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED); return false; } try {
     * synchronized (relatedConnection) {
     * outBuffer.newRequest(UFunctionCode.NEXT_RESULT);
     * outBuffer.addInt(serverHandler); outBuffer.addInt(rs_mode);
     *
     * inBuffer = relatedConnection.send_recv_msg(); }
     *
     * closeInternal(); executeResult = inBuffer.readInt(); commandTypeIs =
     * inBuffer.readByte(); isUpdatable = (inBuffer.readByte() == 1) ? true :
     * false; columnNumber = inBuffer.readInt(); columnInfo =
     * readColumnInfo(inBuffer); } catch (UJciException e) { closeInternal();
     * e.toUError(errorHandler); return false; } catch (IOException e) {
     * closeInternal(); errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
     * return false; }
     *
     * fetchedTupleNumber = 0; currentFirstCursor = cursorPosition = -1; // set
     * max resultset row size totalTupleNumber = executeResult =
     * ((maxFetchSize>0) && (executeResult>maxFetchSize)) ? maxFetchSize :
     * executeResult; realFetched = false; return true; } 3.0
     */

    public boolean realFetched() {
        return realFetched;
    }

    public boolean tryTofetchFromCache() {
        int fetch;
        int first;

        for (int i = 0; i < stmt_cache_data.numberOfCursorIndex(); i++) {
            fetch = stmt_cache_data.getFetchNumber(i);
            first = stmt_cache_data.getFirstCursor(i);
            if (cursorPosition >= first && cursorPosition <= first + fetch - 1) {
                tuples = stmt_cache_data.getTuples(i);
                currentFirstCursor = first;
                fetchedTupleNumber = fetch;

                return true;
            }
        }

        return false;
    }

    public synchronized void reFetch() {
        UInputBuffer inBuffer;

        if (stmt_cache_data != null) {
            if (tryTofetchFromCache()) {
                return;
            }
        }

        errorHandler = new UError(relatedConnection);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return;
        }

        if (statementType == GET_BY_OID) return;

        try {
            synchronized (relatedConnection) {
                outBuffer.newRequest(UFunctionCode.FETCH);
                outBuffer.addInt(serverHandler);
                if (fetchDirection == ResultSet.FETCH_REVERSE) {
                    int startPos = cursorPosition - fetchSize + 2;
                    if (startPos < 1) startPos = 1;
                    outBuffer.addInt(startPos);
                } else {
                    outBuffer.addInt(cursorPosition + 1);
                }
                outBuffer.addInt(fetchSize);
                outBuffer.addByte((isSensitive == true) ? (byte) 1 : (byte) 0);
                // jci 3.0
                outBuffer.addInt(0);
                // outBuffer.addInt(resultset_index);

                inBuffer = relatedConnection.send_recv_msg();
            }

            read_fetch_data(inBuffer, UFunctionCode.FETCH);
            realFetched = true;
        } catch (UJciException e) {
            relatedConnection.logException(e);
            e.toUError(errorHandler);
        } catch (IOException e) {
            relatedConnection.logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
        }
    }

    public synchronized void setFetchDirection(int direction) {
        errorHandler = new UError(relatedConnection);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return;
        }
        if (direction != ResultSet.FETCH_FORWARD
                && direction != ResultSet.FETCH_REVERSE
                && direction != ResultSet.FETCH_UNKNOWN) {
            errorHandler.setErrorCode(UErrorCode.ER_INVALID_ARGUMENT);
            return;
        }
        fetchDirection = direction;
    }

    public synchronized void setFetchSize(int size) {
        errorHandler = new UError(relatedConnection);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return;
        }

        if (size < 0) {
            errorHandler.setErrorCode(UErrorCode.ER_INVALID_ARGUMENT);
            return;
        }

        if (size == 0) fetchSize = DEFAULT_FETCH_SIZE;
        else fetchSize = size;
    }

    public synchronized void updateRows(int cursorPosition, int[] indexes, Object[] values) {
        errorHandler = new UError(relatedConnection);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return;
        }

        UUpdateParameter updateParameter;

        try {
            updateParameter = new UUpdateParameter(columnInfo, indexes, values);
            synchronized (relatedConnection) {
                outBuffer.newRequest(UFunctionCode.CURSOR_UPDATE);
                outBuffer.addInt(serverHandler);
                outBuffer.addInt(cursorPosition + 1);
                updateParameter.writeParameter(outBuffer);

                relatedConnection.send_recv_msg();
            }
        } catch (UJciException e) {
            relatedConnection.logException(e);
            e.toUError(errorHandler);
        } catch (IOException e) {
            relatedConnection.logException(e);
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
        }
    }

    public synchronized byte getCommandType() {
        return commandTypeIs;
    }

    /*
     * 3.0 synchronized public UParameterInfo[] getParameterInfo() {
     * UInputBuffer inBuffer;
     *
     * errorHandler = new UError(); if (isClosed == true) {
     * errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED); return null; } try {
     * synchronized (relatedConnection) {
     * outBuffer.newRequest(UFunctionCode.PARAMETER_INFO);
     * outBuffer.addInt(serverHandler);
     *
     * inBuffer = relatedConnection.send_recv_msg(); }
     *
     * pramNumber = inBuffer.getResCode(); return readParameterInfo(); } catch
     * (UJciException e) { e.toUError(errorHandler); } catch (IOException e) {
     * errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION); } return null; }
     * 3.0
     */

    public synchronized String getQueryplan() {
        String plan = null;

        errorHandler = new UError(relatedConnection);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return null;
        }

        try {
            UInputBuffer inBuffer;

            synchronized (relatedConnection) {
                outBuffer.newRequest(UFunctionCode.GET_QUERY_INFO);
                outBuffer.addInt(serverHandler);
                outBuffer.addByte(QUERY_INFO_PLAN);

                inBuffer = relatedConnection.send_recv_msg();
            }

            plan = inBuffer.readString(inBuffer.remainedCapacity(), relatedConnection.getCharset());
        } catch (UJciException e) {
            relatedConnection.logException(e);
            closeInternal();
            e.toUError(errorHandler);
            return null;
        } catch (IOException e) {
            relatedConnection.logException(e);
            closeInternal();
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
            return null;
        }

        return plan;
    }

    public synchronized boolean getGeneratedKeys() {
        errorHandler = new UError(relatedConnection);
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return false;
        }

        try {
            UInputBuffer inBuffer;

            synchronized (relatedConnection) {
                outBuffer.newRequest(UFunctionCode.GET_GENERATED_KEYS);
                outBuffer.addInt(serverHandler);
                inBuffer = relatedConnection.send_recv_msg();

                commandTypeIs = inBuffer.readByte();
                totalTupleNumber = inBuffer.readInt();
                isUpdatable = (inBuffer.readByte() == 1) ? true : false;
                columnNumber = inBuffer.readInt();
                statementType = GET_AUTOINCREMENT_KEYS;
                readColumnInfo(inBuffer);
                executeResult = totalTupleNumber;
                read_fetch_data(inBuffer, UFunctionCode.GET_GENERATED_KEYS);
            }
        } catch (UJciException e) {
            relatedConnection.logException(e);
            closeInternal();
            e.toUError(errorHandler);
            return false;
        } catch (IOException e) {
            relatedConnection.logException(e);
            closeInternal();
            errorHandler.setErrorCode(UErrorCode.ER_COMMUNICATION);
            return false;
        }

        return true;
    }

    private void bindValue(int index, byte type, Object data) {
        UError localError;

        localError = new UError(relatedConnection);
        if (bindParameter == null || index < 0 || index >= parameterNumber) {
            localError.setErrorCode(UErrorCode.ER_BIND_INDEX);
            errorHandler = localError;
            return;
        }

        try {
            synchronized (bindParameter) {
                bindParameter.setParameter(index, type, data);
            }
        } catch (UJciException e) {
            e.toUError(localError);
        }
        errorHandler = localError;
    }

    private Object beforeGetXXX(int index) {
        if (isClosed == true) {
            errorHandler.setErrorCode(UErrorCode.ER_IS_CLOSED);
            return null;
        }
        if (index < 0 || index >= columnNumber) {
            errorHandler.setErrorCode(UErrorCode.ER_COLUMN_INDEX);
            return null;
        }

        if (checkReFetch() != true) return null;

        if (fetchedTupleNumber <= 0) {
            errorHandler.setErrorCode(UErrorCode.ER_NO_MORE_DATA);
            return null;
        }

        /*
         * if (tuples == null || tuples[cursorPosition - currentFirstCursor] ==
         * null || tuples[cursorPosition-currentFirstCursor].wasNull(index) ==
         * true)
         */
        Object obj;
        if ((tuples == null)
                || (tuples[cursorPosition - currentFirstCursor] == null)
                || ((obj = tuples[cursorPosition - currentFirstCursor].getAttribute(index))
                        == null)) {
            errorHandler.setErrorCode(UErrorCode.ER_WAS_NULL);
            return null;
        }

        return obj;
    }

    private boolean checkReFetch() {
        if ((currentFirstCursor < 0)
                || (cursorPosition >= 0
                        && ((cursorPosition < currentFirstCursor)
                                || (cursorPosition
                                        > currentFirstCursor + fetchedTupleNumber - 1)))) {
            fetch();
            if (errorHandler.getErrorCode() != UErrorCode.ER_NO_ERROR) return false;
        }
        return true;
    }

    private byte[] readTypeFromData(int index, UInputBuffer inBuffer) throws UJciException {
        byte typeInfo[];

        typeInfo = new byte[4];
        /* first two bytes are reserverd for type value */
        typeInfo[2] = 0; /* this holds how many bytes we read for type */
        typeInfo[3] = DEFAULT_CHARSET; /* this holds charset id */
        if ((commandTypeIs == TURBOGRAPHCommandType.CUBRID_STMT_CALL)
                || (commandTypeIs == TURBOGRAPHCommandType.CUBRID_STMT_EVALUATE)
                || (commandTypeIs == TURBOGRAPHCommandType.CUBRID_STMT_CALL_SP)
                || (columnInfo[index].getColumnType() == UUType.U_TYPE_NULL)) {
            byte collectionByte, setType;

            collectionByte = inBuffer.readByte();
            if ((byte) (collectionByte & MASK_TYPE_HAS_2_BYTES) == 0) {
                /* legacy protocol : we expect only simple type here */
                typeInfo[0] = collectionByte;
                typeInfo[2] = 1;
            } else {
                byte typeInfoColumn[];
                byte charset;

                charset = (byte) (collectionByte & MASK_CHARSET_FROM_TYPE);
                collectionByte = (byte) (collectionByte & MASK_COLLECTION_FROM_TYPE);
                setType = inBuffer.readByte();
                typeInfoColumn = UColumnInfo.confirmType(setType, collectionByte);
                typeInfo[0] = typeInfoColumn[0];
                typeInfo[1] = typeInfoColumn[1];
                typeInfo[2] = 2;
                typeInfo[3] = charset;
            }
        } else {
            typeInfo[0] = UUType.U_TYPE_NULL;
        }
        return typeInfo;
    }

    private void closeInternal() {
        if (columnInfo != null) {
            synchronized (columnInfo) {
                for (int i = 0; i < columnInfo.length; i++) columnInfo[i] = null;
            }
            columnInfo = null;

            colNameToIndex.clear();
            colNameToIndex = null;
        }
        if (bindParameter != null) {
            synchronized (bindParameter) {
                bindParameter.close();
            }
            bindParameter = null;
        }

        closeResult();
    }

    public void closeTuples(UResultTuple[] tuples) {
        if (tuples != null) {
            synchronized (tuples) {
                for (int i = 0; i < tuples.length; i++) {
                    if (tuples[i] == null) continue;
                    tuples[i].close();
                    tuples[i] = null;
                }
            }
            tuples = null;
        }
    }

    public void closeResult() {
        if (stmt_cache_data != null) {
            getResCache().setExpire();
            for (int n = stmt_cache_data.tuples.size(); n > 0; n--) {
                if (tuples == stmt_cache_data.tuples.get(n - 1)) {
                    /* do not remove tuples is cached
                    the tuples will be removed on a condition
                    in daemon thread of driver
                    */
                    return;
                }
            }
        }

        closeTuples(tuples);
    }

    private void confirmSchemaTypeInfo(int index) throws UJciException {
        if (statementType != GET_SCHEMA_INFO) return;

        byte realType[];
        Short fetchedType;
        Short type_msb_byte;
        Short type_lsb_byte;
        byte collectionFlags;
        byte originalType;

        switch (schemaType) {
            case USchType.SCH_ATTRIBUTE:
            case USchType.SCH_CLASS_ATTRIBUTE:
            case USchType.SCH_METHOD:
            case USchType.SCH_CLASS_METHOD:
            case USchType.SCH_SUPERCLASS:
            case USchType.SCH_SUBCLASS:
                fetchedType = (Short) tuples[index].getAttribute(1);

                type_msb_byte = (short) (fetchedType & 0xff00);
                type_msb_byte = (short) (type_msb_byte >> 8);
                if (((short) type_msb_byte & MASK_TYPE_HAS_2_BYTES) == MASK_TYPE_HAS_2_BYTES) {
                    /* V7 protocol (extended type has two bytes) */
                    collectionFlags =
                            (byte) (type_msb_byte.byteValue() & MASK_COLLECTION_FROM_TYPE);
                    type_lsb_byte = (short) (fetchedType & 0x00ff);
                    originalType = type_lsb_byte.byteValue();
                } else {
                    collectionFlags = (byte) (fetchedType.byteValue() & MASK_COLLECTION_FROM_TYPE);
                    originalType = (byte) (fetchedType.byteValue() & MASK_TYPE_FROM_SINGLE_BYTE);
                }

                realType = UColumnInfo.confirmType(originalType, collectionFlags);
                tuples[index].setAttribute(1, new Short((short) realType[0]));
        }
    }

    private Object readAAttribute(int index, UInputBuffer inBuffer) throws UJciException {
        int size;
        int localType;
        byte typeInfo[];
        String charsetName;

        size = inBuffer.readInt();
        if (size < 0) return null;

        typeInfo = readTypeFromData(index, inBuffer);
        localType = typeInfo[0];
        if (localType == UUType.U_TYPE_NULL) {
            localType = columnInfo[index].getColumnType();
            charsetName = columnInfo[index].getColumnCharset();
        } else {
            if (typeInfo[3] == DEFAULT_CHARSET) {
                charsetName = null;
            } else {
                charsetName = UJCIUtil.getJavaCharsetName((byte) typeInfo[3]);
            }
        }

        size = size - typeInfo[2];
        fetchedSize += size;

        return (readData(inBuffer, localType, size, charsetName));
    }

    private Object readData(UInputBuffer inBuffer, int dataType, int dataSize, String charsetName)
            throws UJciException {
        switch (dataType) {
            case UUType.U_TYPE_CHAR:
            case UUType.U_TYPE_NCHAR:
            case UUType.U_TYPE_STRING:
            case UUType.U_TYPE_VARNCHAR:
            case UUType.U_TYPE_ENUM:
            case UUType.U_TYPE_JSON:
                if (charsetName != null && charsetName.equals("BINARY")) {
                    return inBuffer.readBinaryString(dataSize);
                } else {
                    return inBuffer.readString(dataSize, charsetName);
                }
            case UUType.U_TYPE_NUMERIC:
                return new BigDecimal(inBuffer.readString(dataSize, UJCIManager.sysCharsetName));
            case UUType.U_TYPE_BIGINT:
            case UUType.U_TYPE_UBIGINT:
                return new Long(inBuffer.readLong());
            case UUType.U_TYPE_INT:
            case UUType.U_TYPE_UINT:
                return new Integer(inBuffer.readInt());
            case UUType.U_TYPE_SHORT:
            case UUType.U_TYPE_USHORT:
                return new Short(inBuffer.readShort());
            case UUType.U_TYPE_DATE:
                return inBuffer.readDate();
            case UUType.U_TYPE_TIME:
                return inBuffer.readTime();
            case UUType.U_TYPE_TIMESTAMP:
                return inBuffer.readTimestamp(false);
            case UUType.U_TYPE_TIMESTAMPTZ:
            case UUType.U_TYPE_TIMESTAMPLTZ:
                return inBuffer.readTimestamptz(dataSize);
            case UUType.U_TYPE_DATETIME:
                return inBuffer.readDatetime(false);
            case UUType.U_TYPE_DATETIMETZ:
            case UUType.U_TYPE_DATETIMELTZ:
                return inBuffer.readDatetimetz(dataSize);
            case UUType.U_TYPE_OBJECT:
                return inBuffer.readOID(relatedConnection.getCUBRIDConnection());
            case UUType.U_TYPE_SET:
            case UUType.U_TYPE_MULTISET:
            case UUType.U_TYPE_SEQUENCE:
                {
                    TURBOGRAPHArray aArray;
                    aArray = new TURBOGRAPHArray(inBuffer.readByte(), inBuffer.readInt());
                    int baseType = aArray.getBaseType();
                    for (int i = 0; i < aArray.getLength(); i++) {
                        int eleSize = inBuffer.readInt();
                        if (eleSize <= 0) aArray.setElement(i, null);
                        else
                            aArray.setElement(
                                    i, readData(inBuffer, baseType, eleSize, charsetName));
                    }
                    return aArray;
                }
            case UUType.U_TYPE_MONETARY:
            case UUType.U_TYPE_DOUBLE:
                return new Double(inBuffer.readDouble());
            case UUType.U_TYPE_FLOAT:
                return new Float(inBuffer.readFloat());
            case UUType.U_TYPE_BIT:
            case UUType.U_TYPE_VARBIT:
                return inBuffer.readBytes(dataSize);
            case UUType.U_TYPE_RESULTSET:
                if (UConnection.protoVersionIsLower(UConnection.PROTOCOL_V11)) {
                    return new TURBOGRAPHOutResultSet(relatedConnection, (long) inBuffer.readInt());
                } else {
                    return new TURBOGRAPHOutResultSet(relatedConnection, inBuffer.readLong());
                }
            case UUType.U_TYPE_BLOB:
                return inBuffer.readBlob(dataSize, relatedConnection.cubridcon);
            case UUType.U_TYPE_CLOB:
                return inBuffer.readClob(dataSize, relatedConnection.cubridcon);
            case UUType.U_TYPE_NULL:
                return null;
            case UUType.U_TYPE_HUGEINT:
                return inBuffer.readString(dataSize, charsetName);
            default:
                throw new UJciException(UErrorCode.ER_TYPE_CONVERSION);
        }
    }

    private void read_fetch_data(UInputBuffer inBuffer, UFunctionCode functionCode)
            throws UJciException {
        fetchedTupleNumber = inBuffer.readInt();
        if (fetchedTupleNumber < 0) {
            fetchedTupleNumber = 0;
        }

        tuples = new UResultTuple[fetchedTupleNumber];

        for (int i = 0; i < fetchedTupleNumber; i++) {
            readATuple(i, inBuffer);
        }

        if (functionCode == UFunctionCode.GET_GENERATED_KEYS) {
            isFetchCompleted = true;
        }

        if (functionCode == UFunctionCode.FETCH
                && relatedConnection.protoVersionIsAbove(UConnection.PROTOCOL_V5)) {
            isFetchCompleted = inBuffer.readByte() == 1 ? true : false;
        }
    }

    private void readATupleByOid(TURBOGRAPHOID oid, UInputBuffer inBuffer) throws UJciException {
        tuples[0] = new UResultTuple(1, columnNumber);
        tuples[0].setOid(oid);
        for (int i = 0; i < columnNumber; i++) {
            tuples[0].setAttribute(i, readAAttribute(i, inBuffer));
        }
        currentFirstCursor = 0;
    }

    private void readATuple(int index, UInputBuffer inBuffer) throws UJciException {

        UResultTuple tuple = new UResultTuple(inBuffer.readInt(), columnNumber);
        tuples[index] = tuple;
        tuples[index].setOid(inBuffer.readOID(relatedConnection.getCUBRIDConnection()));
        for (int i = fetchedSize = 0; i < columnNumber; i++) {
            tuples[index].setAttribute(i, readAAttribute(i, inBuffer));
        }

        confirmSchemaTypeInfo(index);

        if (index == 0) currentFirstCursor = tuple.tupleNumber() - 1;
    }

    private void readColumnInfo(UInputBuffer inBuffer) throws UJciException {
        byte type, collectionByte = 0;
        short scale;
        int precision;
        byte charset;
        String name;
        String charsetName;

        columnInfo = new UColumnInfo[columnNumber];
        colNameToIndex = new HashMap<String, Integer>(columnNumber);

        for (int i = 0; i < columnNumber; i++) {
            collectionByte = inBuffer.readByte();
            if ((byte) (collectionByte & MASK_TYPE_HAS_2_BYTES) == 0) {
                /* legacy server : type has only one byte */
                type = (byte) (collectionByte & MASK_TYPE_FROM_SINGLE_BYTE);
                collectionByte = (byte) (collectionByte & MASK_COLLECTION_FROM_TYPE);
                charsetName = relatedConnection.getCharset();
            } else {
                charset = (byte) (collectionByte & MASK_CHARSET_FROM_TYPE);
                charsetName = UJCIUtil.getJavaCharsetName((byte) charset);
                collectionByte = (byte) (collectionByte & MASK_COLLECTION_FROM_TYPE);
                type = inBuffer.readByte();
            }

            scale = inBuffer.readShort();
            precision = inBuffer.readInt();
            name = inBuffer.readString(inBuffer.readInt(), relatedConnection.getCharset());
            columnInfo[i] =
                    new UColumnInfo(type, scale, precision, name, collectionByte, charsetName);
            name = name.toLowerCase();
            if (statementType == NORMAL) {
                /*
                 * read extra data here (according to broker cas_execute
                 * prepare_column_info_set order)
                 */

                String attributeName =
                        inBuffer.readString(inBuffer.readInt(), relatedConnection.getCharset());
                String className =
                        inBuffer.readString(inBuffer.readInt(), relatedConnection.getCharset());
                byte byteData = inBuffer.readByte();
                columnInfo[i].setRemainedData(
                        attributeName, className, ((byteData == (byte) 0) ? true : false));

                String defValue =
                        inBuffer.readString(inBuffer.readInt(), relatedConnection.getCharset());
                byte bAI = inBuffer.readByte();
                byte bUK = inBuffer.readByte();
                byte bPK = inBuffer.readByte();
                byte bRI = inBuffer.readByte();
                byte bRU = inBuffer.readByte();
                byte bFK = inBuffer.readByte();
                byte bSh = inBuffer.readByte();

                columnInfo[i].setExtraData(defValue, bAI, bUK, bPK, bFK, bRI, bRU, bSh);
            }
            if (colNameToIndex.containsKey(name) == false) {
                colNameToIndex.put(name, i);
            }
        }
    }

    private void readResultInfo(UInputBuffer inBuffer) throws UJciException {
        numQueriesExecuted = inBuffer.readInt();
        resultInfo = new UResultInfo[numQueriesExecuted];
        for (int i = 0; i < resultInfo.length; i++) {
            resultInfo[i] = new UResultInfo(inBuffer.readByte(), inBuffer.readInt());
            resultInfo[i].setResultOid(inBuffer.readOID(relatedConnection.getCUBRIDConnection()));
            resultInfo[i].setSrvCacheTime(inBuffer.readInt(), inBuffer.readInt());
        }
    }

    public int getNumQueriesExecuted() {
        return numQueriesExecuted;
    }

    public int getServerHandle() {
        return serverHandler;
    }

    /*
     * 3.0 private UParameterInfo[] readParameterInfo() throws UJciException {
     * byte mode; byte type; short scale; int precision;
     *
     * UParameterInfo localParameterInfo[] = new UParameterInfo[pramNumber]; for
     * (int i=0; i < pramNumber ; i++) { mode = inBuffer.readByte(); type =
     * inBuffer.readByte(); scale = inBuffer.readShort(); precision =
     * inBuffer.readInt(); localParameterInfo[i] = new UParameterInfo(mode,
     * type, scale, precision); } return localParameterInfo; }
     */

    /*
     * public UStatementCacheData makeCacheData() { if (fetchedTupleNumber <
     * totalTupleNumber) return null;
     *
     * if (resultInfo.length > 1) { result_cacheable = false; return null; }
     *
     * return (new UStatementCacheData(totalTupleNumber, tuples, resultInfo)); }
     */

    public void getCacheData(UStatementCacheData cache_data) {
        totalTupleNumber = cache_data.tuple_count;
        resultInfo = cache_data.resultInfo;
        currentFirstCursor = cache_data.getFirstCursor(0);
        cursorPosition = 0;
        fetchedTupleNumber = cache_data.getFetchNumber(0);
        executeResult = totalTupleNumber;
        realFetched = true;
    }

    public boolean is_result_cacheable() {
        if (result_cacheable == true && relatedConnection.update_executed == false) return true;
        else return false;
    }

    public void setAutoGeneratedKeys(boolean isGeneratedKeys) {
        this.isGeneratedKeys = isGeneratedKeys;
    }

    protected void finalize() {
        if (stmt_cache != null) stmt_cache.decr_ref_count();
    }

    private void flushLobStreams() {
        if (bindParameter != null) {
            synchronized (bindParameter) {
                bindParameter.flushLobStreams();
            }
        }
    }

    public String getQuery() {
        return sql_stmt;
    }

    public UBindParameter getBindParameter() {
        return bindParameter;
    }

    public boolean hasBatch() {
        return batchParameter != null && batchParameter.size() != 0;
    }

    public boolean isFetchCompleted(int current_row) {
        return relatedConnection.isConnectedToOracle()
                && isFetchCompleted
                && current_row >= currentFirstCursor + fetchedTupleNumber;
    }
}
