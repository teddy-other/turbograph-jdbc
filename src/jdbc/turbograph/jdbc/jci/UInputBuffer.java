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

import turbograph.jdbc.driver.TURBOGRAPHBinaryString;
import turbograph.jdbc.driver.TURBOGRAPHBlob;
import turbograph.jdbc.driver.TURBOGRAPHClob;
import turbograph.jdbc.driver.TURBOGRAPHConnection;
import turbograph.jdbc.driver.TURBOGRAPHXid;
import turbograph.sql.TURBOGRAPHOID;
import turbograph.sql.TURBOGRAPHOIDImpl;
import turbograph.sql.TURBOGRAPHTimestamp;
import turbograph.sql.TURBOGRAPHTimestamptz;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.util.Calendar;
import java.util.TimeZone;

class UInputBuffer {
    private UTimedDataInputStream input;
    private int position;
    private int capacity;
    private byte casinfo[];
    private byte buffer[];
    private int resCode;
    private static final int CAS_INFO_SIZE = 4;
    private UConnection uconn;

    UInputBuffer(UTimedDataInputStream relatedI, UConnection con)
            throws IOException, UJciException {
        input = relatedI;
        position = 0;
        uconn = con;

        int readLen = 0;
        int totalReadLen = 0;
        byte[] headerData = new byte[8];

        while (totalReadLen < 8) {
            readLen = input.read(headerData, totalReadLen, 8 - totalReadLen, 0);
            if (readLen == -1) {
                throw uconn.createJciException(UErrorCode.ER_ILLEGAL_DATA_SIZE);
            }
            totalReadLen = totalReadLen + readLen;
        }

        capacity = UJCIUtil.bytes2int(headerData, 0);
        casinfo = new byte[CAS_INFO_SIZE];
        System.arraycopy(headerData, 4, casinfo, 0, 4);
        con.setCASInfo(casinfo);

        if (capacity <= 0) {
            resCode = 0;
            capacity = 0;
            return;
        }

        buffer = new byte[capacity];
        readData();

        resCode = readInt();

        if (resCode < 0) {
            int eCode = readInt();
            String msg;

            if (con.isRenewedSessionId()) {
                byte[] newSessionId = new byte[20];

                msg =
                        readString(
                                remainedCapacity() - newSessionId.length,
                                UJCIManager.sysCharsetName);
                readBytes(newSessionId);
                con.setNewSessionId(newSessionId);
            } else {
                msg = readString(remainedCapacity(), UJCIManager.sysCharsetName);
            }

            eCode = convertErrorByVersion(resCode, eCode);
            throw uconn.createJciException(UErrorCode.ER_DBMS, resCode, eCode, msg);
        }
    }

    UInputBuffer(UTimedDataInputStream relatedI, UConnection con, int timeout)
            throws IOException, UJciException {
        input = relatedI;
        position = 0;
        uconn = con;

        int readLen = 0;
        int totalReadLen = 0;
        byte[] headerData = new byte[8];

        while (totalReadLen < 8) {
            readLen = input.read(headerData, totalReadLen, 8 - totalReadLen, timeout);
            if (readLen == -1) {
                throw uconn.createJciException(UErrorCode.ER_ILLEGAL_DATA_SIZE);
            }
            totalReadLen = totalReadLen + readLen;
        }

        capacity = UJCIUtil.bytes2int(headerData, 0);
        casinfo = new byte[CAS_INFO_SIZE];
        System.arraycopy(headerData, 4, casinfo, 0, 4);
        con.setCASInfo(casinfo);

        if (capacity <= 0) {
            resCode = 0;
            capacity = 0;
            return;
        }

        buffer = new byte[capacity];
        readData();

        resCode = readInt();

        if (resCode < 0) {
            int eCode = readInt();
            String msg;

            if (con.isRenewedSessionId()) {
                byte[] newSessionId = new byte[20];

                msg =
                        readString(
                                remainedCapacity() - newSessionId.length,
                                UJCIManager.sysCharsetName);
                readBytes(newSessionId);
                con.setNewSessionId(newSessionId);
            } else {
                msg = readString(remainedCapacity(), UJCIManager.sysCharsetName);
            }

            eCode = convertErrorByVersion(resCode, eCode);
            throw uconn.createJciException(UErrorCode.ER_DBMS, resCode, eCode, msg);
        }
    }

    int convertErrorByVersion(int indicator, int error) {
        if (!uconn.protoVersionIsSame(UConnection.PROTOCOL_V2)
                && !uconn.brokerInfoRenewedErrorCode()) {
            if (indicator == UErrorCode.CAS_ERROR_INDICATOR
                    || error == UErrorCode.CAS_ER_NOT_AUTHORIZED_CLIENT) {
                // old error converts to new error
                return error - 9000;
            }
        }

        return error;
    }

    byte[] getCasInfo() {
        return casinfo;
    }

    int getResCode() {
        return resCode;
    }

    byte readByte() throws UJciException {
        if (position >= capacity) {
            throw uconn.createJciException(UErrorCode.ER_ILLEGAL_DATA_SIZE);
        }

        return buffer[position++];
    }

    void readBytes(byte value[], int offset, int len) throws UJciException {
        if (value == null) return;

        if (position + len > capacity) {
            throw uconn.createJciException(UErrorCode.ER_ILLEGAL_DATA_SIZE);
        }

        System.arraycopy(buffer, position, value, offset, len);
        position += len;
    }

    void readBytes(byte value[]) throws UJciException {
        readBytes(value, 0, value.length);
    }

    byte[] readBytes(int size) throws UJciException {
        byte[] value = new byte[size];
        readBytes(value, 0, size);
        return value;
    }

    double readDouble() throws UJciException {
        return Double.longBitsToDouble(readLong());
    }

    float readFloat() throws UJciException {
        return Float.intBitsToFloat(readInt());
    }

    int readInt() throws UJciException {
        if (position + 4 > capacity) {
            throw uconn.createJciException(UErrorCode.ER_ILLEGAL_DATA_SIZE);
        }

        int data = UJCIUtil.bytes2int(buffer, position);
        position += 4;

        return data;
    }

    long readLong() throws UJciException {
        long data = 0;

        if (position + 8 > capacity) {
            throw uconn.createJciException(UErrorCode.ER_ILLEGAL_DATA_SIZE);
        }

        for (int i = 0; i < 8; i++) {
            data <<= 8;
            data |= (buffer[position++] & 0xff);
        }

        return data;
    }

    short readShort() throws UJciException {
        if (position + 2 > capacity) {
            throw uconn.createJciException(UErrorCode.ER_ILLEGAL_DATA_SIZE);
        }

        short data = UJCIUtil.bytes2short(buffer, position);
        position += 2;

        return data;
    }

    String readString(int size, String charsetName) throws UJciException {
        String stringData;

        if (size <= 0) return null;

        if (position + size > capacity) {
            throw uconn.createJciException(UErrorCode.ER_ILLEGAL_DATA_SIZE);
        }

        if (charsetName != null) {
            try {
                stringData = new String(buffer, position, size - 1, charsetName);
            } catch (java.io.UnsupportedEncodingException e) {
                stringData = new String(buffer, position, size - 1);
            }
        } else {
            stringData = new String(buffer, position, size - 1);
        }

        position += size;

        return stringData;
    }

    TURBOGRAPHBinaryString readBinaryString(int size) throws UJciException {
        byte[] byteArray;

        if (size <= 0) return null;

        if (position + size > capacity) {
            throw uconn.createJciException(UErrorCode.ER_ILLEGAL_DATA_SIZE);
        }

        byteArray = java.util.Arrays.copyOfRange(buffer, position, position + size - 1);

        TURBOGRAPHBinaryString stringData = new TURBOGRAPHBinaryString(byteArray);

        position += size;

        return stringData;
    }

    Date readDate() throws UJciException {
        int year, month, day;
        year = readShort();
        month = readShort();
        day = readShort();

        if (year == 0 && month == 0 && day == 0) {
            if (uconn.getZeroDateTimeBehavior()
                    .equals(UConnection.ZERO_DATETIME_BEHAVIOR_EXCEPTION)) {
                throw new UJciException(UErrorCode.ER_ILLEGAL_TIMESTAMP);
            } else if (uconn.getZeroDateTimeBehavior()
                    .equals(UConnection.ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL)) {
                return null;
            }
        }

        Calendar cal = Calendar.getInstance();
        if (year == 0 && month == 0 && day == 0) {
            cal.set(0, 0, 1, 0, 0, 0); /* round to 0001-01-01 00:00:00) */
        } else {
            cal.set(year, month - 1, day, 0, 0, 0);
        }
        cal.set(Calendar.MILLISECOND, 0);

        return new Date(cal.getTimeInMillis());
    }

    Time readTime() throws UJciException {
        int hour, minute, second;
        hour = readShort();
        minute = readShort();
        second = readShort();

        Calendar cal = Calendar.getInstance();
        cal.set(1970, 0, 1, hour, minute, second);
        cal.set(Calendar.MILLISECOND, 0);

        return new Time(cal.getTimeInMillis());
    }

    TURBOGRAPHTimestamp readTimestamp(boolean is_tz) throws UJciException {
        int year, month, day, hour, minute, second;
        year = readShort();
        month = readShort();
        day = readShort();
        hour = readShort();
        minute = readShort();
        second = readShort();

        if (year == 0 && month == 0 && day == 0) {
            if (uconn.getZeroDateTimeBehavior()
                    .equals(UConnection.ZERO_DATETIME_BEHAVIOR_EXCEPTION)) {
                throw new UJciException(UErrorCode.ER_ILLEGAL_TIMESTAMP);
            } else if (uconn.getZeroDateTimeBehavior()
                    .equals(UConnection.ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL)) {
                return null;
            }
        }

        Calendar cal = Calendar.getInstance();
        if (is_tz) {
            cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        }
        if (year == 0 && month == 0 && day == 0) {
            cal.setTimeInMillis(0); /* round to 1970-01-01 00:00:00 UTC */
        } else {
            cal.set(year, month - 1, day, hour, minute, second);
        }
        cal.set(Calendar.MILLISECOND, 0);

        return new TURBOGRAPHTimestamp(cal.getTimeInMillis(), TURBOGRAPHTimestamp.TIMESTAMP);
    }

    TURBOGRAPHTimestamptz readTimestamptz(int size) throws UJciException {
        TURBOGRAPHTimestamp cubrid_ts;
        String timezone;
        int tmp_position;
        int ts_size;

        if (position + size > capacity) {
            throw uconn.createJciException(UErrorCode.ER_ILLEGAL_DATA_SIZE);
        }

        tmp_position = position;
        cubrid_ts = readTimestamp(true);

        ts_size = position - tmp_position;

        if (ts_size > 0) {
            timezone = new String(buffer, position, size - ts_size - 1);
            position += size - ts_size;
        } else {
            timezone = "";
            position++;
        }

        return TURBOGRAPHTimestamptz.valueOf(cubrid_ts, timezone);
    }

    TURBOGRAPHTimestamp readDatetime(boolean is_tz) throws UJciException {
        int year, month, day, hour, minute, second, millisecond;
        year = readShort();
        month = readShort();
        day = readShort();
        hour = readShort();
        minute = readShort();
        second = readShort();
        millisecond = readShort();

        if (year == 0 && month == 0 && day == 0) {
            if (uconn.getZeroDateTimeBehavior()
                    .equals(UConnection.ZERO_DATETIME_BEHAVIOR_EXCEPTION)) {
                throw new UJciException(UErrorCode.ER_ILLEGAL_TIMESTAMP);
            } else if (uconn.getZeroDateTimeBehavior()
                    .equals(UConnection.ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL)) {
                return null;
            }
        }

        Calendar cal = Calendar.getInstance();
        if (is_tz) {
            cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        }
        if (year == 0 && month == 0 && day == 0) {
            cal.set(0, 0, 1, 0, 0, 0); /* round to 0001-01-01 00:00:00) */
        } else {
            cal.set(year, month - 1, day, hour, minute, second);
        }
        cal.set(Calendar.MILLISECOND, millisecond);

        return new TURBOGRAPHTimestamp(cal.getTimeInMillis(), TURBOGRAPHTimestamp.DATETIME);
    }

    TURBOGRAPHTimestamptz readDatetimetz(int size) throws UJciException {
        TURBOGRAPHTimestamp cubrid_ts;
        String timezone;
        int tmp_position;
        int ts_size;

        if (position + size > capacity) {
            throw uconn.createJciException(UErrorCode.ER_ILLEGAL_DATA_SIZE);
        }

        tmp_position = position;
        cubrid_ts = readDatetime(true);

        if (cubrid_ts == null) {
            /* zero datetime NULL behavior */
            return null;
        }

        ts_size = position - tmp_position;

        if (ts_size > 0) {
            timezone = new String(buffer, position, size - ts_size - 1);
            position += size - ts_size;
        } else {
            timezone = "";
            position++;
        }

        return TURBOGRAPHTimestamptz.valueOf(cubrid_ts, timezone);
    }

    TURBOGRAPHOID readOID(TURBOGRAPHConnection con) throws UJciException {
        byte[] oid = readBytes(UConnection.OID_BYTE_SIZE);
        for (int i = 0; i < oid.length; i++) {
            if (oid[i] != (byte) 0) {
                return (new TURBOGRAPHOIDImpl(con, oid));
            }
        }
        return null;
    }

    TURBOGRAPHBlob readBlob(int packedLobHandleSize, TURBOGRAPHConnection conn) throws UJciException {
        try {
            byte[] packedLobHandle = readBytes(packedLobHandleSize);
            return new TURBOGRAPHBlob(conn, packedLobHandle, true);
        } catch (Exception e) {
            throw uconn.createJciException(UErrorCode.ER_UNKNOWN);
        }
    }

    TURBOGRAPHClob readClob(int packedLobHandleSize, TURBOGRAPHConnection conn) throws UJciException {
        try {
            byte[] packedLobHandle = readBytes(packedLobHandleSize);
            return new TURBOGRAPHClob(conn, packedLobHandle, conn.getUConnection().getCharset(), true);
        } catch (Exception e) {
            throw uconn.createJciException(UErrorCode.ER_UNKNOWN);
        }
    }

    int remainedCapacity() {
        return capacity - position;
    }

    TURBOGRAPHXid readXid() throws UJciException {
        readInt(); // msg_size
        int formatId = readInt();
        int gid_size = readInt();
        int bid_size = readInt();
        byte[] gid = readBytes(gid_size);
        byte[] bid = readBytes(bid_size);

        return (new TURBOGRAPHXid(formatId, gid, bid));
    }

    private void readData() throws IOException, UJciException {
        int realRead = 0, tempRead = 0;
        while (realRead < capacity) {
            tempRead = input.read(buffer, realRead, capacity - realRead);
            if (tempRead < 0) {
                capacity = realRead;
                break;
            }
            realRead += tempRead;
        }
    }
}
