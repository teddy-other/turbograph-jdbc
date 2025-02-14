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

import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;

import turbograph.jdbc.jci.UUType;

public class TURBOGRAPHBlob implements Blob {
    /*
     * ======================================================================= |
     * CONSTANT VALUES
     * =======================================================================
     */
    private static final int BLOB_MAX_IO_LENGTH = 128 * 1024; // 128KB at once

    /*
     * ======================================================================= |
     * PRIVATE
     * =======================================================================
     */
    private TURBOGRAPHConnection conn;
    private boolean isWritable;
    private boolean isLobLocator;
    private TURBOGRAPHLobHandle lobHandle;

    private ArrayList<java.io.Flushable> streamList = new ArrayList<java.io.Flushable>();

    /*
     * ======================================================================= |
     * CONSTRUCTOR
     * =======================================================================
     */
    // make a new blob
    public TURBOGRAPHBlob(TURBOGRAPHConnection conn) throws SQLException {
        if (conn == null) {
            throw new TURBOGRAPHException(TURBOGRAPHJDBCErrorCode.invalid_value);
        }

        byte[] packedLobHandle = conn.lobNew(UUType.U_TYPE_BLOB);

        this.conn = conn;
        isWritable = true;
        isLobLocator = true;
        lobHandle = new TURBOGRAPHLobHandle(UUType.U_TYPE_BLOB, packedLobHandle, isLobLocator);
    }

    // get blob from existing result set
    public TURBOGRAPHBlob(TURBOGRAPHConnection conn, byte[] packedLobHandle, boolean isLobLocator)
            throws SQLException {
        if (conn == null || packedLobHandle == null) {
            throw new TURBOGRAPHException(TURBOGRAPHJDBCErrorCode.invalid_value);
        }

        this.conn = conn;
        isWritable = false;
        this.isLobLocator = isLobLocator;
        lobHandle = new TURBOGRAPHLobHandle(UUType.U_TYPE_BLOB, packedLobHandle, isLobLocator);
    }

    /*
     * ======================================================================= |
     * java.sql.Blob interface
     * =======================================================================
     */
    public long length() throws SQLException {
        if (lobHandle == null) {
            throw conn.createTURBOGRAPHException(TURBOGRAPHJDBCErrorCode.invalid_value, null);
        }
        return lobHandle.getLobSize();
    }

    public byte[] getBytes(long pos, int length) throws SQLException {
        if (lobHandle == null) {
            throw conn.createTURBOGRAPHException(TURBOGRAPHJDBCErrorCode.invalid_value, null);
        }
        if (pos < 1 || length < 0) {
            throw conn.createTURBOGRAPHException(TURBOGRAPHJDBCErrorCode.invalid_value, null);
        }
        if (length == 0) {
            return new byte[0];
        }

        pos--; // pos is now offset from 0
        int real_read_len, read_len, total_read_len = 0;

        if (pos + length > length()) {
            length = (int) (length() - pos);
        }

        if (length <= 0) {
            return new byte[0];
        }

        byte[] buf = new byte[length];

        if (isLobLocator) {
            while (length > 0) {
                read_len = Math.min(length, BLOB_MAX_IO_LENGTH);
                real_read_len =
                        conn.lobRead(
                                lobHandle.getPackedLobHandle(), pos, buf, total_read_len, read_len);

                pos += real_read_len;
                length -= real_read_len;
                total_read_len += real_read_len;

                if (real_read_len == 0) {
                    break;
                }
            }
        } else {
            System.arraycopy(lobHandle.getPackedLobHandle(), (int) pos, buf, 0, length);
            total_read_len = length;
        }

        if (total_read_len < buf.length) {
            // In common case, this code cannot be executed
            throw conn.createTURBOGRAPHException(TURBOGRAPHJDBCErrorCode.unknown, null);
            // byte[]new_buf = new byte[total_read_len];
            // System.arraycopy (buf, 0, new_buf, 0, total_read_len);
            // return new_buf;
        } else {
            return buf;
        }
    }

    public InputStream getBinaryStream() throws SQLException {
        return getBinaryStream(1, length());
    }

    /* JDK 1.6 */
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        if (lobHandle == null) {
            throw conn.createTURBOGRAPHException(TURBOGRAPHJDBCErrorCode.invalid_value, null);
        }
        if (pos < 1 || length < 0) {
            throw conn.createTURBOGRAPHException(TURBOGRAPHJDBCErrorCode.invalid_value, null);
        }

        return new TURBOGRAPHBufferedInputStream(
                new TURBOGRAPHBlobInputStream(this, pos, length), BLOB_MAX_IO_LENGTH);
    }

    public long position(byte[] pattern, long start) throws SQLException {
        throw new SQLException(new java.lang.UnsupportedOperationException());
    }

    public long position(Blob pattern, long start) throws SQLException {
        throw new SQLException(new java.lang.UnsupportedOperationException());
    }

    public int setBytes(long pos, byte[] bytes) throws SQLException {
        return (setBytes(pos, bytes, 0, bytes.length));
    }

    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        if (lobHandle == null) {
            throw conn.createTURBOGRAPHException(TURBOGRAPHJDBCErrorCode.invalid_value, null);
        }
        if (pos < 1 || offset < 0 || len < 0) {
            throw conn.createTURBOGRAPHException(TURBOGRAPHJDBCErrorCode.invalid_value, null);
        }
        if (offset + len > bytes.length) {
            throw new IndexOutOfBoundsException();
        }

        if (isWritable) {
            if (length() + 1 != pos) {
                throw conn.createTURBOGRAPHException(TURBOGRAPHJDBCErrorCode.lob_pos_invalid, null);
            }

            pos--; // pos is now offset from 0

            int real_write_len, write_len, total_write_len = 0;

            while (len > 0) {
                write_len = Math.min(len, BLOB_MAX_IO_LENGTH);
                real_write_len =
                        conn.lobWrite(
                                lobHandle.getPackedLobHandle(), pos, bytes, offset, write_len);

                pos += real_write_len;
                len -= real_write_len;
                offset += real_write_len;
                total_write_len += real_write_len;
            }

            if (pos > length()) {
                lobHandle.setLobSize(pos);
            }

            return total_write_len;
        } else {
            throw conn.createTURBOGRAPHException(TURBOGRAPHJDBCErrorCode.lob_is_not_writable, null);
        }
    }

    public OutputStream setBinaryStream(long pos) throws SQLException {
        if (lobHandle == null) {
            throw conn.createTURBOGRAPHException(TURBOGRAPHJDBCErrorCode.invalid_value, null);
        }
        if (pos < 1) {
            throw conn.createTURBOGRAPHException(TURBOGRAPHJDBCErrorCode.invalid_value, null);
        }

        if (isWritable) {
            if (length() + 1 != pos) {
                throw conn.createTURBOGRAPHException(TURBOGRAPHJDBCErrorCode.lob_pos_invalid, null);
            }

            OutputStream out =
                    new TURBOGRAPHBufferedOutputStream(
                            new TURBOGRAPHBlobOutputStream(this, pos), BLOB_MAX_IO_LENGTH);
            addFlushableStream(out);
            return out;
        } else {
            throw conn.createTURBOGRAPHException(TURBOGRAPHJDBCErrorCode.lob_is_not_writable, null);
        }
    }

    public void truncate(long len) throws SQLException {
        throw new SQLException(new java.lang.UnsupportedOperationException());
    }

    /* JDK 1.6 */
    public void free() throws SQLException {
        conn = null;
        lobHandle = null;
        streamList = null;
        isWritable = false;
        isLobLocator = true;
    }

    public TURBOGRAPHLobHandle getLobHandle() {
        return lobHandle;
    }

    private void addFlushableStream(Flushable out) {
        streamList.add(out);
    }

    public void removeFlushableStream(Flushable out) {
        streamList.remove(out);
    }

    public void flushFlushableStreams() {
        if (!streamList.isEmpty()) {
            for (Flushable out : streamList) {
                try {
                    out.flush();
                } catch (IOException e) {
                }
            }
        }
    }

    public String toString() throws RuntimeException {
        if (isLobLocator == true) {
            return lobHandle.toString();
        } else {
            throw new RuntimeException(
                    "The lob locator does not exist because the column type has changed.");
        }
    }

    public boolean equals(Object obj) {
        if (obj instanceof TURBOGRAPHBlob) {
            TURBOGRAPHBlob that = (TURBOGRAPHBlob) obj;
            return lobHandle.equals(that.lobHandle);
        }
        return false;
    }
}
