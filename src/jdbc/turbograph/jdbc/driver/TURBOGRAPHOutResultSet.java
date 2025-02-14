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

import java.sql.SQLException;

import turbograph.jdbc.jci.UConnection;
import turbograph.jdbc.jci.UStatement;

public class TURBOGRAPHOutResultSet extends TURBOGRAPHResultSet {
    private boolean created;

    /*
     * if under PROTOCOL_V11) SRV_HANDLE id
     * else) QUERY_ID
     */
    private long resultId;

    private UConnection ucon;

    public TURBOGRAPHOutResultSet(UConnection ucon, long id) {
        super(null);
        created = false;
        this.resultId = id;
        this.ucon = ucon;
        ucon.getCUBRIDConnection().addOutResultSet(this);
    }

    public void createInstance() throws Exception {
        if (created) return;
        if (resultId <= 0) {
            throw new IllegalArgumentException();
        }

        u_stmt = new UStatement(ucon, resultId);
        column_info = u_stmt.getColumnInfo();
        col_name_to_index = u_stmt.getColumnNameToIndexMap();
        number_of_rows = u_stmt.getExecuteResult();

        created = true;
    }

    @Override
    public void close() throws SQLException {
        if (is_closed) {
            return;
        }
        is_closed = true;

        clearCurrentRow();

        u_stmt.close();

        streams = null;
        u_stmt = null;
        column_info = null;
        error = null;
    }
}
