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

import turbograph.sql.TURBOGRAPHOID;

public class UResultInfo {
    byte statementType;
    private int resultCount;
    private boolean isResultSet; /* if result is resultset, true. otherwise false */
    private TURBOGRAPHOID oid;
    private long srv_cache_time;

    UResultInfo(byte type, int count) {
        statementType = type;
        resultCount = count;
        if (statementType == TURBOGRAPHCommandType.CUBRID_STMT_SELECT
                || statementType == TURBOGRAPHCommandType.CUBRID_STMT_CALL
                || statementType == TURBOGRAPHCommandType.CUBRID_STMT_GET_STATS
                || statementType == TURBOGRAPHCommandType.CUBRID_STMT_EVALUATE) {
            isResultSet = true;
        } else {
            isResultSet = false;
        }
        oid = null;
        srv_cache_time = 0L;
    }

    public int getResultCount() {
        return resultCount;
    }

    public boolean isResultSet() {
        return isResultSet;
    }

    TURBOGRAPHOID getCUBRIDOID() {
        return oid;
    }

    void setResultOid(TURBOGRAPHOID o) {
        if (statementType == TURBOGRAPHCommandType.CUBRID_STMT_INSERT && resultCount == 1) oid = o;
    }

    void setSrvCacheTime(int sec, int usec) {
        srv_cache_time = sec;
        srv_cache_time = (srv_cache_time << 32) | usec;
    }

    long getSrvCacheTime() {
        return srv_cache_time;
    }
}
