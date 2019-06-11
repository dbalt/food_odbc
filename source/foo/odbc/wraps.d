module foo.odbc.wraps;

import std.stdio;
import std.string;
import std.exception;
import std.traits;

import core.sys.windows.sql;
import core.sys.windows.sqlext;
import core.sys.windows.sqltypes;

import std.datetime;

pragma(lib, "odbc32");

import foo.odbc.errs;

import core.thread;


enum logging = true;
void log(lazy string s){
    static if(logging) writeln(s);
}


/*  ==============================================
    shortcuts for return codes
=============================================   */
bool strict_ok(SQLRETURN rc) {
    return rc == SQL_SUCCESS;
}

bool ok_with_info(SQLRETURN rc) {
    return rc == SQL_SUCCESS_WITH_INFO;
}

bool ok(SQLRETURN rc) {
    return rc.strict_ok || rc.ok_with_info;		
}

bool error(SQLRETURN rc) {
    return rc == SQL_ERROR;
}

bool still_exec(SQLRETURN rc){
    return rc == SQL_STILL_EXECUTING;
}

/*  Diagnostic records from ODBC driver  */
ErrInfo[] odbc_get_diag_rec(short handleType, SQLHANDLE handle)
{
    SQLLEN recordsCount;
    short len_ind;
    SQLGetDiagField(handleType, handle, cast(short) 0, cast(short) SQL_DIAG_NUMBER, cast(void*) &recordsCount, cast(short) 0,  &len_ind);

    SQLCHAR[6] sqlStateBuffer;
    SQLCHAR[SQL_MAX_MESSAGE_LENGTH] messageBuffer;
    SQLINTEGER nativeError = 0;
    SQLSMALLINT messageLength = 0;

    ErrInfo[] errors;

    short index = 1;
    while(index <= recordsCount){
        auto rc = SQLGetDiagRec(
            handleType, handle, index, 
            cast(ubyte*) sqlStateBuffer.ptr, &nativeError, cast(ubyte*)messageBuffer.ptr, cast(short) messageBuffer.length,
            &messageLength
        );

        if(rc == SQL_NO_DATA) break;

        ErrInfo err;
        err.nativeError = nativeError;
        err.sqlState = cast(string) sqlStateBuffer[0 .. $].idup;
        err.msg = cast(string) messageBuffer[0 .. messageLength - 1];
        errors ~= err;
        index ++;
    }    
    return errors;
}



/* init/free odbc shortcuts */
void odbc_init_env(SQLHENV* envPtr){
    SQLRETURN rc;
    rc = SQLAllocHandle(SQL_HANDLE_ENV, cast(void*) SQL_NULL_HANDLE, envPtr);
    if(!rc.ok){
        auto errs = odbc_get_diag_rec(SQL_HANDLE_ENV, *envPtr);
        throw_exc(errs);
    }

    rc = SQLSetEnvAttr(*envPtr, SQL_ATTR_ODBC_VERSION, cast(void*) SQL_OV_ODBC3, 0);
    if(!rc.ok){
        auto errs = odbc_get_diag_rec(SQL_HANDLE_ENV, *envPtr);
        throw_exc(errs);
    }
}

void odbc_free_env(SQLHENV env){
    if(env is null) 
        return;

    auto rc = SQLFreeHandle(SQL_HANDLE_ENV, env);
    if(!rc.ok){
        auto errs = odbc_get_diag_rec(SQL_HANDLE_ENV, env);
        throw_exc(errs);
    }
}

void odbc_init_dbc(SQLHENV env, SQLHDBC* dbcPtr, string connectionString){
    SQLRETURN rc;
    rc = SQLAllocHandle(SQL_HANDLE_DBC, env, dbcPtr);
    if(!rc.ok){
        auto errs = odbc_get_diag_rec(SQL_HANDLE_DBC, *dbcPtr);
        errs.throw_exc();
    }

    rc = SQLDriverConnect(*dbcPtr, null, cast(ubyte*)connectionString.ptr, SQL_NTS, null, 0, null, SQL_DRIVER_NOPROMPT);
    if(!rc.ok){
        auto errs = odbc_get_diag_rec(SQL_HANDLE_DBC, *dbcPtr);
        errs.throw_exc();
    }
}

void odbc_free_dbc(SQLHDBC dbc){
    if(dbc is null)
        return;

    SQLRETURN rc;

    rc = SQLDisconnect(dbc);
    if(!rc.ok){
        auto errs = odbc_get_diag_rec(SQL_HANDLE_DBC, dbc);
        errs.throw_exc();   
    }

    rc = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    if(!rc.ok){
        auto errs = odbc_get_diag_rec(SQL_HANDLE_DBC, dbc);
        errs.throw_exc(); 
    }
}

void odbc_free_stmt(SQLHSTMT stmt){
    if(stmt is null)
        return;
}


/* statement shortcuts */
auto odbc_sql_alloc_stmt_handle(SQLHDBC dbc, SQLHSTMT* stmtPtr){
    auto rc = SQLAllocHandle(SQL_HANDLE_STMT, dbc, stmtPtr);
    if(!rc.ok){
        auto errs = odbc_get_diag_rec(SQL_HANDLE_STMT, *stmtPtr);
        errs.throw_exc();
    }
	return rc;
}

/*  ========================================================
    template func for checking return code in place.
    ========================================================    */
auto _fx_stmt(alias fn)(Parameters!fn params) {
    auto stmt = params[0]; //stmt is always first parameter
    auto rc = fn(params);    
    if(!rc.ok && !rc.still_exec) {
        auto errs = odbc_get_diag_rec(SQL_HANDLE_STMT, stmt);
        errs.throw_exc();
    }
    return rc; 
}


auto odbc_sql_prepare(SQLHSTMT stmt, string query) {
    auto q_ptr = cast(ubyte*) query.ptr;
    auto q_len = cast(SQLINTEGER) query.length;
    auto rc = _fx_stmt!SQLPrepare(stmt, q_ptr, q_len);
	return rc;
}

/* =======================================================================
    SQLExecDirect
======================================================================== */
auto odbc_sql_exec_direct(SQLHSTMT stmt, string query){
    auto q_ptr = cast(ubyte*) query.ptr;
    auto q_len = cast(SQLINTEGER) query.length;
    return _fx_stmt!SQLExecDirect(stmt, q_ptr, q_len);            
}

//auto _odbc_sql_exec_direct_async(SQLHSTMT stmt, string query){
//    import vibe.core.core: yield;
//    odbc_sql_async_enable(stmt);
//    while(_odbc_sql_exec_direct(stmt, query) == SQL_STILL_EXECUTING){
//        yield();
//    }
//}
//
///* run direct execution in separate thread */
//auto _odbc_sql_exec_direct_worker_thread(shared(SQLHSTMT) stmt, string query) {
//    import std.concurrency;
//    auto parentTid = receiveOnly!Tid();
//    try{
//        _odbc_sql_exec_direct(cast(SQLHSTMT) stmt, query);
//        parentTid.send(thisTid, true);
//    }
//    catch(DodbcException e){
//        parentTid.send(thisTid, cast(shared) e.errors);
//    }    
//}
//
//auto _odbc_sql_exec_direct_threaded(SQLHSTMT stmt, string query){
//    import vibe.core.core : yield; 
//    import std.concurrency;
//    auto workerTid = spawn(&_odbc_sql_exec_direct_worker_thread, cast(shared) stmt, query);
//    workerTid.send(thisTid);
//    bool exitFlag = false;
//    while(!exitFlag){
//        yield();
//        receiveTimeout(
//            100.msecs, 
//            (Tid tid, bool done) { 
//                if(tid == workerTid) exitFlag = done;                        
//            },
//            (Tid tid, shared(ErrInfo[]) errs) { 
//                if(tid == workerTid) (cast(ErrInfo[])errs).throw_exc();
//            }
//        );                
//    }
//}
//
///* interface */
//void odbc_sql_exec_direct(SQLHSTMT stmt, string query, bool async = false, bool sep_thread = false) {
//    if(!async){
//        // not async mode -> just execute
//        _odbc_sql_exec_direct(stmt, query);
//    }
//    else {                        
//        if(sep_thread) {
//            _odbc_sql_exec_direct_threaded(stmt, query);
//        }
//        else{
//            _odbc_sql_exec_direct_async(stmt, query);
//        }
//    }
//}



/* =======================================================================
    SQLExecute
======================================================================== */
auto odbc_sql_exec(SQLHSTMT stmt){
    return _fx_stmt!SQLExecute(stmt);        
}

//void _odbc_sql_exec_async(SQLHSTMT stmt){
//    import vibe.core.core : yield; 
//    odbc_sql_async_enable(stmt);
//    while(_odbc_sql_exec(stmt).still_exec){
//        yield();
//    }
//}
//
//void _odbc_sql_exec_worker_thread(shared(SQLHSTMT) stmt) {
//    import std.concurrency;
//    auto parentTid = receiveOnly!Tid();
//    try{
//        _odbc_sql_exec(cast(SQLHSTMT) stmt);
//        parentTid.send(thisTid, true);
//    }
//    catch(DodbcException e){
//        parentTid.send(thisTid, cast(shared) e.errors);
//    }    
//}
//
//void _odbc_sql_exec_threaded(SQLHSTMT stmt){
//    import vibe.core.core : yield; 
//    import std.concurrency;
//    auto workerTid = spawn(&_odbc_sql_exec_worker_thread, cast(shared) stmt);
//    workerTid.send(thisTid);
//    bool exitFlag = false;
//    while(!exitFlag){
//        yield();
//        receiveTimeout(
//            100.msecs, 
//            (Tid tid, bool done) { 
//                if(tid == workerTid) exitFlag = done;
//                
//            },
//            (Tid tid, shared(ErrInfo[]) errs) { 
//                if(tid == workerTid) (cast(ErrInfo[])errs).throw_exc();
//            }
//        );                
//    }
//}
//
//void odbc_sql_exec(SQLHSTMT stmt, bool async = false, bool sep_thread = false) {
//    if(!async){
//        _odbc_sql_exec(stmt);
//    }
//    else{                
//        
//        if(sep_thread) {            
//           _odbc_sql_exec_threaded(stmt);
//        }
//        else{            
//            _odbc_sql_exec_async(stmt);
//        }
//    }
//}

/*  ======================================================= */


auto odbc_sql_reset_params(SQLHSTMT stmt){
    return _fx_stmt!SQLFreeStmt(stmt, SQL_RESET_PARAMS);
}

/*  extract Values */
T columnValue(T)(SQLHSTMT stmt, int col){
    scope(failure) return T.init; 

    SQLLEN len; 
    // ==========================================
    static if(is(string == T)) {
        string res;
        enum SIZE = 255;			
        SQLCHAR[SIZE] buf;
        
        bool haveReadAll = false;
        while(!haveReadAll){
            _fx_stmt!SQLGetData(stmt, cast(ushort)col, SQL_CHAR, buf.ptr, SIZE, &len);          

            if(len == SQL_NULL_DATA)
                return string.init;

            if(len <= SIZE)
                haveReadAll = true;

            res ~= cast(string) buf[0 .. len > SIZE ? SIZE : len].idup;
        }
                                
        return res;
    }
    // ==========================================
    else static if(is(double == T)) {						
        double res;

        _fx_stmt!SQLGetData(stmt, cast(ushort)col, SQL_DOUBLE, &res, double.sizeof, &len);
   
        if(len == SQL_NULL_DATA)
            return double.init;
            
        return res;
    }	
    // ==========================================
    else static if(is(int == T)) {
        version(X86){
            int res;
        }
        else version(X86_64){
            long res;		
        }

        _fx_stmt!SQLGetData(stmt, cast(ushort)col, SQL_INTEGER, &res, int.sizeof, &len);

        if(len == SQL_NULL_DATA)
            return int.init;    

        version(X86){
            return res;
        }
        else version(X86_64) {
            return cast(int) res;
        }			
    }
    // ==========================================
    else static if(is(Date == T)) {
        DATE_STRUCT s;

        _fx_stmt!SQLGetData(stmt, cast(ushort) col, SQL_C_DATE, cast(SQLPOINTER) &s, s.sizeof, &len);

        if(len == SQL_NULL_DATA)
            return Date.init;    

        auto d = Date(s.year, s.month, s.day);
        return d;
    }
    // ==========================================
    else static if(is(DateTime == T)) {
        TIMESTAMP_STRUCT s;
        _fx_stmt!SQLGetData(stmt, cast(ushort) col, SQL_C_TIMESTAMP, &s, s.sizeof, &len);

        if(len == SQL_NULL_DATA)
            return DateTime.init;

        auto d = DateTime(s.year, s.month, s.day, s.hour, s.minute, s.second);
        return d;
    }
    // ==========================================
    else {
        /* catch all return null */
        return T.init;
    }	
}

/*  Column information */
struct ColumnInfo {
    string name;
    short dataType;
    short decimalsCount;
    short nullable;
    uint size;
    @property bool isNullable() {
        return nullable == SQL_NULLABLE;
    }  
}

ColumnInfo[] getColumnsInfo(SQLHSTMT stmt){
    short count;

    SQLRETURN rc;
    rc = SQLNumResultCols(stmt, &count);
    if(!rc.ok) {
        auto errs = odbc_get_diag_rec(SQL_HANDLE_STMT, stmt);
        errs.throw_exc();  
    }
    
    ColumnInfo[] res;

    enum SIZE = 255;
    SQLCHAR[SIZE] buf;

    short len;
    short type;
    uint size;
    short decs;
    short nullable;

    for(int i=0;i<count;i++){

        rc = SQLDescribeCol(stmt, cast(ushort)(i + 1), buf.ptr, SIZE, &len, &type, &size, &decs, &nullable);    
        if(!rc.ok){
            auto errs = odbc_get_diag_rec(SQL_HANDLE_STMT, stmt);
            errs.throw_exc();      
        }
        
        auto ci = ColumnInfo(
            cast(string) buf[0 .. len > SIZE ? SIZE : len].idup,
            type,
            decs,
            nullable,
            size
        );    

        res ~= ci;
    }
    return res;
}


/* describe parameter */
struct ParameterInfo {
    ushort type;
    uint size;
    ushort decimalDigits;
}

ParameterInfo odbc_sql_describe_parameter(SQLHSTMT stmt, ushort position){
    /* ====================================            
        SQLRETURN SQLDescribeParam(  
            SQLHSTMT        StatementHandle,  
            SQLUSMALLINT    ParameterNumber ,  
            SQLSMALLINT *   DataTypePtr,  
            SQLULEN *       ParameterSizePtr,  
            SQLSMALLINT *   DecimalDigitsPtr,  
            SQLSMALLINT *   NullablePtr);              
    ======================================= */

    SQLSMALLINT type;
    SQLULEN size;
    SQLSMALLINT decimalDigits;

    auto rc = SQLDescribeParam(
        stmt, position,
        &type, &size, &decimalDigits,
        null
    );

    if(!rc.ok) {
        auto errs = odbc_get_diag_rec(SQL_HANDLE_STMT, stmt);
        errs.throw_exc(); 
    }

    auto retVal = ParameterInfo(type, size, decimalDigits);
    return retVal;
}



/*  Bind sql parameter */
auto odbc_sql_bind_parameter(T)(SQLHSTMT stmt, ushort position, T value) {
/*
+++++++++++++++++++++++++++++++++++++++++++++++++++++++
    SQLRETURN SQLBindParameter(  
      SQLHSTMT        StatementHandle,  
      SQLUSMALLINT    ParameterNumber,  
      SQLSMALLINT     InputOutputType,  
      SQLSMALLINT     ValueType,  
      SQLSMALLINT     ParameterType,  
      SQLULEN         ColumnSize,  
      SQLSMALLINT     DecimalDigits,  
      SQLPOINTER      ParameterValuePtr,  
      SQLLEN          BufferLength,  
      SQLLEN *        StrLen_or_IndPtr
    );  
+++++++++++++++++++++++++++++++++++++++++++++++++++++++
*/ 
    static if(is(string==T)) {
        auto data = cast(void[]) value;
        SQLSMALLINT valueType = SQL_C_CHAR;
        SQLSMALLINT parameterType = SQL_VARCHAR; 
    }
    else static if (is(double == T)){
        auto data = cast(void[]) [value];
        SQLSMALLINT valueType = SQL_C_DOUBLE;
        SQLSMALLINT parameterType = SQL_DOUBLE;
    }
    else static if (is(int == T)){
        auto data = cast(void[])[value];
        SQLSMALLINT valueType = SQL_C_SLONG;
        SQLSMALLINT parameterType = SQL_INTEGER;
    }
    else static if (is(Date == T)){
        auto data = cast(void[])[SQL_DATE_STRUCT(value.year, value.month, value.day)];
        SQLSMALLINT valueType = SQL_C_TYPE_DATE;
        SQLSMALLINT parameterType = SQL_TYPE_DATE;
    }
    else static if (is(DateTime == T)){
        auto data = cast(void[])[TIMESTAMP_STRUCT(value.year, value.month, value.day, value.hour, value.minute, value.second, 0)];
        SQLSMALLINT valueType = SQL_C_TYPE_TIMESTAMP;
        SQLSMALLINT parameterType = SQL_TYPE_TIMESTAMP;
    }
    else static if (is(bool == T)){
        // just convert to int 1 or 0
        odbc_sql_bind_parameter!int(stmt, position, value ? 1 : 0);
    }
    else {
        throw new DodbcException("cant bind this type");
    }

    auto data_ptr = data.ptr;
    auto data_len = cast(int) data.length;

    auto rc = SQLBindParameter(
        stmt, position, SQL_PARAM_INPUT, valueType, parameterType, 0, 0, 
        data_ptr, data_len,        
        null
    ); 

    if(!rc.ok){
        auto errs = odbc_get_diag_rec(SQL_HANDLE_STMT, stmt);
        errs.throw_exc(); 
    }
	return rc;
}


auto odbc_sql_async_enable(SQLHSTMT stmt){
    auto rc = SQLSetStmtAttr(stmt, SQL_ASYNC_ENABLE, cast(void*) SQL_ASYNC_ENABLE_ON, 0);
    if(!rc.ok) {
        auto errs = odbc_get_diag_rec(SQL_HANDLE_STMT, stmt);
        errs.throw_exc;
    }
	return rc;
}

bool odbc_sql_can_async(SQLHDBC dbc) {
/*  ========================================
        SQLRETURN SQLGetInfo(  
        SQLHDBC         ConnectionHandle,  
        SQLUSMALLINT    InfoType,  
        SQLPOINTER      InfoValuePtr,  
        SQLSMALLINT     BufferLength,  
        SQLSMALLINT *   StringLengthPtr);  
     ===========================================    */
    SQLUINTEGER res;
    auto rc = SQLGetInfo(dbc, SQL_ASYNC_MODE, cast(SQLPOINTER) &res, res.sizeof, null);
    if(!rc.ok) {
        auto errs = odbc_get_diag_rec(SQL_HANDLE_DBC, dbc);
        errs.throw_exc();
    }

    return res == core.sys.windows.sqlext.SQL_AM_CONNECTION || res == core.sys.windows.sqlext.SQL_AM_STATEMENT;
}