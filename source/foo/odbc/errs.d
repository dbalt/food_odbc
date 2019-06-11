module foo.odbc.errs;

import std.stdio;
import std.string;
import std.exception;

class DodbcException : Exception {
    ErrInfo[] errors;

    this(string msg, string file = __FILE__, size_t line = __LINE__) {
        super(msg, file, line);
        ErrInfo err;
        err.nativeError = 1000;
        err.sqlState = "";
        err.msg = msg;
        errors ~= err;
    }

	this(ErrInfo[] errs, string file = __FILE__, size_t line = __LINE__) {
        super("", file, line);
        errors = errs;
    }	

    void writeToCons(){
        foreach(err;errors){
            writeln("[Native]: ", err.nativeError, " [SqlState]: ", err.sqlState, "[Msg]: ", err.msg);
        }
    }
}

struct ErrInfo {
    int nativeError;
    string sqlState;
    string msg;
}

/*  shortcut for throwing exception  */
void throw_exc(ErrInfo[] errs, string file = __FILE__, size_t line = __LINE__){
    throw new DodbcException(errs, file, line);
}