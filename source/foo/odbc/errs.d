module foo.odbc.errs;

import std.stdio;
import std.string;
import std.array : join, array;
import std.algorithm.iteration : map;
import std.exception;
import std.format : format;

class FoodOdbcException : Exception {
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

    override string toString(){
        enum sep = "\n";
        auto err_msgs = errors.map!(x => x.toString());
        return err_msgs.join(sep);
    }
}

struct ErrInfo {
    int nativeError;
    string sqlState;
    string msg;

    string toString(){
        return format!("[Native]: %s [SqlState]: %s [Msg]: %s")(nativeError, sqlState, msg);
    }
}

/*  shortcut for throwing exception  */
void throw_exc(ErrInfo[] errs, string file = __FILE__, size_t line = __LINE__){
    throw new FoodOdbcException(errs, file, line);
}