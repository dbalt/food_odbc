module foo.odbc.objs;

import std.stdio;
import std.string;
import std.exception;
import std.variant;
import std.datetime;
import std.traits;
import std.parallelism;

//import core.thread;

// import etc.c.odbc.sql;
// import etc.c.odbc.sqlext;
import core.sys.windows.sql;

import foo.odbc.wraps;
import foo.odbc.errs;

class Connection {	
	private {
		SQLHENV _env;
		SQLHDBC _dbc;		
		string _connString;
		bool _async;
	}

	this(string connString, bool async = false){
		_async = async;
		_connString = connString;	
		
		odbc_init_env(&_env);
		scope (failure) odbc_free_env(_env);

		odbc_init_dbc(_env, &_dbc, _connString);
		scope (failure) odbc_free_dbc(_dbc);		
	}

	@property bool supportAsync(){
		if(null == _dbc) return false; //fixme: throw exception?
		return odbc_sql_can_async(_dbc);
	}

	Command createCommand(string sql){
		auto command = new Command(this, sql);
		return command;
	}

	~this(){
		odbc_free_dbc(_dbc);
		odbc_free_env(_env);		
	}
}

struct ExecHelper {
	private {
		bool _async;
		bool _inSepThread;
	}

	this(bool async, bool inSepThread){
		_async = async;
		_inSepThread = inSepThread;
	}
	
	auto exec(alias fn)(Parameters!fn ps){
		import vibe.core.core : yield, sleep;
		
		if(!_async) return fn(ps);
		
		if(!_inSepThread){
			bool exitFlag = false;
			auto stmt = ps[0];
			odbc_sql_async_enable(stmt);
			while(true) {
				auto rc = fn(ps);
				if(!rc.still_exec) return rc;
				yield();
				//sleep(5.msecs);
			}
		}

		auto tsk = task!fn(ps);
		tsk.executeInNewThread();
		while(!tsk.done) {
			yield();
			//sleep(5.msecs);
		}
		return tsk.yieldForce;
	}
	
}

class Command {
	private {
		Connection _connection;
		string _commandText;
		SQLHSTMT _statementHandler;
		void _allocStatement(){
			odbc_sql_alloc_stmt_handle(_connection._dbc, &_statementHandler);
		}
	}

	this(Connection connection, string commandText){
		_connection = connection;
		_commandText = commandText;
	}

	ParameterDescriptor descriptor() {
		auto x = new ParameterDescriptor(this);
		_allocStatement();
		return x;
	}

	PreparedStatement prepare(){
		auto ps = new PreparedStatement(this);
		_allocStatement();
		return ps;
	}

	ResultSet select() {
		_allocStatement();
		auto execHelper = ExecHelper(_connection._async, !_connection.supportAsync);
		execHelper.exec!odbc_sql_exec_direct(_statementHandler, _commandText);
		//odbc_sql_exec_direct(_statementHandler, _commandText, _connection._async, !_connection.supportAsync);
		auto res =  new ResultSet(this);
		res.popFront(); 	
		return res;
	}

	void execute(){
		_allocStatement();
		auto execHelper = ExecHelper(_connection._async, !_connection.supportAsync);
		execHelper.exec!odbc_sql_exec_direct(_statementHandler, _commandText);
		//odbc_sql_exec_direct(_statementHandler, _commandText, _connection._async, !_connection.supportAsync);
	}

	~this(){

	}
}

class PreparedStatement {
	private {
		Command _command;
		bool _isPrepared = false;		
		void _prepareStatement(){
			odbc_sql_prepare(_command._statementHandler, _command._commandText);
		}
	}

	this(Command command) {
		_command = command;		
	}

	void bindParam(T)(ushort pos, T val){
		odbc_sql_bind_parameter!T(_command._statementHandler, pos, val);
	}

	ResultSet select() {	
		_prepareStatement();
		auto execHelper = ExecHelper(_command._connection._async, !_command._connection.supportAsync);
		execHelper.exec!odbc_sql_exec(_command._statementHandler);
		auto res =  new ResultSet(_command);
		res.popFront(); 	
		return res;
	}

	void execute(){
		_prepareStatement();
		auto execHelper = ExecHelper(_command._connection._async, !_command._connection.supportAsync);
		execHelper.exec!odbc_sql_exec(_command._statementHandler);
	}

	void reset(){
		odbc_sql_reset_params(_command._statementHandler);
	}
}

class ResultSet {
	private {
		Command _command;
		bool _isEmpty;
		Record _record;			
	}
		
	this(Command command){
		_command = command;
		auto stmt = _command._statementHandler;
		_record = new Record(stmt, getColumnsInfo(stmt));			
	}

	@property bool empty() { return _isEmpty; }

	@property auto front() { return _record; }

	void popFront() {
		if(_isEmpty) return;
		_isEmpty = !SQLFetch(_command._statementHandler).ok;
		_record.clear();
	}
}


class Record {
	private {
		SQLHSTMT _statementHandler;
		Variant[] _values;
		bool[] _used;
		ColumnInfo[] _columns;
 
		int _getColumnIndex(string columnName){
			for(int idx = 0;idx<_columns.length;idx++)
				if(_columns[idx].name == columnName) return idx + 1;		
			throw new DodbcException("There is no such column : " ~ columnName);
		}
	}

	this(SQLHSTMT statementHandler, ColumnInfo[] columns) {
		_statementHandler = statementHandler;
		_columns = columns;
		auto length = _columns.length;
		_values.length = length;
		_used.length = length;
	}
	
	void clear(){
		foreach(ref flg;_used){
			flg = false;
		}
	}

	T getval(T)(string columnName) {
		return getval!T(_getColumnIndex(columnName));
	}

	// private int _lastIndex = -1;
	T getval(T)(int col){
		auto idx = col - 1;
		if(!_used[idx]) {
			_values[idx] = _getval!T(col);
			_used[idx] = true;
		}
		auto val = _values[idx];
		return val.get!(T);
	}

	private T _getval(T)(int col) {
		return columnValue!(T)(_statementHandler, col);
	}
}


class ParameterDescriptor {
	private {
		Command _command;
	}

	this(Command command) {
		_command = command;
		odbc_sql_prepare(_command._statementHandler, _command._commandText);
	}
	
	ParameterInfo parameterInfo(ushort position) {
		return odbc_sql_describe_parameter(_command._statementHandler, position);
	}
}

PreparedStatement sql(Connection cn, string query){
	auto cmd = cn.createCommand(query);
	auto ps = cmd.prepare();
	return ps;
}

ParameterDescriptor params(Connection cn, string query) {
	auto cmd = cn.createCommand(query);
	auto dm = cmd.descriptor();
	return dm;
}

PreparedStatement set(T)(PreparedStatement ps, ushort pos, T val){
	ps.bindParam!T(pos, val);
	return ps;
}

Connection connect(string connectionString, bool async = false) {
	return new Connection(connectionString, async);
}


void write_pi(ParameterInfo pi, string prefix = ""){
	writeln(prefix, "tp: ", pi.type, "sz: ", pi.size,"digs: ", pi.decimalDigits);
}
