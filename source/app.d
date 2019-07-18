import std.stdio;
import std.string;
import std.exception;
import std.variant;
import std.datetime;

import foo.odbc;



void example() {
	auto connString = "dsn=PGX112";
	// auto connString = "dsn=SQLX;uid=foo;pwd=123";

	auto cn = connect(connString, false);

	// create example table
	cn.sql("create table foobarx1(foo int, bar varchar(255));").execute();

	// insert few records
	auto ps = cn.sql("
		insert into foobarx1(foo, bar) 
		values (?,?);");
	ps.set(1, 10).set(2, "ten").execute();
	ps.set(1, 20).set(2, "twenty").execute();
	ps.set(1, 30).set(2, "thirty").execute();

	// query for inserted records
	auto res = cn
		.sql("
			select 
				t.foo,
				t.bar 
			from foobarx1 t 
			where 1=1 
				and t.foo < ?")
		.set(1, 30)
		.select();

	// iterating over result set
	foreach(r;res){
		writeln("foo: ", r.getval!int("foo"), " bar: ", r.getval!string("bar"));
	}

	/*
		output:
		foo: 10 bar: ten
		foo: 20 bar: twenty 
	*/

	// drop example table
	cn.sql("drop table foobarx1;").execute();
}

void main()
{
	try {
		// throw new FoodOdbcException("fuck");
		example();
	}
	catch(FoodOdbcException exc){
		// writeln("###  ", exc, "  ###");
		writeln("1st # ", exc, " # 1st");
	}
	catch(Exception exc){
		writeln("2nd # ", exc, " # 2nd");
	}
	catch(Throwable exc){
		writeln("3rd # ", exc, " # 3rd");
	}
	writeln("fin");

	int x = 0;
	while(x<10){
		writeln("shit");
		x++;
	}
}