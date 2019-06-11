# Foo-D tools

only simple odbc wrapper atm ^_^

Tested on windows only, with postgresql, mssql (sqlncli11) and teradata odbc drivers

Can be used in async mode with vibe.d. 
When it's possible (with sqlncli11) odbc drivers async capabilities are utilised, when it's not possible - sql command invoked in separate thread via **Task.executeInNewThread()**

Usage:

~~~~
import foo.odbc;
...
auto odbcConnString = "DSN=MYDSN";
auto res = connect(odbcConnString) 
// connectAsync(odbcConnString) for using in vibe.d
    .sql("
        select
            t.id,   -- int fld
            t.name, -- varchar fld
            t.amt   -- float fld
        from t_some_entity t
        where 1=1
            and t.grp = ? -- varchar param 
            and t.lvl = ? -- int param
            and t.dt > ?  -- date
    ")
    .set(1, "group_x1") // setup first param
    .set(2, 3) // setup second param
    .set(3, Date(2019,2,3)) //setup third param
    .select();

foreach(r;res){
    writeln(
        "id: ", r.getval!int("id"), 
        "name: ", r.getval!string("name"), 
        "amt: ", r.getval!double("amt")
    );
}    
~~~~

