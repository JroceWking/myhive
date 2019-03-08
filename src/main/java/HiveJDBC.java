import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;


public class HiveJDBC {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "localhost";
    private static String user = "hadoop";
    private static String password  = "";

    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;

    public void init() throws Exception{

        Class.forName(driverName);
        conn = DriverManager.getConnection(url,user,password);
        stmt = conn.createStatement();


    }
public void createDatabase() throws Exception{

        String sql = "create database hive_jdbc_test";
        System.out.println("Running" + sql);
        stmt.execute(sql);
}

public void showDatabases() throws  Exception{

        String sql = "show databasees";
        System.out.println("runing"+sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()){
            System.out.println(rs.getString(1));
        }
}

public void createTable() throws Exception {
        String sql = "create table emp(\n"+
                "empno int,\n"+
                "ename string,\n"+
                "job string,\n"+
                "mgr int,\n"+
                "sal double,\n"+
                "deptno int,\n"+
                ")\n" +
                "row format delimited fiields terminated by '\\t'";
        System.out.println("runing" + sql);
        stmt.execute(sql);
}
public void showtables() throws Exception{
        String sql = "show tables";
        System.out.println("runing" +sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()){
            System.out.println(rs.getString(1));
        }

}

public void loadData() throws Exception {
        String filePath = "/homehadoop/data/emp.txt";
        String sql = "load data local inpath"+ filePath + "overwrite into table emp";
        System.out.println("runing" + sql);
        stmt.execute(sql);

}


    public void destory() throws Exception {
        if ( rs != null)
        {
            rs.close();
        }
        if (stmt != null)
        {
            stmt.close();
        }
        if (conn != null)
        {
            conn.close();
        }
    }

}

