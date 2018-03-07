import org.apache.hive.jdbc.HiveStatement;
import org.apache.hive.jdbc.logs.InPlaceUpdateStream;
import org.apache.hive.service.rpc.thrift.TProgressUpdateResp;

import java.sql.*;
import java.util.List;


public class Test {

    /**
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default;auth=noSasl", "warrenyoung", "warrenyoung");
        HiveStatement stmt = (HiveStatement)con.createStatement();
        stmt.setInPlaceUpdateStream(new InPlaceUpdateStream() {
            @Override
            public void update(TProgressUpdateResp tProgressUpdateResp) {
                List<List<String>> a = tProgressUpdateResp.getRows();
                if (a.size() > 0)
                    System.out.print(a.get(0).get(0));
            }

            @Override
            public EventNotifier getEventNotifier() {
                return new EventNotifier();
            }
        });
        Thread thread = new Thread(){
            @Override
            public void run() {
                try {
                    while (!stmt.isClosed() && stmt.hasMoreLogs()) {
                        try {
                            for (String log : stmt.getQueryLog(true, 100)) {
                                System.out.println(log);
                            }
                            sleep(500L);
                        } catch (SQLException e) {
                            e.printStackTrace();
                            return;
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            return;
                        }
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };
        thread.start();
        String tableName = "`userinfo`";
        String sql = "use blog";
        System.out.println("Running: " + sql);
        stmt.execute(sql);

        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }

        // load data into table
        // NOTE: filepath has to be local to the hive server
        // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line

        // select * query
        sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getString(1)) + "\t" + res.getString(2));
        }

        // regular hive query
        sql = "select count(1) from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }

        sql = "insert into  " + tableName + " values ('192.168.0.104', 'warren', '123', '')";
        System.out.println("Running: " + sql);
        stmt.execute(sql);

        sql = "select name, count(1) from " + tableName + " group by name";
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getString(1)) );
        }
        List<String> logs  = stmt.getQueryLog(true, 10);
        for (String log : logs){
            System.out.print(log);
        }
    }
}
