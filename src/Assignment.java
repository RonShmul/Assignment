import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Assignment {

    private String connection_str="";
    private String username="";
    private String password="";
    private Connection connection;

    public Assignment(String Connection_str, String DB_username, String DB_password){
        this.connection_str = Connection_str;
        this.username = DB_username;
        this.password = DB_password;
    }

    public void connect(){
        try
        {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            connection = DriverManager.getConnection(connection_str, username, password);
            //connection.setAutoCommit(false); todo: maybe?
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void disconnect(){  //todo: ?

    }

    public void fileToDataBase(String path){
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line;
            connect();  // todo: maybe only when null
            while( (line=reader.readLine()) != null){
                String [] parts = line.split(",");
                addToTable(parts);
            }
        }catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void addToTable(String[] parts) throws SQLException {
        PreparedStatement ps = null;
        try{
            String title = parts[0];
            int year = Integer.parseInt(parts[1]);
            String insert = "INSERT into MediaItem(TITLE,PROD_YEAR) VALUES(?,?)";
            ps = connection.prepareStatement(insert);
            ps.setString(1,title);
            ps.setInt(2,year);
            ps.executeUpdate();
            connection.commit();
            ps.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }finally{
        try{
            if(ps != null){ps.close();}
        }catch (SQLException e3) {e3.printStackTrace();}
            try{
                if(connection != null){connection.close();}
            }catch (SQLException e3) {e3.printStackTrace();}
        }
    }

    public void calculateSimilarity(){

    }
}


