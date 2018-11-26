import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

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
        }
    }

    private void addToTable(String[] parts){
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
        }catch (SQLException e) {e.printStackTrace();}
            try{
                if(connection != null){connection.close();}
            }catch (SQLException e) {e.printStackTrace();}
        }
    }

    public void calculateSimilarity(){
        List<Integer> mids = readMidsFromTable();  // todo: needs to be long or possible with int?
        int maxDist = maxDistFromSQL();
        for (Integer i=0;i<mids.size();i++){
            for (Integer j=i+1;j<mids.size();i++){
                float similarity = simCalcFromSQL(i,j,maxDist);
                PreparedStatement ps = null;
                try{
                    connect();
                    String insert = "INSERT into Similarity VALUES(?,?,?)";
                    ps = connection.prepareStatement(insert);
                    ps.setLong(1,i);
                    ps.setLong(2,j);
                    ps.setFloat(3,similarity);
                    ps.executeUpdate();
                    connection.commit();
                    ps.close();
                }catch (SQLException e) {
                e.printStackTrace();
                }finally{
                    try{
                        if(ps != null){ps.close();}
                    }catch (SQLException e) {e.printStackTrace();}
                    try{
                        if(connection != null){connection.close();}
                    }catch (SQLException e) {e.printStackTrace();}
                }
            }
        }
    }

    //function that takes the similarity for 2 mids from the SQL function
    private float simCalcFromSQL(Integer mid1, Integer mid2, int maxDist) {
        float similarity = 0;
        connect();
        CallableStatement cstmt = null;
        String call = "{? = call SimCalculation(?,?,?)}";
        try{
            cstmt = connection.prepareCall(call);
            cstmt.setLong(2, mid1);
            cstmt.setLong(3, mid2);
            cstmt.setInt(4, maxDist);
            cstmt.registerOutParameter(1, oracle.jdbc.OracleTypes.FLOAT);
            cstmt.execute();
            similarity=cstmt.getFloat(1);
            cstmt.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }finally{
            try{
                if(cstmt != null){cstmt.close();}
            }catch (SQLException e) {e.printStackTrace();}
            try{
                if(connection != null){connection.close();}
            }catch (SQLException e) {e.printStackTrace();}
        }
        return similarity;
    }

    //function that takes the maximal distance from the SQL function
    private int maxDistFromSQL() {
        int max=0;
        connect();
        CallableStatement cstmt = null;
        String call = "{? = call MaximalDistance()}";
        try{
            cstmt = connection.prepareCall(call);
            cstmt.registerOutParameter(1, oracle.jdbc.OracleTypes.NUMBER); //todo: what?!?
            cstmt.execute();
            max=cstmt.getInt(1);
            cstmt.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }finally{
            try{
                if(cstmt != null){cstmt.close();}
            }catch (SQLException e) {e.printStackTrace();}
            try{
                if(connection != null){connection.close();}
            }catch (SQLException e) {e.printStackTrace();}
        }
        return max;
    }

    //function that takes the list of all mids from the SQL table
    private List<Integer> readMidsFromTable() {
        List<Integer> ans = new ArrayList<>();
        connect();
        PreparedStatement ps = null;
        String Select = "SELECT MID FROM MediaItems";
        try{
            ResultSet rs=ps.executeQuery();
            while(rs.next()){
                ans.add(rs.getInt("MID"));
            }
            rs.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }finally{
            try{
                if(ps != null){ps.close();}
            }catch (SQLException e) {e.printStackTrace();}
            try{
                if(connection != null){connection.close();}
            }catch (SQLException e) {e.printStackTrace();}
        }
        return ans;
    }


    // function that prints titles for mids with similarity > 0.3
    public void printSimilarItems(long mid){
        List<String> similarities = new  ArrayList<>();
        connect();
        PreparedStatement ps = null;
        String select = "SELECT MediaItems.TITLE as TITLE,SIMILARITY.MID2,SIMILARITY.SIMILARITY as SIM FROM SIMILARITY INNER JOIN MediaItems ON SIMILARITY.MID2=MediaItems.MID WHERE MID1=? ORDER BY SIMILARITY DESC";
        try{
            ps = connection.prepareStatement(select);
            ps.setLong(1, mid);
            ResultSet rs = ps.executeQuery();
            while(rs.next()){
                if (rs.getFloat("SIM") >  0.3)
                    similarities.add(rs.getString("TITLE"));
            }
            rs.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }finally{
            try{
                if(ps != null){ps.close();}
            }catch (SQLException e) {e.printStackTrace();}
            try{
                if(connection != null){connection.close();}
            }catch (SQLException e) {e.printStackTrace();}
        }

        for (String title : similarities)
        {
            System.out.println(title);
        }
    }
}
