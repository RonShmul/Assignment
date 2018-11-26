import java.io.IOException;
import java.sql.SQLException;

public class main {
    public static void main(String[] args) throws IOException, SQLException {
        Assignment a = new Assignment("jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/ORACLE", "ronshmul", "abcd");
        //a.fileToDataBase("D:\\documents\\users\\ronshmul\\Downloads\\films.csv");
        //a.calculateSimilarity();
        a.printSimilarItems(9);

    }
}
