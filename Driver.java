// javac -d. A0.java  && java -cp ".:/tmp/sqlite-jdbc-3.8.11.2.jar" A0.java
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class Driver {
	public static void main(String[] args) throws SQLException{
		Querier q=new Querier(DriverManager.getConnection("jdbc:sqlite:"+args[0]));
		MatchFinder mf=new MatchFinder(q);
		String result=mf.message(); System.out.println(result);
		if (result != "NO MATCH") {Writer w=new Writer(args[1]); w.newTable(mf.commandsForRel1()); w.newTable(mf.commandsForRel2());}
	}
}

