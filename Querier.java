import java.util.ArrayList;
import java.util.List;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;
import java.sql.Connection;
import java.sql.SQLException;

public class Querier {
	private Connection conn;
	
	public Querier(Connection conn) {this.conn = conn;}
	
	public List<String> getRelNames() throws SQLException {DatabaseMetaData metaData = conn.getMetaData();
		List<String> result = new ArrayList<>();
		ResultSet rs = metaData.getTables(null, null, "%", new String[]{"TABLE"});
		while (rs.next()) { result.add(rs.getString("TABLE_NAME"));}
		return result;
	}
	
	public ResultSet getResultSet(String relname) throws SQLException {return conn.createStatement().executeQuery("SELECT * FROM "+relname);}
	
}

