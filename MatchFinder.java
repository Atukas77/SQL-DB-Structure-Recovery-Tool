import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;


public class MatchFinder implements IMatchFinder {
	
	private Querier querier;
	private List<String> relNames;
	private String relationX; // store names of x and y when they're found
	private String relationY;
	
	public MatchFinder(Querier arg) throws SQLException {
		this.querier = arg;
		this.relNames = querier.getRelNames();
	}

	// returns the number of rows in a relation
	private int numRows(String relName) throws SQLException {
		// get ResultSet for the relation
		ResultSet rs = querier.getResultSet(relName);
		int rowNum = 0;
		while (rs.next()) {
			rowNum++;
		}
		return rowNum;
	}

	// returns the number of columns in a relation
	private int numColumns(String relName) throws SQLException {
		// get ResultSet for the relation
		ResultSet rs = querier.getResultSet(relName);
		
		// get the ResultSetMetaData object
		ResultSetMetaData md = rs.getMetaData();
		
		// retrieve and return column num
		return md.getColumnCount();
	}

	// converts a row of a ResultSet into a list of objects
	private List<Object> rowToList(ResultSet rs) throws SQLException {
		ResultSetMetaData md = rs.getMetaData();
		int columnCount = md.getColumnCount();
		List<Object> row = new ArrayList<>();
		// iterate over each value of a row
		for (int i = 1; i <= columnCount; i++) {
			row.add(rs.getObject(i)); // store each value in its own type
		}
		return row;
	}

	// returns a set of rows from a relation
	private Set<List<Object>> rowSet(String relName) throws SQLException {
		Set<List<Object>> rowSet = new HashSet<>();
		ResultSet rs = querier.getResultSet(relName);
		while (rs.next()) {
			rowSet.add(rowToList(rs)); // add each row as a list of objects
		}
		return rowSet;
	}

	// checks if a relation is a union of two other relations
	private boolean Union(String relX, String relY, String relZ) throws SQLException {
		// filter by checking if the column numbers match
		if (numColumns(relX) != numColumns(relY) || numColumns(relX) != numColumns(relZ)) {
			return false;
		}
		// filter by checking if num of z rows at least as large as x+y
		// (may not be exactly equal if there are duplicates)
		if (numRows(relZ) < numRows(relX) + numRows(relY)) {
			return false;
		}
		// create set for each table's rows
		Set<List<Object>> xRows = rowSet(relX);
		Set<List<Object>> yRows = rowSet(relY);
		Set<List<Object>> zRows = rowSet(relZ);

		//  union of x and y rows
		Set<List<Object>> xyUnion = new HashSet<>();
		xyUnion.addAll(xRows);
		xyUnion.addAll(yRows);

		// check if the union of x and y rows equals the rows of z
		return xyUnion.equals(zRows);

	}

	// checks if a relation is a cartesian product of two other relations
	private boolean CartProd(String relX, String relY, String relZ) throws SQLException {
		// filter by checking if the number of z columns matches x+y columns
		if (numColumns(relZ) != numColumns(relX) + numColumns(relY)) {
			return false;
		}
		// filter by checking if num of z rows equals x*y rows
		if (numRows(relZ) != numRows(relX) * numRows(relY)) {
			return false;
		}
		
		// create set for each table's rows
		Set<List<Object>> xRows = rowSet(relX);
		Set<List<Object>> yRows = rowSet(relY);
		Set<List<Object>> zRows = rowSet(relZ);
		
		// check if each row of z is a combination of a row from x and a row from y
		int xColumnNum = numColumns(relX);
		for (List<Object> zRow : zRows) {
			// slice zRow into two sections - one corresponding to x and one to y
			List<Object> xSection = zRow.subList(0, xColumnNum);
			List<Object> ySection = zRow.subList(xColumnNum, zRow.size());
	
			// check if both sections exist in their sets
			if (!(xRows.contains(xSection) && yRows.contains(ySection))) {
				return false; // if any section is missing, z is not a cartprod of x and y
			}
		}
		return true;
	}

	// checks each combination of relations and looks for a match 
	// then returns a message of the result
	public String message() throws SQLException {
		for (String z : relNames) {
			for (String x : relNames) {
				for (String y : relNames) {
					// filter the duplicates
					if (!z.equals(x) && !z.equals(y) && !x.equals(y)) {
						// check for union
						if (Union(x, y, z)) {
							this.relationX = x;
							this.relationY = y;
							return z + " is UNION of " + x + " and " + y;
						}
						// check for cartprod
						if (CartProd(x, y, z)) {
							this.relationX = x;
							this.relationY = y;
							return z + " is CARTPROD of " + x + " and " + y;
						}
					}
				}
			}
		}

		return "NO MATCH";
	}
		
	public List<String> commandsForRel1() {

		List<String> commands = new ArrayList<>();
		 try {
			// overwrite table if it already exists
			commands.add("DROP TABLE IF EXISTS " + relationX + ";");
			
			// write the CREATE TABLE command
			String CREATE_TABLE = "CREATE TABLE " + relationX + " (";
			int columnNum = numColumns(relationX);
			for (int i = 1; i <= columnNum; i++) {
				CREATE_TABLE += "column_" + i + " INTEGER"; // type is guaranteed to be int
				if (i < columnNum) { // to avoid trailing comma
					CREATE_TABLE += ", ";
				}
			}
			CREATE_TABLE += ");";
			commands.add(CREATE_TABLE);
	
			// write INSERT INTO statements for each row
			ResultSet rs = querier.getResultSet(relationX);
			while (rs.next()) {
				String INSERT_INTO = "INSERT INTO " + relationX + " VALUES (";
				for (int i = 1; i <= columnNum; i++) {
					int value = rs.getInt(i);
					INSERT_INTO += value;
					if (i < columnNum) { // to avoid trailing comma
						INSERT_INTO += ", ";
					}
				}
				INSERT_INTO += ");";
				commands.add(INSERT_INTO);
			}
		 }
		 catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return commands;
	}
	
	public List<String> commandsForRel2(){
		List<String> commands = new ArrayList<>();
		 try {
			// overwrite table if it already exists
			commands.add("DROP TABLE IF EXISTS " + relationY + ";");
			
			// write the CREATE TABLE command
			String CREATE_TABLE = "CREATE TABLE " + relationY + " (";
			int columnNum = numColumns(relationY);
			for (int i = 1; i <= columnNum; i++) {
				CREATE_TABLE += "column_" + i + " INTEGER"; // type is guaranteed to be int
				if (i < columnNum) { // to avoid trailing comma
					CREATE_TABLE += ", ";
				}
			}
			CREATE_TABLE += ");";
			commands.add(CREATE_TABLE);
	
			// write INSERT INTO statements for each row
			ResultSet rs = querier.getResultSet(relationY);
			while (rs.next()) {
				String INSERT_INTO = "INSERT INTO " + relationY + " VALUES (";
				for (int i = 1; i <= columnNum; i++) {
					int value = rs.getInt(i);
					INSERT_INTO += value;
					if (i < columnNum) { // to avoid trailing comma
						INSERT_INTO += ", ";
					}
				}
				INSERT_INTO += ");";
				commands.add(INSERT_INTO);
			}
		 }
		 catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return commands;
	}
}

