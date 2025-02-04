import java.sql.Statement;
import java.sql.DriverManager;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class Writer implements IWriter {

	private Connection conn;

	public Writer(String path) throws SQLException {

		// overwrite output.db if it exists
        File output = new File(path);
        if (output.exists()) {
            output.delete();
        }
		// open new connection which creates an empty output.db file
		this.conn = DriverManager.getConnection("jdbc:sqlite:" + path);
	}

	public void newTable(List<String> commands) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
            // iterate through each command
            for (String command : commands) {
                stmt.executeUpdate(command); // run the sql command
            }
		}
	}
}