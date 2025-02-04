import java.util.List;
import java.sql.SQLException;

public interface IWriter {
	public void newTable(List<String> commands) throws SQLException;
}
