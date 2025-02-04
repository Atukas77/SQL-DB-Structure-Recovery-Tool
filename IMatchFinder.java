import java.sql.SQLException;
import java.util.List;

public interface IMatchFinder {
	
		public String message() throws SQLException;
	
		public List<String> commandsForRel1();
	
		public List<String> commandsForRel2();
	
}

