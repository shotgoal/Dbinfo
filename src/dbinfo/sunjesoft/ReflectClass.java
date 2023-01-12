package dbinfo.sunjesoft;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ReflectClass {

	static Logger logger = LogManager.getLogger("ConvertManager");
	public ReflectClass() {
		// TODO Auto-generated constructor stub
		
	}
	public void cFnVarchar(int i,ResultSet rs,PreparedStatement pstm)
	{
		try {
			pstm.setString(i, rs.getString(i));
		} catch (SQLException e) {
			logger.error(e.toString());
		}
	}

}
