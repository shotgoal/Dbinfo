package dbinfo.sunjesoft;
import java.sql.*;
import javax.sql.*;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.poi.hssf.util.HSSFColor.HSSFColorPredefined;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.util.*;
import java.util.Properties;
import org.apache.log4j.*;
import sunje.goldilocks.jdbc.GoldilocksDriver;
import oracle.jdbc.driver.*;

public class InfoManager {

	public static Connection con = null;
	public static List<Tableinfo> tableList = new ArrayList<Tableinfo>();
	public static List<Columninfo> colList = new ArrayList<Columninfo>();
	public static List<Indexinfo> idxList = new ArrayList<Indexinfo>();
	public static List<PKinfo> pkList = new ArrayList<PKinfo>();
	public static List<Uinfo> uList = new ArrayList<Uinfo>();
	
	public static int curRows = 0;
	public static int curRows2 = 0;
	
	public static XSSFWorkbook wb = null;
	public static XSSFSheet sheet = null ;
	public static XSSFSheet sheet2 = null ;
	public static XSSFFont Font1 = null;
	public static XSSFFont Font2 = null;
	public static CellStyle style1 = null;
	public static CellStyle style2 = null;
	public static CellStyle style3 = null;
	public static HashMap hashPK = null;
	
	//RAW는 BLOB처럼, XMLTYPE은 CLOB처럼
	enum TypeName { BLOB, CLOB, CHAR, DATE, LONG , NUMBER, NVARCHAR2, VARCHAR2, RAW, XMLTYPE };

	static Logger logger = LogManager.getLogger("InfoManager");
	
	
	public static void main(String[] args) throws Exception {
		
		String db_type = args[0];
		String job_type =args[1];
		
		// properties로 connection db정보 관리
		logger.info("InfoManager Start");
		hashPK = new HashMap();
		
		logger.info("Checking DB connect information.");
        FileReader resources = null;
		try {
			resources = new FileReader("db.properties");
		} catch (FileNotFoundException e1) {
			logger.error("not found db.properties.");
		}
        Properties properties = new Properties();
        try {
        	properties.load(resources);
        	
        } catch (IOException e)
        {
        	logger.error("error:",e);
        }
        
        // properties에서 연결할 DB정보로 connection 생성
        
        
        if(db_type.equals("GOLDILOCKS"))
        {
	        String URL_NODE1 = properties.getProperty("goldi_url");
	        Properties sProp = new Properties();
	        sProp.put("user", properties.getProperty("goldi_user"));
	        sProp.put("password", properties.getProperty("goldi_password"));
	        Class.forName("sunje.goldilocks.jdbc.GoldilocksDriver");
			try {
				con = DriverManager.getConnection(URL_NODE1, sProp);
				con.setAutoCommit(false);
			} catch (SQLException e) {
				logger.error("error:",e);
			}
			//1.접속자 테이블 정보 조회
			fnGetTableList();
        }
        else if(db_type.equals("ORACLE"))
        {
        	String URL_NODE1 = properties.getProperty("ora_url");
  	        Properties sProp = new Properties();
  	        sProp.put("user", properties.getProperty("ora_user"));
  	        sProp.put("password", properties.getProperty("ora_password"));
  	        Class.forName("oracle.jdbc.driver.OracleDriver");
  			try {
  				con = DriverManager.getConnection(URL_NODE1, sProp);
  			} catch (SQLException e) {
  				logger.error("error:",e);  			
  			}
  			//1.접속자 테이블 정보 조회
  			fnGetTableListORA();
        }
		
		
		//table 없으면 파일 만들지 말자.
		if(tableList.size() <= 0)
		{
			logger.warn("No table in database.");
			con.close();
			System.exit(0);
		}
		if(job_type.equals("1") || job_type.equals("2")) {
			CreateDocument(db_type, properties.getProperty("filename"));
		}
		else if(job_type.equals("3"))
		{
			CreateSqlForGoldilocks();
		}
		con.close();
		//모두 해제
		tableList.clear();
		colList.clear();
		idxList.clear();
		pkList.clear();
		curRows = 0;
		if(job_type.equals("1") || job_type.equals("2")) {
			wb.close();
		}
		
		logger.info("Work done. Check your report.");
		
	}
	
	public static void CreateDocument(String db_type, String filename)
	{
	//작업 분리, 엑셀 생성과 SQL만드는 부분
		
		//excel 서식 apache poi 참조하세요
		wb = new XSSFWorkbook();
		String sheetName = "명세서";//한개의 sheet사용 나눠서 하고 싶으면.. 분할처리 하셔요
		
		sheet = wb.createSheet(sheetName);
		sheet2 =wb.createSheet("Table List");
		// 엑셀 서식에 사용할 style 3개 만듬
		Font1 =wb.createFont();
		style1 = wb.createCellStyle();
		Font1.setFontHeightInPoints((short)12);
		Font1.setFontName("맑은고딕");
		Font1.setBold(true);
		style1.setFont(Font1);
		style1.setAlignment(HorizontalAlignment.CENTER);
		style1.setBorderTop(BorderStyle.THIN);
		style1.setBorderLeft(BorderStyle.THIN);
		style1.setBorderRight(BorderStyle.THIN);
		style1.setBorderBottom(BorderStyle.THIN);
		style1.setFillForegroundColor(HSSFColorPredefined.BRIGHT_GREEN.getIndex());
		style1.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		
		
		Font2 =wb.createFont();
		style2 = wb.createCellStyle();
		Font2.setFontHeightInPoints((short)10);
		Font2.setFontName("맑은고딕");
		Font2.setBold(false);
		style2.setFont(Font2);
		style2.setAlignment(HorizontalAlignment.LEFT);
		style2.setBorderTop(BorderStyle.THIN);
		style2.setBorderLeft(BorderStyle.THIN);
		style2.setBorderRight(BorderStyle.THIN);
		style2.setBorderBottom(BorderStyle.THIN);
		
		style3 = wb.createCellStyle();
		style3.setFont(Font2);
		style3.setAlignment(HorizontalAlignment.CENTER);
		style3.setBorderTop(BorderStyle.THIN);
		style3.setBorderLeft(BorderStyle.THIN);
		style3.setBorderRight(BorderStyle.THIN);
		style3.setBorderBottom(BorderStyle.THIN);
				
		//접속 DB별 처리
		if(db_type.equals("GOLDILOCKS"))
		{
			try {
				
				for(Tableinfo ti :tableList)
				{
					fnGetColumnList(ti.getT_name());
					fnGetTableIndex(ti.getT_name());
					fnGetTablePK(ti.getT_name());
					
					ti.setRow_cnt(fnGetTableCount(ti.getT_name()));
					fnMakeTitle(wb, sheet, ti);
					fnMakeBodyInfo(wb, sheet);
					fnMakeIndexInfo(wb, sheet);
					fnMakePKInfo(wb, sheet);
					

					fnMakeList(wb,sheet2,ti);
					
					colList.clear();
					idxList.clear();
					pkList.clear();
					
				}
			}catch(Exception ex)
			{
				logger.error("error:",ex);
			}
			
		}
		else //ORACLES
		{
			try
			{
				
				for(Tableinfo ti :tableList)
				{
					//테이블 명 파일에 출력
					fnGetColumnListORA(ti.getT_name());
					
					fnGetTableIndexORA(ti.getT_name());
					fnGetTablePKORA(ti.getT_name());
					fnGetTableUORA(ti.getT_name());
					ti.setRow_cnt(fnGetTableCount(ti.getT_name()));
					
					fnMakeTitle(wb, sheet, ti);
					fnMakeBodyInfo(wb, sheet);
					fnMakeIndexInfo(wb, sheet);
					fnMakePKInfo(wb, sheet);
					
					fnMakeList(wb,sheet2,ti);
					//shading 
					
					colList.clear();
					idxList.clear();
					pkList.clear();
					
				}

			}catch(Exception ex)
			{
				logger.error("error:",ex);
			}
		}
		logger.info("Create result xlsx file start");
		//만들어진 workbook IO처리
		try {
			String excelFileName = filename+".xlsx";//name of excel file
			FileOutputStream fileOut = new FileOutputStream(excelFileName);
	
			//write this workbook to an Outputstream.
			wb.write(fileOut);
			fileOut.flush();
			fileOut.close();
		}catch(Exception ex)
		{
			logger.error("error:",ex);
		}
				
	}
	
	public static void CreateSqlForGoldilocks()
	{
		//대상되는 오라클 테이블의 기본정보를 가지고 테이블 생성 쿼리를 만든다.

		//접속 DB별 처리
		//추후에 만들지 말지 생각해야 하는 부분, 우리는 ddl정보로 처리하니까..
		/*
		if(db_type.equals("GOLDILOCKS"))
		{
			try {
				FileOutputStream sqlOut = new FileOutputStream("Goldi.sql");
				for(Tableinfo ti :tableList)
				{
					fnGetColumnList(ti.getT_name());
					fnGetTableIndex(ti.getT_name());
					fnGetTablePK(ti.getT_name());
					
					fnMakeTitle(wb, sheet, ti);
					fnMakeBodyInfo(wb, sheet, sqlOut);
					fnMakeIndexInfo(wb, sheet, sqlOut);
					fnMakePKInfo(wb, sheet, sqlOut);
					
					colList.clear();
					idxList.clear();
					pkList.clear();
					
				}
			}catch(Exception ex)
			{
				logger.error(ex.toString());
			}
			
		}
		else //ORACLES
		{
		*/
		try
		{
			FileOutputStream sqlOut = new FileOutputStream("oraToGoldi.sql");
			
			for(Tableinfo ti :tableList)
			{
				//테이블 명 파일에 출력
				sqlOut.write(("\n").getBytes());
				sqlOut.flush();
				sqlOut.write(("--TableName :" +ti.getT_name()+"\n").getBytes());
				sqlOut.flush();
				sqlOut.write(("DROP TABLE " +ti.getT_name()+";\n").getBytes());
				sqlOut.flush();
				sqlOut.write(("commit;\n").getBytes());
				sqlOut.flush();
				sqlOut.write(("CREATE TABLE "+ ti.getT_name()+"\n").getBytes());
				sqlOut.flush();
				sqlOut.write(("(\n").getBytes());
				sqlOut.flush();
				//테이블 기본 정보 조회 한 후에 문자열 조립
				fnGetColumnListORA(ti.getT_name());				
				fnGetTableIndexORA(ti.getT_name());
				fnGetTablePKORA(ti.getT_name());
				//fnGetTableUORA(ti.getT_name());

				fnMakeSQLColumn(sqlOut);
				//fnMakeSQLPK(sqlOut);
				String str_pk = ",CONSTRAINT \"";
				
				String s_pk = "";
				String s_const ="";
				String s_shk = "";
				if(pkList.size() >0) {
					for(PKinfo pi : pkList)
					{
						s_const = pi.getPk_name();
						s_pk+=pi.getPk_colname()+", ";
					}
					if(s_pk.length()>0) {
					s_pk = s_pk.substring(0,s_pk.length()-2);
					
					str_pk += s_const +"\" PRIMARY KEY ("+s_pk+")";
					
					sqlOut.write(str_pk.getBytes());
					sqlOut.flush();
					
					}
				}
				//pk없는 경우 샤딩키를 위해서 not null 컬럼을 참조하여 임시로 만든다
				else
				{
					for(Columninfo ci: colList)
					{
						if(ci.getCol_null().equals("N"))
						{
							s_shk +=ci.getCol_name()+", ";
						}
					}
					if(s_shk.length()>0)
					{
						s_shk = s_shk.substring(0,s_shk.length()-2);
					}
				}
				sqlOut.write((")").getBytes());
				sqlOut.flush();
				sqlOut.write(("\n").getBytes());
				sqlOut.flush();
				//fnMakePKInfo(wb, sheet, sqlOut);
				//fnMakeIndexInfo(wb, sheet,sqlOut);

				
				//CONSTRAINT "AAA01M10_PK" PRIMARY KEY ("ACNT_NO", "SUB_NO", SELF_TP, "CLS_DTM")
				//shading 문자열 추가
				//테이블 스페이스??
				//클론테스트를 위해 샤딩 뺌 20210311
				if(s_pk.length()>0) {
					String str_shd = "SHARDING BY ("+s_pk+")";
					//sqlOut.write(str_shd.getBytes());
					//sqlOut.flush();
				}
				else if(s_pk.length() <= 0 && s_shk.length()>0)
				{
					String str_shd = "SHARDING BY ("+s_shk+")";
					//sqlOut.write(str_shd.getBytes());
					//sqlOut.flush();
				}
				else
				{
					String str_shd = "--CANT SHARDING, No Key for this table";
					//sqlOut.write(str_shd.getBytes());
					//sqlOut.flush();
				}
				sqlOut.write(("\n").getBytes());
				sqlOut.flush();
				//sqlOut.write(("TABLESPACE \"VIETNAM_TAB\"").getBytes());
				//SK제공을 위해 수정
				sqlOut.write(("TABLESPACE \"MEM_DATA_TBS\"").getBytes());
				sqlOut.flush();
				sqlOut.write((";\n").getBytes());
				sqlOut.flush();
				//index 생성 구문 추가.
				//create index idx_name on table (?,?,?);
				colList.clear();
				String idx_str ="";
				if(idxList.size()>0)
				{
					String before_i_name=""; 
					int i_start = 0;
					for(Indexinfo ii : idxList)
					{
						String i_name = ii.getIdx_name();
						if(!before_i_name.equals(i_name))
						{
							//처음 나올때
							if(i_start == 0)
							{
								idx_str ="\n CREATE INDEX "+ii.getIdx_name()+" ON "+ti.getT_name() +"("+ii.getIdx_colname()+" , ";
							}
							else
							{
								idx_str = idx_str.substring(0, idx_str.length()-2)+"); ";
								idx_str ="\n CREATE INDEX "+ii.getIdx_name()+" ON "+ti.getT_name() +"("+ii.getIdx_colname()+" , ";
							}
						}
						else
						{
							idx_str += ii.getIdx_colname() +" , ";
						}
						before_i_name = i_name;
					}
					if(idx_str.length()>0)
					{
						idx_str = idx_str.substring(0, idx_str.length()-2)+");";
					}
					sqlOut.write((idx_str).getBytes());
					sqlOut.flush();
				}
				idxList.clear();
				pkList.clear();
			}
			sqlOut.flush();
			sqlOut.close();
		}catch(Exception ex)
		{
			logger.error("error:",ex);
		}


	}

	public static void fnGetTableList() throws Exception
	{                  
		//User grant
		//PreparedStatement pstmt = con.prepareStatement("SELECT TABLE_SCHEMA, TABLE_NAME,TABLESPACE_NAME FROM tabs");
		//sys grant
		PreparedStatement pstmt = con.prepareStatement("SELECT TABLE_SCHEMA, TABLE_NAME,TABLESPACE_NAME FROM all_tables where TABLE_SCHEMA='IPS'");
		ResultSet rs = pstmt.executeQuery();

		while(rs.next())
		{
			Tableinfo t_info = new Tableinfo();
			
			t_info.setT_schema(rs.getString("TABLE_SCHEMA"));
			t_info.setT_name(rs.getString("TABLE_NAME"));
			t_info.setTbs_name(rs.getString("TABLESPACE_NAME"));
			
			tableList.add(t_info);
			//logger.info(t_info.t_name );
		}
		rs.close();
		pstmt.close();
	}
	public static void fnGetTableListORA() throws Exception
	{

		PreparedStatement pstmt = con.prepareStatement("SELECT TABLE_NAME, TABLESPACE_NAME FROM tabs");
		ResultSet rs = pstmt.executeQuery();
		while(rs.next())
		{
			Tableinfo t_info = new Tableinfo();
			t_info.setT_name(rs.getString("TABLE_NAME"));
			t_info.setTbs_name(rs.getString("TABLESPACE_NAME"));
			
			tableList.add(t_info);
			//logger.info(t_info.t_name );
		}
		rs.close();
		pstmt.close();
	}
	public static int fnGetTableCount(String t_name) throws Exception
	{
		PreparedStatement pstmt = con.prepareStatement("SELECT count(*) from IPS."+t_name+"");
		ResultSet rs = pstmt.executeQuery();
		int rowCount = 0;
		while(rs.next())
		{
			rowCount = Integer.parseInt(rs.getString("count(*)"));
		}
		 
		rs.close();
		pstmt.close();
		return rowCount;
	}
	
	
	public static void fnGetColumnList(String t_name) throws Exception
	{
		
		/*
		 * PreparedStatement pstmt = con.
		 * prepareStatement("select A.COLUMN_ID, A.COLUMN_NAME, A.DATA_TYPE, A.DATA_LENGTH, A.DATA_DEFAULT, A.DATA_PRECISION, A.NULLABLE, A.DATA_DEFAULT"
		 * + " from USER_TAB_COLUMNS A " + " WHERE " +
		 * " A.TABLE_SCHEMA='VN' AND A.TABLE_NAME = (?) ORDER BY A.COLUMN_ID");
		 */
		 
			
			
			  PreparedStatement pstmt = con.
			  prepareStatement("select A.COLUMN_ID, A.COLUMN_NAME, A.DATA_TYPE, A.DATA_LENGTH, TO_CHAR(NVL(A.DATA_DEFAULT,'null')) AS DATA_DEFAULT, A.DATA_PRECISION, A.NULLABLE"
			  + " from ALL_TAB_COLUMNS A " + " WHERE " +
			  " A.TABLE_SCHEMA='IPS' AND A.TABLE_NAME = (?) ORDER BY A.COLUMN_ID");
			 
			 
		pstmt.setString(1, t_name);
		ResultSet rs = pstmt.executeQuery();
			
		while(rs.next())
		{
			Columninfo c_info = new Columninfo();
			c_info.setCol_id(rs.getInt("COLUMN_ID"));
			c_info.setCol_name(rs.getString("COLUMN_NAME"));
			c_info.setCol_dtype(rs.getString("DATA_TYPE"));
			c_info.setCol_dlength(rs.getInt("DATA_LENGTH"));

			//DATA_DEAFULT , type long 처리
			String l_data = rs.getString("DATA_DEFAULT");
			String sample = l_data.toString();
			if(sample.isEmpty())
			{
				c_info.setCol_default("");
			}
			else
			{
				c_info.setCol_default(sample);
			}
			c_info.setCol_pre(rs.getString("DATA_PRECISION"));
			c_info.setCol_null(rs.getString("NULLABLE"));
			//c_info.setCol_comments(rs.getString("COMMENTS"));
			c_info.setCol_comments("");
			colList.add(c_info);
			//System.out.printf("{%s}, {%s},{%s} \n",c_info.getCol_name(),c_info.getCol_dtype(),c_info.getCol_dlength());
		}
		rs.close();
		pstmt.close();
	}
	
	public static void fnGetColumnListORA(String t_name) throws Exception
	{
		//System.out.printf("Table query : %s \n",t_name);

		PreparedStatement pstmt = con.prepareStatement("select A.COLUMN_ID, A.COLUMN_NAME, A.DATA_TYPE, A.DATA_LENGTH, A.DATA_DEFAULT, A.DATA_PRECISION, A.NULLABLE, B.COMMENTS,"
				+ "(select NVL(S.CONSTRAINT_TYPE,'N') from USER_CONS_COLUMNS C, USER_CONSTRAINTS S where A.TABLE_NAME= C.TABLE_NAME AND A.COLUMN_NAME = C.COLUMN_NAME AND C.CONSTRAINT_NAME = S.CONSTRAINT_NAME and S.CONSTRAINT_TYPE = 'U') CONSTRAINT_TYPE "
				+ "from USER_TAB_COLUMNS A, USER_COL_COMMENTS B "
				+ " WHERE A.TABLE_NAME = B.TABLE_NAME AND A.COLUMN_NAME = B.COLUMN_NAME "
				+ " AND A.TABLE_NAME = UPPER(?) ORDER BY A.COLUMN_ID");
		
		pstmt.setString(1, t_name);
		ResultSet rs = pstmt.executeQuery();
		
		while(rs.next())
		{
			Columninfo c_info = new Columninfo();
			c_info.setCol_id(rs.getInt("COLUMN_ID"));
			c_info.setCol_name(rs.getString("COLUMN_NAME"));
			c_info.setCol_dtype(rs.getString("DATA_TYPE"));
			c_info.setCol_dlength(rs.getInt("DATA_LENGTH"));
			//c_info.setCol_default(rs.getString("DATA_DEFAULT"));
			//System.out.println(rs.getString(5));
			String data = null;
			Reader csr = rs.getCharacterStream("DATA_DEFAULT");
			
			
			if(csr != null)
			{
				StringBuffer buff = new StringBuffer();
				char[] ch = new char[4000];
				int len = -1;
				while((len=csr.read(ch)) != -1)
				{
					buff.append(ch,0,len);
				}
				data = buff.toString();
				c_info.setCol_default(data);
			}
			else
			{
				c_info.setCol_default("");
			}

			c_info.setCol_pre(rs.getString("DATA_PRECISION"));
			c_info.setCol_null(rs.getString("NULLABLE"));
			c_info.setCol_comments(rs.getString("COMMENTS"));
			c_info.setCol_constraint_type(rs.getString("CONSTRAINT_TYPE"));
			colList.add(c_info);
			//System.out.printf("{%s}, {%s},{%s} \n",c_info.getCol_name(),c_info.getCol_dtype(),c_info.getCol_dlength());
		}
		rs.close();
		pstmt.close();
	}
	public static void fnGetTablePKORA(String t_name) throws Exception
	{

		PreparedStatement pstmt = con.prepareStatement("select C.CONSTRAINT_NAME, C.COLUMN_NAME from USER_CONS_COLUMNS C, USER_CONSTRAINTS S"
				+ " where C.CONSTRAINT_NAME = S.CONSTRAINT_NAME and S.CONSTRAINT_TYPE = 'P'"
				+ " AND C.TABLE_NAME = UPPER(?) ORDER BY C.POSITION");
		
		pstmt.setString(1, t_name);
		ResultSet rs = pstmt.executeQuery();

		while(rs.next())
		{
			PKinfo pk_info = new PKinfo();
			pk_info.setPk_name(rs.getString("CONSTRAINT_NAME"));
			pk_info.setPk_colname(rs.getString("COLUMN_NAME"));
			
			pkList.add(pk_info);
			//System.out.printf("{%s}, {%s} \n", pk_info.getPk_name(), pk_info.getPk_colname());
		}
		rs.close();
		pstmt.close();
	}
	public static void fnGetTableUORA(String t_name) throws Exception
	{

		PreparedStatement pstmt = con.prepareStatement("select C.CONSTRAINT_NAME, C.COLUMN_NAME from USER_CONS_COLUMNS C, USER_CONSTRAINTS S"
				+ " where C.CONSTRAINT_NAME = S.CONSTRAINT_NAME and S.CONSTRAINT_TYPE = 'U'"
				+ " AND C.TABLE_NAME = UPPER(?) ORDER BY C.POSITION");
		
		pstmt.setString(1, t_name);
		ResultSet rs = pstmt.executeQuery();

		while(rs.next())
		{
			Uinfo u_info = new Uinfo();
			u_info.setU_name(rs.getString("CONSTRAINT_NAME"));
			u_info.setU_colname(rs.getString("COLUMN_NAME"));
			
			uList.add(u_info);
			//System.out.printf("{%s}, {%s} \n", pk_info.getPk_name(), pk_info.getPk_colname());
		}
		rs.close();
		pstmt.close();
	}
	public static void fnGetPKList(HashMap<String, Integer> hm) throws Exception
	{
		//user grant
		//PreparedStatement pstmt = con.prepareStatement("select distinct C.COLUMN_NAME from USER_CONS_COLUMNS C, USER_CONSTRAINTS S where C.CONSTRAINT_NAME = S.CONSTRAINT_NAME and S.CONSTRAINT_TYPE = 'P'");
		//sys grant
		PreparedStatement pstmt = con.prepareStatement("select distinct C.COLUMN_NAME from ALL_CONS_COLUMNS C, ALL_CONSTRAINTS S where C.TABLE_SCHEMA='IPS' AND C.CONSTRAINT_NAME = S.CONSTRAINT_NAME and S.CONSTRAINT_TYPE = 'P'");
		ResultSet rs = pstmt.executeQuery();

		while(rs.next())
		{
			//PKinfo pk_info = new PKinfo();
			hm.put(rs.getString("COLUMN_NAME"), 1);
		}
		rs.close();
		pstmt.close();
	}
	public static void fnGetTablePK(String t_name) throws Exception
	{

		/*
		 * PreparedStatement pstmt = con.
		 * prepareStatement("select C.CONSTRAINT_NAME, C.COLUMN_NAME from USER_CONS_COLUMNS C, USER_CONSTRAINTS S"
		 * + " where C.CONSTRAINT_NAME = S.CONSTRAINT_NAME and S.CONSTRAINT_TYPE = 'P'"
		 * + " AND C.TABLE_NAME = UPPER(?) ORDER BY C.POSITION");
		 */
		
		PreparedStatement pstmt = con.prepareStatement("select C.CONSTRAINT_NAME, C.COLUMN_NAME from ALL_CONS_COLUMNS C, ALL_CONSTRAINTS S"
				+ " where C.TABLE_SCHEMA='IPS' AND C.CONSTRAINT_NAME = S.CONSTRAINT_NAME and S.CONSTRAINT_TYPE = 'P'"
				+ " AND C.TABLE_NAME = UPPER(?) ORDER BY C.POSITION");
		
		pstmt.setString(1, t_name);
		ResultSet rs = pstmt.executeQuery();

		while(rs.next())
		{
			PKinfo pk_info = new PKinfo();
			pk_info.setPk_name(rs.getString("CONSTRAINT_NAME"));
			pk_info.setPk_colname(rs.getString("COLUMN_NAME"));
			
			pkList.add(pk_info);
			//System.out.printf("{%s}, {%s} \n", pk_info.getPk_name(), pk_info.getPk_colname());
		}
		rs.close();
		pstmt.close();
	}
	

	
	public static void fnGetTableIndexORA(String t_name) throws Exception
	{

		PreparedStatement pstmt = con.prepareStatement("select A.INDEX_NAME, B.INDEX_TYPE, A.COLUMN_NAME "
				+ " from user_ind_columns A, user_indexes B where A.TABLE_NAME = B.TABLE_NAME AND A.INDEX_NAME= B.INDEX_NAME AND A.table_name = UPPER(?)");
		
		
		pstmt.setString(1, t_name);
		ResultSet rs = pstmt.executeQuery();

		while(rs.next())
		{
			Indexinfo i_info = new Indexinfo();
			i_info.setIdx_name(rs.getString("INDEX_NAME"));
			i_info.setIdx_type(rs.getString("INDEX_TYPE"));
			i_info.setIdx_colname(rs.getString("COLUMN_NAME"));
			
			idxList.add(i_info);
			//System.out.printf("{%s}, {%s} \n", i_info.getIdx_name(), i_info.getIdx_type());
		}
		rs.close();
		pstmt.close();
	}
	public static void fnGetTableIndex(String t_name) throws Exception
	{

		/*
		 * PreparedStatement pstmt =
		 * con.prepareStatement("select A.INDEX_NAME, B.INDEX_TYPE, A.COLUMN_NAME " +
		 * " from user_ind_columns A, user_indexes B where A.TABLE_NAME = B.TABLE_NAME AND A.INDEX_NAME= B.INDEX_NAME AND A.table_name = UPPER(?)"
		 * );
		 */
		PreparedStatement pstmt = con.prepareStatement("select A.INDEX_NAME, B.INDEX_TYPE, A.COLUMN_NAME "
				+ " from all_ind_columns A, all_indexes B where A.TABLE_NAME = B.TABLE_NAME AND A.INDEX_NAME= B.INDEX_NAME AND A.table_name = UPPER(?)");
		
		pstmt.setString(1, t_name);
		ResultSet rs = pstmt.executeQuery();

		while(rs.next())
		{
			Indexinfo i_info = new Indexinfo();
			i_info.setIdx_name(rs.getString("INDEX_NAME"));
			i_info.setIdx_type(rs.getString("INDEX_TYPE"));
			i_info.setIdx_colname(rs.getString("COLUMN_NAME"));
			idxList.add(i_info);
			//System.out.printf("{%s}, {%s} \n", i_info.getIdx_name(), i_info.getIdx_type());
		}
		rs.close();
		pstmt.close();
	}
	
	
	public static void fnMakeTitle(XSSFWorkbook wb,XSSFSheet sheet,Tableinfo ti) throws Exception
	{
		XSSFRow curRow;
		Cell cell = null;
		int sRows = curRows +1; //한줄 공백 만들어 주기
		curRow = sheet.createRow(sRows);
		
		cell= curRow.createCell(0); //한개씩 cell 만들어서 데이터 처리
		cell.setCellValue("테이블스키마");
		cell.setCellStyle(style1);
		
		cell= curRow.createCell(1);
		cell.setCellValue(ti.getT_schema());
		cell.setCellStyle(style2);
		cell= curRow.createCell(2);
		cell.setCellStyle(style2);
		cell= curRow.createCell(3);
		cell.setCellStyle(style2);
		cell= curRow.createCell(4);
		cell.setCellStyle(style2);
		cell= curRow.createCell(5);
		cell.setCellStyle(style2);
		cell= curRow.createCell(6);
		cell.setCellStyle(style2);
		cell= curRow.createCell(7);
		cell.setCellStyle(style2);
		sheet.addMergedRegion(new CellRangeAddress(sRows,sRows,1,7));
		
		
		sRows += 1;
		
		curRow = sheet.createRow(sRows);
		cell= curRow.createCell(0);
		cell.setCellValue("테이블명");
		cell.setCellStyle(style1);
		
		cell= curRow.createCell(1);
		cell.setCellValue(ti.getT_name());
		cell.setCellStyle(style2);
		cell= curRow.createCell(2);
		cell.setCellStyle(style2);
		cell= curRow.createCell(3);
		cell.setCellStyle(style2);
		cell= curRow.createCell(4);
		cell.setCellStyle(style2);
		cell= curRow.createCell(5);
		cell.setCellStyle(style2);
		cell= curRow.createCell(6);
		cell.setCellStyle(style2);
		cell= curRow.createCell(7);
		cell.setCellStyle(style2);
		sheet.addMergedRegion(new CellRangeAddress(sRows,sRows,1,7));
		
		
		sRows += 1;
		
		curRow = sheet.createRow(sRows);
		cell= curRow.createCell(0);
		cell.setCellValue("테이블사용공간");
		cell.setCellStyle(style1);
		
		cell= curRow.createCell(1);
		cell.setCellValue(ti.getTbs_name());
		cell.setCellStyle(style2);
		cell= curRow.createCell(2);
		cell.setCellStyle(style2);
		cell= curRow.createCell(3);
		cell.setCellStyle(style2);
		cell= curRow.createCell(4);
		cell.setCellStyle(style2);
		cell= curRow.createCell(5);
		cell.setCellStyle(style2);
		cell= curRow.createCell(6);
		cell.setCellStyle(style2);
		cell= curRow.createCell(7);
		cell.setCellStyle(style2);
		sheet.addMergedRegion(new CellRangeAddress(sRows,sRows,1,7));
		
		
		sRows += 1;
		
		curRow = sheet.createRow(sRows);
		cell= curRow.createCell(0);
		cell.setCellValue("테이블Row카운트");
		cell.setCellStyle(style1);
		
		cell= curRow.createCell(1);
		cell.setCellValue(ti.getRow_cnt());
		cell.setCellStyle(style2);
		cell= curRow.createCell(2);
		cell.setCellStyle(style2);
		cell= curRow.createCell(3);
		cell.setCellStyle(style2);
		cell= curRow.createCell(4);
		cell.setCellStyle(style2);
		cell= curRow.createCell(5);
		cell.setCellStyle(style2);
		cell= curRow.createCell(6);
		cell.setCellStyle(style2);
		cell= curRow.createCell(7);
		cell.setCellStyle(style2);
		
		sheet.addMergedRegion(new CellRangeAddress(sRows,sRows,1,7));

	    curRows = sRows;
	}
	
	public static void fnMakeList(XSSFWorkbook wb,XSSFSheet sheet,Tableinfo ti)
	{
		XSSFRow curRow;
		Cell cell = null;
		int sRows = curRows2;
		
		
		if(sRows == 0)
		{
			curRow = sheet.createRow(sRows);
			String[] head = {"No","TABLE NAME","ROW COUNT","HAS PK","HAS INDEX","HAS UNSUPPORT TYPE","COLUMN TYPE"};
			
			sheet.setColumnWidth((short)0, 2000);
			sheet.setColumnWidth((short)1, 5800);
			sheet.setColumnWidth((short)2, 2500);
			sheet.setColumnWidth((short)3, 3000);
			sheet.setColumnWidth((short)4, 4000);
			sheet.setColumnWidth((short)5, 5000);
			sheet.setColumnWidth((short)6, 5000);
			
			int i=0;
			for(String str : head)
			{
				cell= curRow.createCell(i);
				cell.setCellValue(str);
				cell.setCellStyle(style1);
				i++;
			}
		}
		sRows += 1;
		

		curRow = sheet.createRow(sRows);
		cell= curRow.createCell(0);
		cell.setCellValue(sRows);
		cell.setCellStyle(style3);
		
		
		cell= curRow.createCell(1);
		cell.setCellValue(ti.getT_name());
		cell.setCellStyle(style2);
		
		cell= curRow.createCell(2);
		cell.setCellValue(ti.getRow_cnt());
		cell.setCellStyle(style2);
		
		cell= curRow.createCell(3);
		if(pkList.size()>0)
		{
			cell.setCellValue("Y");
		}
		else
		{
			cell.setCellValue("N");
		}
		cell.setCellStyle(style3);
		
		cell= curRow.createCell(4);
		if(idxList.size()>0)
		{
			cell.setCellValue("Y");
		}
		else
		{
			cell.setCellValue("N");
		}
		cell.setCellStyle(style3);
		String str_uns = "N";
		String col_type = "";
		for(Columninfo ci: colList)
		{
			if(ci.getCol_dtype().equals("BLOB"))
			{
				str_uns = "Y";
				col_type +=ci.getCol_dtype()+" ";
			}
			else if(ci.getCol_dtype().equals("CLOB"))
			{
				str_uns = "Y";
				col_type +=ci.getCol_dtype()+" ";
			}
			else if(ci.getCol_dtype().equals("LONG"))
			{
				str_uns = "Y";
				col_type +=ci.getCol_dtype()+" ";
			}
			else if(ci.getCol_dtype().equals("NVARCHAR2"))
			{
				str_uns = "Y";
				col_type +=ci.getCol_dtype()+" ";
			}
			//NVARCHAR ,LONG
		}
		cell= curRow.createCell(5);
		cell.setCellValue(str_uns);
		cell.setCellStyle(style3);
		
		cell= curRow.createCell(6);
		cell.setCellValue(col_type);
		cell.setCellStyle(style3);
		
	    curRows2 = sRows;
	    //logger.info("tablelist:"+curRows2);
	}
	
	
	public static void fnMakeBodyInfo(XSSFWorkbook wb,XSSFSheet sheet)
	{
		XSSFRow curRow;
		Cell cell = null;
		int sRows = curRows +1;
		curRow = sheet.createRow(sRows);
		
		
		String[] head = {"COLUMN_ID","COLUMN_NAME","DATA_TYPE","DATA_LENGTH","DATA_DEFAULT","DATA_PRECISION","NULLABLE","COMMENT"};
		
		sheet.setColumnWidth((short)0, 6000);
		sheet.setColumnWidth((short)1, 4800);
		sheet.setColumnWidth((short)2, 3200);
		sheet.setColumnWidth((short)3, 3400);
		sheet.setColumnWidth((short)4, 4000);
		sheet.setColumnWidth((short)5, 4000);
		sheet.setColumnWidth((short)6, 3000);
		sheet.setColumnWidth((short)7, 8000);
		
		int i=0;
		for(String str : head)
		{
			cell= curRow.createCell(i);
			cell.setCellValue(str);
			cell.setCellStyle(style1);
			i++;
		}
		sRows += 1;
		
		for(Columninfo ci : colList)
		{
			curRow = sheet.createRow(sRows);
			cell= curRow.createCell(0);
			cell.setCellValue(ci.getCol_id());
			cell.setCellStyle(style3);
			
			
			cell= curRow.createCell(1);
			cell.setCellValue(ci.getCol_name());
			cell.setCellStyle(style2);
			
			cell= curRow.createCell(2);
			cell.setCellValue(ci.getCol_dtype());
			cell.setCellStyle(style2);
			
			cell= curRow.createCell(3);
			cell.setCellValue(ci.getCol_dlength());
			cell.setCellStyle(style3);
			
			cell= curRow.createCell(4);
			cell.setCellValue(ci.getCol_default());
			cell.setCellStyle(style3);
			
			cell= curRow.createCell(5);
			cell.setCellValue(ci.getCol_pre());
			cell.setCellStyle(style3);
			
			cell= curRow.createCell(6);
			cell.setCellValue(ci.getCol_null());
			cell.setCellStyle(style3);
			
			cell= curRow.createCell(7);
			cell.setCellValue(ci.getCol_comments());
			cell.setCellStyle(style2);
			//sheet.addMergedRegion(new CellRangeAddress(sRows,sRows,6,7));
			
			sRows += 1;
			
		}
	    curRows = sRows;
	}
	
	public static void fnMakeSQLColumn(FileOutputStream sqlOut)
	{
		String c_str = "";
		for(Columninfo ci : colList)
		{

			//SQL문 만드는 부분
			
			
			String s_null = "";
			String s_default = "";
			String s_unique = ""; 
			if(ci.getCol_null().equals("N"))
			{
				s_null = " NOT NULL ";
			}
			
			if(!ci.getCol_default().equals(""))
			{
				s_default = " DEFAULT " + ci.getCol_default()+" ";
			}
			
			if(ci.getCol_constraint_type().equals("U"))
			{
				s_unique = " UNIQUE " ;
			}
			else
			{
				s_unique="";
			}
			
			
			
			c_str += "\t\""+ci.getCol_name() +"\"\t";
			if(ci.getCol_dtype().equals("TIMESTAMP(6)"))
			{
				c_str += " TIMESTAMP(6) "+s_unique+s_null+" "+s_default+" ,\n";
			}
			else if(ci.getCol_dtype().equals("TIMESTAMP(3)"))
			{
				c_str += " TIMESTAMP(3) "+s_unique+s_null+" "+s_default+" ,\n";
			}
			else {
			switch (TypeName.valueOf(ci.getCol_dtype().trim())){
			
				case BLOB :
					c_str += " LONG VARBINARY ,\n"; 
					break;
				case CLOB :
					c_str += " LONG VARCHAR("+ci.getCol_dlength()+") "+s_unique+s_null+" "+s_default+" ,\n"; 
					break;
				case CHAR :
					c_str += " CHAR("+ci.getCol_dlength()+") "+s_unique+s_null+" "+s_default+" ,\n";
					break;
				case DATE :
					c_str += " DATE "+s_unique+s_null+" "+s_default+",\n";
					break;
				case LONG :
					//c_str += " LONG VARCHAR("+ci.getCol_dlength()+") "+s_default+" "+s_null+",\n";
					c_str += " LONG VARCHAR "+s_unique+s_null+" "+s_default+" ,\n";
					break;
				case NUMBER :
					String temp =""+ ci.getCol_pre();
					if(temp.equals("null"))
					{
						c_str += " NUMBER "+s_unique+" "+s_null+" ,\n";
					}
					else {
					c_str += " NUMBER("+ci.getCol_pre()+") "+s_unique+s_null+" "+s_default+" ,\n";
					}
					break;
				case NVARCHAR2 :
					c_str += " LONG VARCHAR "+s_default+" "+s_null+" ,\n";
					break;
				case VARCHAR2 :
					c_str += " VARCHAR("+ci.getCol_dlength()+") "+s_unique+s_null+" "+s_default+" ,\n";
					break;
				case RAW :
					c_str += " LONG VARBINARY ,\n";
					break;
				case XMLTYPE :
					c_str += " LONG VARCHAR "+s_null+" "+s_default+" ,\n";
					break;
				//TIMESTAMP(6) enum처리 안됨, default로 처리
				default :
					break;
				}
			}
			
		}
		if(!c_str.equals(""))
		{
			c_str = c_str.substring(0, c_str.length()-2);
		}
		try {
			sqlOut.write(c_str.getBytes());
			sqlOut.flush();
			sqlOut.write("\n".getBytes());
			sqlOut.flush();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("error:",e);
		}
	}

	//4.엑셀 sheet -> 테이블명 명세서 body - 컬럼 작성 (테이블별)
	
	public static void fnMakeIndexInfo(XSSFWorkbook wb,XSSFSheet sheet)
	{
		XSSFRow curRow;
		Cell cell = null;
		
		int sRows = curRows +1;
		int startRow = sRows;
		curRow = sheet.createRow(sRows);
		
		String[] head = {"INDEX_NAME","INDEX_TYPE","INDEX_COLUMN"};
		
		int i=1;
		
		cell= curRow.createCell(0);
		cell.setCellValue("인덱스");
		cell.setCellStyle(style1);
		
		for(String str : head)
		{
			cell= curRow.createCell(i);
			cell.setCellValue(str);
			cell.setCellStyle(style1);
			i++;
		}
		sRows += 1;
		
		for(Indexinfo ii : idxList)
		{
			curRow = sheet.createRow(sRows);
			cell= curRow.createCell(1);
			cell.setCellValue(ii.getIdx_name());
			cell.setCellStyle(style2);
			
			cell= curRow.createCell(2);
			cell.setCellValue(ii.getIdx_type());
			cell.setCellStyle(style2);
			
			cell= curRow.createCell(3);
			cell.setCellValue(ii.getIdx_colname());
			cell.setCellStyle(style2);

			sRows += 1;
			
		}
		if(startRow != (sRows-1))
		{
			sheet.addMergedRegion(new CellRangeAddress(startRow,sRows-1,0,0));
		}
	    curRows = sRows;
	}
	
	
	public static void fnMakePKInfo(XSSFWorkbook wb,XSSFSheet sheet)
	{
		XSSFRow curRow;
		Cell cell = null;
		
		int sRows = curRows +1;
		int startRow = sRows;
		curRow = sheet.createRow(sRows);
		
		String[] head = {"CONSTRAINT_NAME","COLUMN_NAME"};
		
		int i=1;
		
		cell= curRow.createCell(0);
		cell.setCellValue("PrimaryKey");
		cell.setCellStyle(style1);
		
		for(String str : head)
		{
			cell= curRow.createCell(i);
			cell.setCellValue(str);
			cell.setCellStyle(style1);
			i++;
		}
		sRows += 1;
		
		for(PKinfo ppk : pkList)
		{
			curRow = sheet.createRow(sRows);
			cell= curRow.createCell(1);
			cell.setCellValue(ppk.getPk_name());
			cell.setCellStyle(style2);
			
			cell= curRow.createCell(2);
			cell.setCellValue(ppk.getPk_colname());
			cell.setCellStyle(style2);

			sRows += 1;
			
		}
		if(startRow != (sRows-1))
		{
			sheet.addMergedRegion(new CellRangeAddress(startRow,sRows-1,0,0));
		}
	    curRows = sRows;
	}
	
	//5.엑셀 sheet -> 테이블명 부가정보 기입 - PK 작성 (테이블별)

}


