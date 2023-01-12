package dbinfo.sunjesoft;

import java.sql.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.text.SimpleDateFormat;

import javax.sql.*;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.commons.lang.time.StopWatch;

import java.io.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.Date;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.gson.*;

import sunje.goldilocks.jdbc.GoldilocksDriver;
import oracle.jdbc.driver.*;
import oracle.sql.BLOB;
import oracle.sql.CLOB;
import oracle.xdb.*;

public class ConvertManager {

	public static Connection con_goldi = null;
	public static Connection con_ora = null;
	public static List<Tableinfo> tableList = new ArrayList<Tableinfo>();
	public static List<Tableinfo> targetList = new ArrayList<Tableinfo>();
	public static List<Columninfo> colList = new ArrayList<Columninfo>();
	public static List<Indexinfo> idxList = new ArrayList<Indexinfo>();
	public static List<PKinfo> pkList = new ArrayList<PKinfo>();

	public static LinkedBlockingQueue<JsonObject> queue1 = new LinkedBlockingQueue<>(100000);
	public static LinkedBlockingQueue<JsonObject> queue2 = new LinkedBlockingQueue<>(100000);
	public static LinkedBlockingQueue<JsonObject> queue3 = new LinkedBlockingQueue<>(100000);
	public static LinkedBlockingQueue<JsonObject> queue4 = new LinkedBlockingQueue<>(100000);
	public static LinkedBlockingQueue<JsonObject> queue5 = new LinkedBlockingQueue<>(100000);
	public static LinkedBlockingQueue<JsonObject> queue6 = new LinkedBlockingQueue<>(100000);
	public static LinkedBlockingQueue<JsonObject> queue7 = new LinkedBlockingQueue<>(100000);
	public static LinkedBlockingQueue<JsonObject> queue8 = new LinkedBlockingQueue<>(100000);
	// public static Queue<JsonObject> queue = new LinkedList<>();
	public static StopWatch stopWatch = new StopWatch();
	public static int curRows = 0;
	public static int fetch_rows = 0;
	public static String target_table = "";

	static Logger logger = LogManager.getLogger("ConvertManager");
	public static Properties properties = new Properties();

	public static void main(String[] args) throws Exception {

		stopWatch.reset();
		logger.info("Convert Start");
		// properties로 connection db정보 관리
		// oracle 과 goldi 두개 모두 연결
		FileReader resources = null;
		try {
			resources = new FileReader("db.properties");
		} catch (FileNotFoundException e1) {
			System.out.print("Not found properties file.");
		}

		try {
			properties.load(resources);

		} catch (IOException e) {
			logger.error("error:", e);
		}

		fetch_rows = Integer.parseInt(properties.getProperty("fetch_rows"));
		/*
		String URL_NODE2 = properties.getProperty("ora_url");
		Properties sProp2 = new Properties();
		sProp2.put("user", properties.getProperty("ora_user"));
		sProp2.put("password", properties.getProperty("ora_password"));
		// System.out.printf("ORACLE: %s : %s
		// \n",properties.getProperty("ora_user"),properties.getProperty("ora_password"));
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con_ora = DriverManager.getConnection(URL_NODE2, sProp2);
			logger.info("ora connected!!");
		} catch (SQLException e) {
			logger.error("error:", e);
		}
*/
		String URL_NODE1 = properties.getProperty("goldi_url");
		Properties sProp = new Properties();
		sProp.put("user", properties.getProperty("goldi_user"));
		sProp.put("password", properties.getProperty("goldi_password"));
		try {
			Class.forName("sunje.goldilocks.jdbc.GoldilocksDriver");
			con_goldi = DriverManager.getConnection(URL_NODE1, sProp);
			con_goldi.setAutoCommit(false);
			DatabaseMetaData md = con_goldi.getMetaData();
			
			logger.info("1"+md.getDatabaseMinorVersion());
			logger.info("2"+md.getDatabaseProductName());
			logger.info("3"+md.getDatabaseProductVersion());
			logger.info("4"+con_goldi.isValid(1));
			logger.info("goldi connected!!");
		} catch (SQLException e) {
			logger.error("error:", e);
		}

		// fnSelectBlobtoimg(); //이미지 테스트 용
		// fnTestInputGoldi();

		// 1.오라클에 접속해서 대상 테이블 목록을 추출
		fnGetTableListORA(con_ora); // -->ini파일에서 읽어 오는 걸로 바꿈
		// 2.오라클에 대상테이블 목록의 컬럼을 검색해서 blob/clob등의 미지원 컬럼을 사용하는 테이블 목록을 생성, 이때 rowcount를
		// 포함한다.

		/*
		 * for(Tableinfo ti : tableList) { fnSelectTargetTableList(ti.getT_name()); }
		 */
		// 2-1.수아님 요청, 지원 테이블이지만 자동화 요청
		// String[] etc = {"AAA04M00","AAA10M00", "CWD06H00", "CWD11M00",
		// "CWW02H00","DRVSDM01","GGA06M00","SSB05M00","SSI02M20","TSO03H20","TSO04M00","VSD01M00","VSD01M10","XIB01H00","XIH02M01"};
		// 2021.07.16 자동화 제거, AAA01M10만 가능하도록 수정
		/*
		 * for(Tableinfo ti :targetList) { logger.info(ti.getT_name()); }
		 */
		/*
		 * //String[] etc =
		 * {"AAA10M00","CWW02H00","DRVSDM01","SSB05M00","TSO03H20","TSO04M00","VSD01M00"
		 * ,"VSD01M10","XIB01H00","XIH02M01"}; //String[] etc = {"AAA01M10"}; //String[]
		 * etc = {"AAA10M00"}; //String[] etc = {"tt1"}; //String[] etc = {"ttt1"};
		 * 
		 */
		logger.info("target_tables :" + properties.getProperty("target_table"));
		String[] etc = properties.getProperty("target_table").split(",");

		// String[] etc = {"AAA01M10","AAA04M00"};
		for (String str : etc) {
			logger.info("target_table :" + str);
			Tableinfo aa = new Tableinfo();
			aa.setT_name(str);
			targetList.add(aa);
		}

		// 3.대상 정보를 로그에 기록한다. 추후 테스트 검증을 위해..

		// 4.미지원테이블 목록을 가지고 골디락스용 테이블 생성 쿼리를 만든다.
		// 현재 단계에서 할지, 아님 infoManager에서 생성쿼리를 실행 할지 판단 필요- 현재 infomanager에서 작업 중

		// 5.미지원 테이블 생성,어케 할까?

		// 6.일단 현재 만든 blob이관 함수를 기준으로 다른 유형도 함께 처리가 가능하도록 변경한다.
		// 테이블 명만 가지고 컬럼 정보 체크 하면서, insert sql 자동화 필요
		// for(Tableinfo ti : targetList)

		for (Tableinfo ti : targetList) {

			target_table = ti.getT_name();
			fnGetColumnListORA(ti.getT_name());
			logger.info(ti.getT_name() + "TOTAL COUNT:" + fnGetTableCount(ti.getT_name()));
			stopWatch.start();
			// fnOraToGoldiDirect(ti.t_name);
			// fnOraToGoldiDirect2(ti.t_name);
			// thread job
			
			
			
			  List<Future<String>> futures = new ArrayList<>(); ThreadPoolExecutor
			  threadPool = new ThreadPoolExecutor(2, 10, 10, TimeUnit.SECONDS, new
			  SynchronousQueue<Runnable>()); for (int i = 0; i < 8; i++) {
			  threadPool.submit(new Consumer()); }
			 
			Producer run_producer = new Producer(con_ora,ti.getT_name());
			run_producer.run();
			
			//int rtn = fnConvertBlobToVar(ti.getT_name());
			// Thread.sleep(5000);
			while (((queue1.size() + queue2.size() + queue3.size() + queue4.size() + queue5.size() + queue6.size()
					+ queue7.size() + queue8.size()) > 0)) {
				logger.info("[queue1 size :" + queue1.size() + "]");
				logger.info("[queue2 size :" + queue2.size() + "]");
				logger.info("[queue3 size :" + queue3.size() + "]");
				logger.info("[queue4 size :" + queue4.size() + "]");
				logger.info("[queue5 size :" + queue5.size() + "]");
				logger.info("[queue6 size :" + queue6.size() + "]");
				logger.info("[queue7 size :" + queue7.size() + "]");
				logger.info("[queue8 size :" + queue8.size() + "]");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			threadPool.shutdownNow();
			// logger.info("queue size :"+queue.size());

			for (int j = 0; j < 5; j++) {

				logger.info("thread wait 2sec");
				Thread.sleep(2000);
				// threadPool.shutdown();
			}

			stopWatch.stop();
			logger.info("during time:" + stopWatch.toString());
			stopWatch.reset();
			colList.clear();
		}

		//
		// logger.info("여기 온건가?");
		con_ora.close();
		con_goldi.close();
		tableList.clear();
		colList.clear();
		idxList.clear();
		pkList.clear();
		curRows = 0;

		logger.info("Work done. Check your report.");
		// System.out.println("Work done. Check your report.");
		System.exit(0);
	}

	public static int fnGetTableCount(String t_name) throws Exception {
		PreparedStatement pstmt = con_ora.prepareStatement("SELECT count(*) from " + t_name + "");
		ResultSet rs = pstmt.executeQuery();
		int rowCount = 0;
		while (rs.next()) {
			rowCount = Integer.parseInt(rs.getString("count(*)"));
		}
		logger.info(t_name + " count:" + rowCount);
		rs.close();
		pstmt.close();
		return rowCount;
	}

	public static void fnGetColumnListORA(String t_name) throws Exception {
		// System.out.printf("Table query : %s \n",t_name);
		logger.info("colinfo:target table" + t_name);
		PreparedStatement pstmt = con_ora.prepareStatement(
				"select A.COLUMN_ID, A.COLUMN_NAME, A.DATA_TYPE, A.DATA_LENGTH, A.DATA_DEFAULT, A.DATA_PRECISION, A.NULLABLE, B.COMMENTS"
						+ " from USER_TAB_COLUMNS A, USER_COL_COMMENTS B"
						+ " WHERE A.TABLE_NAME = B.TABLE_NAME AND A.COLUMN_NAME = B.COLUMN_NAME"
						+ " AND A.TABLE_NAME = UPPER(?) ORDER BY A.COLUMN_ID");

		pstmt.setString(1, t_name.trim());

		ResultSet rs = pstmt.executeQuery();
		// if(rs.wasNull()) { logger.info("result:0 traged");}
		// logger.info("col_info income");

		while (rs.next()) {
			Columninfo c_info = new Columninfo();
			c_info.setCol_id(rs.getInt("COLUMN_ID"));
			c_info.setCol_name(rs.getString("COLUMN_NAME"));
			// logger.info(c_info.getCol_name());
			c_info.setCol_dtype(rs.getString("DATA_TYPE"));
			c_info.setCol_dlength(rs.getInt("DATA_LENGTH"));
			// System.out.println(rs.getString(5));
			Long l_data = rs.getLong("DATA_LENGTH");

			String sample = l_data.toString();
			if (sample.isEmpty()) {
				c_info.setCol_default("");
			} else {
				c_info.setCol_default(sample);
			}

			c_info.setCol_pre(rs.getString("DATA_PRECISION"));
			c_info.setCol_null(rs.getString("NULLABLE"));
			c_info.setCol_comments(rs.getString("COMMENTS"));
			colList.add(c_info);
			logger.info(
					"colinfo:" + c_info.getCol_name() + "|" + c_info.getCol_dtype() + "|" + c_info.getCol_dlength());
		}
		rs.close();
		pstmt.close();
	}

	// 테이블리스트 받아와서 처리 하는걸로 바꿔서 미사용
	public static void fnSelectTargetTableList(String t_name) {
		// System.out.printf("Table query : %s \n",t_name);
		try {
			PreparedStatement pstmt = con_ora.prepareStatement(
					"select A.COLUMN_ID, A.COLUMN_NAME, A.DATA_TYPE, A.DATA_LENGTH, A.DATA_DEFAULT, A.DATA_PRECISION, A.NULLABLE, B.COMMENTS"
							+ " from USER_TAB_COLUMNS A, USER_COL_COMMENTS B"
							+ " WHERE A.TABLE_NAME = B.TABLE_NAME AND A.COLUMN_NAME = B.COLUMN_NAME"
							+ " AND A.TABLE_NAME = UPPER(?) ORDER BY A.COLUMN_ID");

			pstmt.setString(1, t_name);
			ResultSet rs = pstmt.executeQuery();

			while (rs.next()) {
				Columninfo c_info = new Columninfo();
				c_info.setCol_id(rs.getInt("COLUMN_ID"));
				c_info.setCol_name(rs.getString("COLUMN_NAME"));
				c_info.setCol_dtype(rs.getString("DATA_TYPE"));
				// BLOB, CLOB
				if (c_info.getCol_dtype().equals("BLOB") || c_info.getCol_dtype().equals("CLOB")
						|| c_info.getCol_dtype().equals("LONG") || c_info.getCol_dtype().equals("NVARCHAR2")
						|| c_info.getCol_dtype().equals("RAW") || c_info.getCol_dtype().equals("XMLTYPE")) {
					Tableinfo aa = new Tableinfo();
					aa.setT_name(t_name);
					targetList.add(aa);
					break;
				}
			}
			rs.close();
			pstmt.close();
		} catch (Exception e) {
			logger.error("error :", e);
		} finally {
		}

	}

	// 테스트를 위한 코드 후에 지워버릴 함수
	public static void fnTestInputOra() throws SQLException, FileNotFoundException {
		PreparedStatement pstmt1 = con_ora.prepareStatement(
				"INSERT INTO AAA01M10 (ACNT_NO,SUB_NO,SELF_TP,CLS_DTM,IDNO,REGI_DT,SGN_IMG,BIT_SZ) VALUES (?,?,?,?,'','',?,0)");
		pstmt1.setString(1, "9990011111");
		pstmt1.setString(2, "88");
		pstmt1.setString(3, "Y");
		Date d = new Date();
		pstmt1.setDate(4, java.sql.Date.valueOf("2020-12-09"));
		;

		FileInputStream fi = new FileInputStream("111.png");
		// BLOB b = null;
		// oracle.sql.BLOB bol = (oracle.sql.BLOB) emptyBlob;
		pstmt1.setBlob(5, fi);

		int rs = pstmt1.executeUpdate();

		pstmt1.close();
		// PreparedStatement pstmt2 = con_goldi.prepareStatement("INSERT INTO AAA01M10
		// VALUES (?,?,?,?)");
		// Boolean result = pstmt2.execute();

	}

	public static void fnTestInputGoldi() throws SQLException, FileNotFoundException {
		PreparedStatement pstmt1 = con_goldi
				.prepareStatement("INSERT INTO AAA01M10 (ACNT_NO,SUB_NO,SELF_TP,SGN_IMG,BIT_SZ) VALUES (?,?,?,?,?)");
		pstmt1.setString(1, "9990011111");
		pstmt1.setString(2, "88");
		pstmt1.setString(3, "Y");
		// Date d = new Date();
		// pstmt1.setDate(4, java.sql.Date.valueOf("2020-12-09"));;
		File file = new File("test1.PNG");
		FileInputStream fi = new FileInputStream(file);

		pstmt1.setBinaryStream(4, fi);
		pstmt1.setLong(5, file.length());

		pstmt1.executeUpdate();
		pstmt1.close();
		// PreparedStatement pstmt2 = con_goldi.prepareStatement("INSERT INTO AAA01M10
		// VALUES (?,?,?,?)");
		// Boolean result = pstmt2.execute();

	}

	public static void fnSelectBlobtoimg() {
		String select_qry = "SELECT BIT_SZ,FILE_NM,SGN_IMG FROM AAA01M10 where rownum=1";
		int BUFFER_SIZE = 102400;
		try {
			PreparedStatement pstmt1 = con_ora.prepareStatement(select_qry);
			ResultSet rs = pstmt1.executeQuery();
			while (rs.next()) {
				int sz = rs.getInt(1);
				String nm = rs.getString(2);
				oracle.sql.BLOB bol = (BLOB) rs.getBlob(3);
				// input stream.. BOLB구조랑 동일한 형태일것이라 이거 사용하면 정상적인 binary못가져올거 같다.
				// 실제 이미지의 binary만 가져와야 이미지화 할수 있을듯 하다. 역직렬화 필요.
				BufferedInputStream binstr = new BufferedInputStream(bol.getBinaryStream());
				logger.info(binstr);
				logger.info("---------------------------------------------");
				OutputStream ostr = new FileOutputStream(nm);

				logger.info("---------------------------------------------");
				int size = bol.getBufferSize();
				logger.info("buffer_size=" + size);
				logger.info("bit_sz :" + sz);

				int bytesRead = -1;
				int j = 0;
				byte[] buffer = new byte[BUFFER_SIZE];
				while ((bytesRead = binstr.read(buffer)) != -1) {
					logger.info(j++);
					ostr.write(buffer, 0, bytesRead);
				}

			}

		} catch (SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	// 오라클SELECT 후, RESULTSET에서 건건히 변환해서 인서트 처리
	public static void fnOraToGoldiDirect(String t_name) throws IOException {

		// 조회된 컬럼 정보 가지고 SQL문 만듬. 간혹 골디락스에서 사용이 어려운 컬럼명이 있어서 ""로 컬럼명을 감쌈
		// yyyy/MM/dd hh24:mm:ss
		String select_qry = "";
		String insert_qry = "";
		String insert_dynqry = "";
		int rowcnt = 0;
		select_qry += "SELECT ";
		insert_qry += "INSERT INTO " + t_name + " (";
		// 컬럼갯수 확인
		int col_cnt = colList.size();
		for (Columninfo ci : colList) {
			select_qry += ci.getCol_name() + ", ";
			insert_qry += "\"" + ci.getCol_name() + "\", ";
			insert_dynqry += "?, ";
		}

		select_qry = select_qry.substring(0, select_qry.length() - 2);
		select_qry += " from " + t_name;
		insert_qry = insert_qry.substring(0, insert_qry.length() - 2);
		insert_dynqry = insert_dynqry.substring(0, insert_dynqry.length() - 2);
		insert_qry += ") VALUES (" + insert_dynqry + ")";

		logger.info("ORACLE SQL:" + select_qry);
		logger.info("GOLDILOCKS SQL:" + insert_qry);
		logger.info("Start: select and insert : " + t_name);
		try {
			PreparedStatement pstmt1 = con_ora.prepareStatement(select_qry);
			ResultSet rs = pstmt1.executeQuery();
			rs.setFetchSize(fetch_rows);

			PreparedStatement pstmt2 = con_goldi.prepareStatement(insert_qry);
			ResultSetMetaData rsmd = rs.getMetaData();

			int q_direct = 1;
			int cnt = 0;
			String err_sample = "";
			while (rs.next()) {
				for (int i = 1; i <= col_cnt; i++) {
					String dType = colList.get(i - 1).getCol_dtype();
					switch (dType) {

					case "VARCHAR2":
						pstmt2.setString(i, rs.getString(i));
						err_sample += rs.getString(i) + ":";
						break;
					case "VARCHAR":
						pstmt2.setString(i, rs.getString(i));
						break;
					case "INT":
						pstmt2.setInt(i, rs.getInt(i));
						break;
					case "CHAR":
						pstmt2.setString(i, rs.getString(i));
						break;
					case "FLOAT":
						pstmt2.setFloat(i, rs.getFloat(i));
						break;
					case "NUMBER":
						pstmt2.setBigDecimal(i, rs.getBigDecimal(i));
						break;
					case "DATE":
						pstmt2.setTimestamp(i, rs.getTimestamp(i));
					case "TIMESTAMP(6)":
						SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
						if (rs.wasNull()) {
							String aa = null;
							pstmt2.setTimestamp(i, null);
						} else {
							pstmt2.setTimestamp(i, rs.getTimestamp(i));
						}

						break;
					case "NVARCHAR2":
						pstmt2.setString(i, rs.getString(i));
						break;
					case "LONG":
						pstmt2.setLong(i, rs.getLong(i));
						break;
					case "CLOB":
						// logger.info("CLOB:"+rsmd.getColumnType(i));
						oracle.sql.CLOB clo = (CLOB) rs.getClob(i);
						try {
							if (clo != null) {
								Reader csr = clo.getCharacterStream();
								pstmt2.setCharacterStream(i, csr, clo.getLength());
							} else {
								pstmt2.setObject(i, null);
							}
						} catch (Exception e) {
							logger.info("CLOB error:" + e.toString());
						}
						break;
					case "BLOB":
						// logger.info("BLOB:"+rsmd.getColumnType(i));
						oracle.sql.BLOB bol = (BLOB) rs.getBlob(i);
						try {
							InputStream binstr = bol.getBinaryStream();
							// pstmt2.setBinaryStream(i, binstr, binstr.available());
							pstmt2.setBinaryStream(i, binstr);
						} catch (Exception e) {
							logger.info("BLOB error");
						}
						break;
					case "XMLTYPE":
						logger.info("XMLTYPE:" + rsmd.getColumnType(i) + "Col:[" + i + "," + rowcnt + "]");
						pstmt2.setString(i, rs.getString(i));
						break;
					case "RAW":
						InputStream binstr2 = rs.getBinaryStream(i);
						pstmt2.setBinaryStream(i, binstr2);
						break;
					}

				}
				try {
					// pstmt2.addBatch();
					// rowcnt +=1;
					// 건건이 수행으로 변경
					pstmt2.execute();
					rowcnt += 1;

				} catch (Exception e) {
					logger.info("addbatch err:" + e.toString());
					logger.info("error_varchar value :" + err_sample);
					rowcnt -= 1;

				} finally {
					err_sample = "";
				}
				// pstmt2.executeUpdate();
				if ((rowcnt % 50) == 0) {
					// logger.info("Thread["+n+"]Batch rowcnt:" +rowcnt);
					// pstmt2.executeBatch();
					con_goldi.commit();
					// process_cnt += 5000;
				}

			}
			// pstmt2.executeBatch();
			con_goldi.commit();
			pstmt1.close();
			pstmt2.close();
			logger.info("one table done!!!");
		} catch (SQLException e) {
			logger.info(e);
		}
	}

	// 오라클SELECT 후, reflection이용하여 parameter bind 할때 함수 호출로 변경, 속도 개선여부 확인 varchar만
	// 테스트해서 5%성능향상. 의미 없을지도..
	public static void fnOraToGoldiDirect2(String t_name)
			throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException,
			NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException {

		// 조회된 컬럼 정보 가지고 SQL문 만듬. 간혹 골디락스에서 사용이 어려운 컬럼명이 있어서 ""로 컬럼명을 감쌈
		// yyyy/MM/dd hh24:mm:ss
		String select_qry = "";
		String insert_qry = "";
		String insert_dynqry = "";
		int rowcnt = 0;
		select_qry += "SELECT ";
		insert_qry += "INSERT INTO " + t_name + " (";
		// 컬럼갯수 확인
		int col_cnt = colList.size();
		for (Columninfo ci : colList) {
			select_qry += ci.getCol_name() + ", ";
			insert_qry += "\"" + ci.getCol_name() + "\", ";
			insert_dynqry += "?, ";
		}

		select_qry = select_qry.substring(0, select_qry.length() - 2);
		select_qry += " from " + t_name;
		insert_qry = insert_qry.substring(0, insert_qry.length() - 2);
		insert_dynqry = insert_dynqry.substring(0, insert_dynqry.length() - 2);
		insert_qry += ") VALUES (" + insert_dynqry + ")";

		logger.info("ORACLE SQL:" + select_qry);
		logger.info("GOLDILOCKS SQL:" + insert_qry);
		logger.info("Start: select and insert : " + t_name);
		try {
			PreparedStatement pstmt1 = con_ora.prepareStatement(select_qry);
			ResultSet rs = pstmt1.executeQuery();
			rs.setFetchSize(fetch_rows);

			PreparedStatement pstmt2 = con_goldi.prepareStatement(insert_qry);
			ResultSetMetaData rsmd = rs.getMetaData();

			// reflection 배열함수
			String[] call_func = new String[col_cnt]; // col 갯수만큼
			for (int i = 1; i <= col_cnt; i++) {
				String colType = colList.get(i - 1).getCol_dtype();
				switch (colType) {
				case "VARCHAR2":
					call_func[i - 1] = "cFnVarchar";
					break;
				case "VARCHAR":
					call_func[i - 1] = "cFnVarchar";
					break;
				case "INT":
					call_func[i - 1] = "cFnInt";
					break;
				case "CHAR":
					call_func[i - 1] = "cFnVarchar";
					break;
				case "FLOAT":
					call_func[i - 1] = "cFnFloat";
					break;
				case "NUMBER":
					call_func[i - 1] = "cFnBigDeci";
					break;
				case "DATE":
					call_func[i - 1] = "cFnTimeStamp";
				case "TIMESTAMP(6)":
					call_func[i - 1] = "cFnTimeStamp";
					break;
				case "NVARCHAR2":
					call_func[i - 1] = "cFnVarchar";
					break;
				case "LONG":
					call_func[i - 1] = "cFnLong";
					break;
				case "CLOB":
					call_func[i - 1] = "cFnCLOB";
					break;
				case "BLOB":
					call_func[i - 1] = "cFnBLOB";
					break;
				case "XMLTYPE":
					call_func[i - 1] = "cFnVarchar";
					break;
				case "RAW":
					call_func[i - 1] = "cFnRaw";
					break;
				}
			}

			int q_direct = 1;
			int cnt = 0;
			String err_sample = "";
			// reflectforeach

			Class cls = Class.forName("dbinfo.sunjesoft.ReflectClass");
			Object obj = cls.newInstance();

			Class[] param = new Class[3];
			param[0] = int.class;
			param[1] = ResultSet.class;
			param[2] = PreparedStatement.class;

			while (rs.next()) {
				int idx = 1;
				for (String str : call_func) {

					Method method = cls.getDeclaredMethod(str, param);
					method.invoke(obj, idx, rs, pstmt2);
					idx++;
				}
				try {
					// pstmt2.addBatch();
					// rowcnt +=1;
					// 건건이 수행으로 변경
					pstmt2.execute();
					rowcnt += 1;

				} catch (Exception e) {
					logger.info("addbatch err:" + e.toString());
					logger.info("error_varchar value :" + err_sample);
					rowcnt -= 1;

				} finally {
					err_sample = "";
				}
				// pstmt2.executeUpdate();
				if ((rowcnt % 50) == 0) {
					// logger.info("Batch rowcnt:" +rowcnt);
					// pstmt2.executeBatch();
					con_goldi.commit();
					// process_cnt += 5000;
				}

			}
			// pstmt2.executeBatch();
			con_goldi.commit();
			pstmt1.close();
			pstmt2.close();
			logger.info("one table done!!!");
		} catch (SQLException e) {
			logger.info(e);
		}
	}
	//

	//
	public static int fnConvertBlobToVar(String t_name) {
		System.out.println("시작");
		// yyyy/MM/dd hh24:mm:ss
		String select_qry = "";
		String insert_qry = "";
		String insert_dynqry = "";
		int rowcnt = 0;
		select_qry += "SELECT ";
		insert_qry += "INSERT INTO " + t_name + " (";
		// 컬럼갯수 확인
		int col_cnt = colList.size();
		for (Columninfo ci : colList) {
			select_qry += ci.getCol_name() + ", ";
			insert_qry += ci.getCol_name() + ", ";
			insert_dynqry += "?, ";
		}
		select_qry = select_qry.substring(0, select_qry.length() - 2);
		select_qry += " from " + t_name;

		insert_qry = insert_qry.substring(0, insert_qry.length() - 2);
		insert_dynqry = insert_dynqry.substring(0, insert_dynqry.length() - 2);
		insert_qry += ") VALUES (" + insert_dynqry + ")";

		logger.info("ORACLE SQL:" + select_qry);
		logger.info("GOLDILOCKS SQL:" + insert_qry);
		logger.info("Start: select and insert : " + t_name);
		// oracle 조회(BLOB)
		// stopWatch.start();

		// logger.info("Select ORA data start at :"+start);
		try {
			PreparedStatement pstmt1 = con_ora.prepareStatement(select_qry);
			ResultSet rs = pstmt1.executeQuery();
			rs.setFetchSize(fetch_rows);
			// goldi insert문
			// stopWatch.stop();
			// logger.info("Select ORA prepare->execute :"+stopWatch.toString());
			// stopWatch.reset();
			// PreparedStatement pstmt2 = con_goldi.prepareStatement(insert_qry);
			// 컬럼갯수 필요
			// stopWatch.start();
			ResultSetMetaData rsmd = rs.getMetaData();
			int q_direct = 1;
			int cnt = 0;
			while (rs.next()) {

				// stopWatch.stop();
				// logger.info("Select ORA rsset 1 cnt ");
				// stopWatch.reset();
				int numColumns = rsmd.getColumnCount();
				JsonObject obj = new JsonObject();
				// System.out.println(numColumns);
				for (int i = 1; i <= numColumns; i++) {
					String column_name = rsmd.getColumnName(i);

					// if(rsmd.getColumnType(i)==java.sql.Types.ARRAY){
					// obj.addProperty(column_name, (Number) rs.getArray(column_name));
					// }
					if (rsmd.getColumnType(i) == java.sql.Types.BIGINT) {
						obj.addProperty(column_name, rs.getInt(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.BOOLEAN) {
						obj.addProperty(column_name, rs.getBoolean(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.NUMERIC) {
						obj.addProperty(column_name, rs.getBigDecimal(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.BLOB) {
						if (!rs.wasNull()) {
							Blob bol = rs.getBlob(column_name);
							InputStream binstr = bol.getBinaryStream();
							try {
								obj.addProperty(column_name, fnConvInputStreamToString(binstr));
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						} else {
							String snul = null;
							obj.addProperty(column_name, snul);
						}
					} else if (rsmd.getColumnType(i) == java.sql.Types.DOUBLE) {
						obj.addProperty(column_name, rs.getDouble(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.FLOAT) {
						obj.addProperty(column_name, rs.getFloat(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.INTEGER) {
						obj.addProperty(column_name, rs.getInt(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.NVARCHAR) {
						obj.addProperty(column_name, rs.getNString(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.VARCHAR) {
						obj.addProperty(column_name, rs.getString(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.TINYINT) {
						obj.addProperty(column_name, rs.getInt(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.SMALLINT) {
						obj.addProperty(column_name, rs.getInt(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.DATE) {
						SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
						String date_info = dateformat.format(rs.getDate(column_name));
//				         logger.info(date_info);
						obj.addProperty(column_name, date_info);
					} else if (rsmd.getColumnType(i) == java.sql.Types.TIMESTAMP) {

						SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
						if (rs.wasNull()) {
							String aa = null;
							obj.addProperty(column_name, aa);
						} else {
							obj.addProperty(column_name, dateformat.format(rs.getTimestamp(column_name)));
						}
					} else {
						// logger.info(column_name);
						obj.addProperty(column_name, rs.getString(column_name));
					}
				}
				cnt += 1;
				// logger.info(obj.toString());
				if (q_direct == 1) {
					// queue1.add(obj);
					queue1.offer(obj, 2000, TimeUnit.MILLISECONDS);
					if ((cnt % 1000) == 0) {
						q_direct = 2;
					}
				} else if (q_direct == 2) {
					queue2.offer(obj, 2000, TimeUnit.MILLISECONDS);
					if ((cnt % 1000) == 0) {
						q_direct = 3;
					}
				} else if (q_direct == 3) {
					queue3.offer(obj, 2000, TimeUnit.MILLISECONDS);
					if ((cnt % 1000) == 0) {
						q_direct = 4;
					}
				} else if (q_direct == 4) {
					queue4.offer(obj, 2000, TimeUnit.MILLISECONDS);
					if ((cnt % 1000) == 0) {
						q_direct = 5;
					}
				} else if (q_direct == 5) {
					queue5.offer(obj, 2000, TimeUnit.MILLISECONDS);
					if ((cnt % 1000) == 0) {
						q_direct = 6;
					}
				} else if (q_direct == 6) {
					queue6.offer(obj, 2000, TimeUnit.MILLISECONDS);
					if ((cnt % 1000) == 0) {
						q_direct = 7;
					}
				} else if (q_direct == 7) {
					queue7.offer(obj, 2000, TimeUnit.MILLISECONDS);
					if ((cnt % 1000) == 0) {
						q_direct = 8;
					}
				} else if (q_direct == 8) {
					queue8.offer(obj, 2000, TimeUnit.MILLISECONDS);
					if ((cnt % 1000) == 0) {
						q_direct = 1;
					}
				}
			}

			// con_goldi.commit();

			pstmt1.close();

		} catch (Exception ex) {
			stopWatch.stop();
			stopWatch.reset();
			logger.error("error:", ex);
			return 0;
		}
		return 1;
	}

	public static void fnGetTableList(Connection con) throws Exception {

		PreparedStatement pstmt = con.prepareStatement("SELECT TABLE_SCHEMA, TABLE_NAME,TABLESPACE_NAME FROM tabs");
		ResultSet rs = pstmt.executeQuery();

		while (rs.next()) {
			Tableinfo t_info = new Tableinfo();

			t_info.setT_schema(rs.getString("TABLE_SCHEMA"));
			t_info.setT_name(rs.getString("TABLE_NAME"));
			t_info.setTbs_name(rs.getString("TABLESPACE_NAME"));

			tableList.add(t_info);
			// System.out.print(t_info.getOwner() +":"+ t_info.getT_schema() + ":" +
			// t_info.t_name +":"+ t_info.tbs_name+"\n");
		}
		rs.close();
	}

	public static void fnGetTableListORA(Connection con) throws Exception {

		PreparedStatement pstmt = con.prepareStatement("SELECT TABLE_NAME, TABLESPACE_NAME FROM tabs");
		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			Tableinfo t_info = new Tableinfo();
			t_info.setT_name(rs.getString("TABLE_NAME"));
			t_info.setTbs_name(rs.getString("TABLESPACE_NAME"));

			tableList.add(t_info);
			// System.out.print(t_info.getOwner() +":"+ t_info.getT_schema() + ":" +
			// t_info.t_name +":"+ t_info.tbs_name+"\n");
		}
		rs.close();
	}

	public static int fnGetTableCount(Connection con, String t_name) throws Exception {
		PreparedStatement pstmt = con.prepareStatement("SELECT count(*) from " + t_name + "");
		ResultSet rs = pstmt.executeQuery();
		int rowCount = 0;
		while (rs.next()) {
			rowCount = Integer.parseInt(rs.getString("count(*)"));
		}

		rs.close();
		return rowCount;
	}

	public static void fnGetTableList(Connection con, String t_name) throws Exception {
		PreparedStatement pstmt = con.prepareStatement(
				"select A.COLUMN_ID, A.COLUMN_NAME, A.DATA_TYPE, A.DATA_LENGTH, A.DATA_PRECISION, A.NULLABLE, A.DATA_DEFAULT, B.COMMENTS"
						+ " from USER_TAB_COLUMNS A, USER_COL_COMMENTS B"
						+ " WHERE  A.TABLE_SCHEMA = B.TABLE_SCHEMA AND A.TABLE_NAME = B.TABLE_NAME AND A.COLUMN_NAME = B.COLUMN_NAME"
						+ " AND A.TABLE_NAME = (?)");

		pstmt.setString(1, t_name);
		ResultSet rs = pstmt.executeQuery();

		while (rs.next()) {
			Columninfo c_info = new Columninfo();
			c_info.setCol_id(rs.getInt("COLUMN_ID"));
			c_info.setCol_name(rs.getString("COLUMN_NAME"));
			c_info.setCol_dtype(rs.getString("DATA_TYPE"));
			c_info.setCol_dlength(rs.getInt("DATA_LENGTH"));
			c_info.setCol_pre(rs.getString("DATA_PRECISION"));
			c_info.setCol_null(rs.getString("NULLABLE"));
			c_info.setCol_default(rs.getString("DATA_DEFAULT"));
			c_info.setCol_comments(rs.getString("COMMENTS"));
			colList.add(c_info);
			// System.out.printf("{%s}, {%s},{%s}
			// \n",c_info.getCol_name(),c_info.getCol_dtype(),c_info.getCol_dlength());
		}
		rs.close();
	}

	public static void fnGetTableListORA(Connection con, String t_name) throws Exception {
		System.out.printf("Table query : %s \n", t_name);

		PreparedStatement pstmt = con.prepareStatement(
				"select A.COLUMN_ID, A.COLUMN_NAME, A.DATA_TYPE, A.DATA_LENGTH, A.DATA_PRECISION, A.NULLABLE, B.COMMENTS"
						+ " from USER_TAB_COLUMNS A, USER_COL_COMMENTS B"
						+ " WHERE A.TABLE_NAME = B.TABLE_NAME AND A.COLUMN_NAME = B.COLUMN_NAME"
						+ " AND A.TABLE_NAME = UPPER(?)");

		pstmt.setString(1, t_name);
		ResultSet rs = pstmt.executeQuery();

		while (rs.next()) {
			Columninfo c_info = new Columninfo();
			c_info.setCol_id(rs.getInt("COLUMN_ID"));
			c_info.setCol_name(rs.getString("COLUMN_NAME"));
			c_info.setCol_dtype(rs.getString("DATA_TYPE"));
			c_info.setCol_dlength(rs.getInt("DATA_LENGTH"));
			c_info.setCol_pre(rs.getString("DATA_PRECISION"));
			c_info.setCol_null(rs.getString("NULLABLE"));
			c_info.setCol_comments(rs.getString("COMMENTS"));
			colList.add(c_info);
			// System.out.printf("{%s}, {%s},{%s}
			// \n",c_info.getCol_name(),c_info.getCol_dtype(),c_info.getCol_dlength());
		}
		rs.close();
	}

	public static String fnConvInputStreamToString(InputStream st) throws IOException {
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		byte[] buffer = new byte[1024];
		int length;
		while ((length = st.read(buffer)) != -1) {
			result.write(buffer, 0, length);
		}
		// StandardCharsets.UTF_8.name() > JDK 7
		return result.toString("UTF-8");
		// return "";
	}

	public static ByteArrayOutputStream fnConvInputStreamToByteOut(InputStream st) throws IOException {
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		byte[] buffer = new byte[1024];
		int length;
		while ((length = st.read(buffer)) != -1) {
			result.write(buffer, 0, length);
		}
		// StandardCharsets.UTF_8.name() > JDK 7
		return result;
		// return "";
	}

	public static class Producer implements Runnable{

		Connection ora_handle; 
		String t_name;
		public Producer(Connection con, String t_name){
			this.ora_handle = con;
			this.t_name = t_name;
		}
		@Override
		public void run() {
			// TODO Auto-generated method stub
			System.out.println("시작");
			//yyyy/MM/dd hh24:mm:ss
			String select_qry = "";
			String insert_qry = "";
			String insert_dynqry = "";
			int rowcnt = 0;
			select_qry += "SELECT ";
			insert_qry += "INSERT INTO "+t_name + " (";
			//컬럼갯수 확인
			int col_cnt = colList.size();
			for( Columninfo ci :colList)
			{
				select_qry += ci.getCol_name()+", ";
				insert_qry += ci.getCol_name()+", ";
				insert_dynqry += "?, ";
			}
			select_qry = select_qry.substring(0,select_qry.length() -2);
			select_qry += " from "+t_name;
			
			insert_qry = insert_qry.substring(0,insert_qry.length() -2);
			insert_dynqry = insert_dynqry.substring(0,insert_dynqry.length() -2);
			insert_qry +=") VALUES (" +insert_dynqry + ")";
			
			logger.info("ORACLE SQL:"+ select_qry);
			logger.info("GOLDILOCKS SQL:"+ insert_qry);
			logger.info("Start: select and insert : "+t_name);
			//oracle 조회(BLOB)
			//stopWatch.start();
			
			//logger.info("Select ORA data start at :"+start);
			try
			{
				PreparedStatement pstmt1 = ora_handle.prepareStatement(select_qry);
				ResultSet rs = pstmt1.executeQuery();
				rs.setFetchSize(fetch_rows);
						//goldi insert문
				//stopWatch.stop();
				//logger.info("Select ORA prepare->execute :"+stopWatch.toString());	
				//stopWatch.reset();
				//PreparedStatement pstmt2 = con_goldi.prepareStatement(insert_qry);
				//컬럼갯수 필요
				//stopWatch.start();
				ResultSetMetaData rsmd = rs.getMetaData();
				int q_direct = 1;
				int cnt = 0;
				while(rs.next())
				{
					
					//stopWatch.stop();
					//logger.info("Select ORA rsset 1 cnt ");
					//stopWatch.reset();
					int numColumns = rsmd.getColumnCount();
					JsonObject obj = new JsonObject();
					//System.out.println(numColumns);
				    for (int i=1; i<=numColumns; i++) {
				        String column_name = rsmd.getColumnName(i);
				        
				        //if(rsmd.getColumnType(i)==java.sql.Types.ARRAY){
				        // obj.addProperty(column_name, (Number) rs.getArray(column_name));
				        //}
				        if(rsmd.getColumnType(i)==java.sql.Types.BIGINT){
				         obj.addProperty(column_name, rs.getInt(column_name));
				        }
				        else if(rsmd.getColumnType(i)==java.sql.Types.BOOLEAN){
				         obj.addProperty(column_name, rs.getBoolean(column_name));
				        }
				        else if(rsmd.getColumnType(i)==java.sql.Types.NUMERIC){
					         obj.addProperty(column_name, rs.getBigDecimal(column_name));
					     }
				        else if(rsmd.getColumnType(i)==java.sql.Types.BLOB){
				        	if(!rs.wasNull())
				        	{
					         Blob bol = rs.getBlob(column_name);
							 InputStream binstr = bol.getBinaryStream();
						         try {
									obj.addProperty(column_name, fnConvInputStreamToString(binstr));
						         	} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
						         	}
				        	}
				        	else
				        	{
				        		String snul = null;
				        		obj.addProperty(column_name, snul); 
				        	}
				        }
				        else if(rsmd.getColumnType(i)==java.sql.Types.DOUBLE){
				         obj.addProperty(column_name, rs.getDouble(column_name)); 
				        }
				        else if(rsmd.getColumnType(i)==java.sql.Types.FLOAT){
				         obj.addProperty(column_name, rs.getFloat(column_name));
				        }
				        else if(rsmd.getColumnType(i)==java.sql.Types.INTEGER){
				         obj.addProperty(column_name, rs.getInt(column_name));
				        }
				        else if(rsmd.getColumnType(i)==java.sql.Types.NVARCHAR){
				         obj.addProperty(column_name, rs.getNString(column_name));
				        }
				        else if(rsmd.getColumnType(i)==java.sql.Types.VARCHAR){
				         obj.addProperty(column_name, rs.getString(column_name));
				        }
				        else if(rsmd.getColumnType(i)==java.sql.Types.TINYINT){
				         obj.addProperty(column_name, rs.getInt(column_name));
				        }
				        else if(rsmd.getColumnType(i)==java.sql.Types.SMALLINT){
				         obj.addProperty(column_name, rs.getInt(column_name));
				        }
				        else if(rsmd.getColumnType(i)==java.sql.Types.DATE){
					         SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
					         String date_info = dateformat.format(rs.getDate(column_name));
//					         logger.info(date_info);
					         obj.addProperty(column_name, date_info);
				        }
				        else if(rsmd.getColumnType(i)==java.sql.Types.TIMESTAMP){
				        	
				        	SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
				        	if(rs.wasNull())
				        	{
				        		String aa = null;
				        		obj.addProperty(column_name, aa);
				        	}
				        	else
				        	{
				        		obj.addProperty(column_name, dateformat.format(rs.getTimestamp(column_name)));
				        	}
				        }
				        else{
		//		        	logger.info(column_name);
				         obj.addProperty(column_name, rs.getString(column_name));
				        }
				    }
				    cnt +=1;
				    //logger.info(obj.toString());
				    if(q_direct == 1)
				    {
				    	//queue1.add(obj);
				    	queue1.offer(obj);
				    	//queue1.offer(obj,2000,TimeUnit.MILLISECONDS);
				    	if((cnt % 1000) == 0)
				    	{
				    		q_direct = 2;
				    	}
				    }
				    else if(q_direct == 2)
				    {
				    	queue2.offer(obj);
				    	//queue2.offer(obj,2000,TimeUnit.MILLISECONDS);
				    	if((cnt % 1000) == 0)
				    	{
				    		q_direct = 3;
				    	}
				    }
				    else if(q_direct == 3)
				    {
				    	queue3.offer(obj);
				    	//queue3.offer(obj,2000,TimeUnit.MILLISECONDS);
				    	if((cnt % 1000) == 0)
				    	{
				    		q_direct = 4;
				    	}
				    }
				    else if(q_direct == 4)
				    {
				    	queue4.offer(obj);
				    	//queue4.offer(obj,2000,TimeUnit.MILLISECONDS);
				    	if((cnt % 1000) == 0)
				    	{
				    		q_direct = 5;
				    	}
				    }
				    else if(q_direct == 5)
				    {
				    	queue5.offer(obj);
				    	//queue5.offer(obj,2000,TimeUnit.MILLISECONDS);
				    	if((cnt % 1000) == 0)
				    	{
				    		q_direct = 6;
				    	}
				    }
				    else if(q_direct == 6)
				    {
				    	queue6.offer(obj);
				    	//queue6.offer(obj,2000,TimeUnit.MILLISECONDS);
				    	if((cnt % 1000) == 0)
				    	{
				    		q_direct = 7;
				    	}
				    }
				    else if(q_direct == 7)
				    {
				    	queue7.offer(obj);
				    	//queue7.offer(obj,2000,TimeUnit.MILLISECONDS);
				    	if((cnt % 1000) == 0)
				    	{
				    		q_direct = 8;
				    	}
				    }
				    else if(q_direct == 8)
				    {
				    	//queue8.offer(obj,2000,TimeUnit.MILLISECONDS);
				    	queue8.offer(obj);
				    	if((cnt % 1000) == 0)
				    	{
				    		q_direct = 1;
				    	}
				    }
				}
				
				//con_goldi.commit();

				pstmt1.close();

			}
			catch(Exception ex)
			{
				stopWatch.stop();
				stopWatch.reset();
				logger.error("error:",ex);
			}
		}
		
	}

	public static class Consumer implements Runnable {

		private volatile boolean running = true;

		public void run() {
			String name = Thread.currentThread().getName();
			System.out.println("Thread " + name + "Start");
			try {
				String URL_NODE1 = properties.getProperty("goldi_url");
				Properties sProp = new Properties();
				sProp.put("user", properties.getProperty("goldi_user"));
				sProp.put("password", properties.getProperty("goldi_password"));
				// sProp.put("locality_aware_transaction","1");
				// sProp.put("locator_file", "location.ini");
				try {
					Class.forName("sunje.goldilocks.jdbc.GoldilocksDriver");
					con_goldi = DriverManager.getConnection(URL_NODE1, sProp);
					con_goldi.setAutoCommit(false);
					logger.info("goldi connected!!");
				} catch (SQLException e) {
					logger.error("error:", e);
				}

				int n = Integer.parseInt(name.substring(name.length() - 1, name.length()));
				logger.info("thread id:"+n);
				String insert_qry = "";
				String insert_dynqry = "";
				int rowcnt = 0;
				int process_cnt = 0;
				insert_qry += "INSERT INTO " + target_table + " (";
				// 컬럼갯수 확인
				int col_cnt = colList.size();
				for (Columninfo ci : colList) {

					insert_qry += ci.getCol_name() + ", ";
					insert_dynqry += "?, ";
				}

				insert_qry = insert_qry.substring(0, insert_qry.length() - 2);
				insert_dynqry = insert_dynqry.substring(0, insert_dynqry.length() - 2);
				insert_qry += ") VALUES (" + insert_dynqry + ")";

				PreparedStatement pstmt2 = con_goldi.prepareStatement(insert_qry);
				LinkedBlockingQueue<JsonObject> queue = null;
				switch (n) {
				case 1:
					queue = queue1;
					break;
				case 2:
					queue = queue2;
					break;
				case 3:
					queue = queue3;
					break;
				case 4:
					queue = queue4;
					break;

				case 5:
					queue = queue5;
					break;
				case 6:
					queue = queue6;
					break;
				case 7:
					queue = queue7;
					break;
				case 8:
					queue = queue8;
					break;

				}

				while (running) {
					JsonObject obj;
					// obj = queue.poll();
					// if((obj = queue.poll()) != null)
					if ((obj = queue.poll()) != null) {
						// System.out.println("Thread " + Thread.currentThread().getName()
						// +"==>"+obj.toString());
						// logger.info("["+n+"] poll");
						for (int i = 1; i <= col_cnt; i++) {
							String dType = colList.get(i - 1).getCol_dtype();
							String col_name = colList.get(i - 1).getCol_name();
							// logger.info(col_name);
							// logger.info(obj.get(colList.get(i-1).getCol_name()).getAsString());
							// logger.info(colList.get(i-1).getCol_name());
							switch (dType) {
							case "VARCHAR2":
							case "VARCHAR":

								if (obj.get(col_name).isJsonNull()) {
									// logger.info("Json is null");
									pstmt2.setString(i, null);
								} else {
									// logger.info("["+n+"]"+obj.get(colList.get(i-1).getCol_name()).getAsString().toString());
									pstmt2.setString(i, obj.get(col_name).getAsString().toString());
								}

								break;
							case "INT":
								// logger.info("["+n+"]"+obj.get(colList.get(i-1).getCol_name()).getAsString().toString());
								if (obj.get(col_name).isJsonNull()) {
									int inul = (Integer) null;
									pstmt2.setInt(i, inul);
								} else {
									pstmt2.setInt(i, obj.get(col_name).getAsInt());
								}

								break;
							case "CHAR":
								// logger.info("["+n+"]"+obj.get(col_name).getAsString().toString());
								if (obj.get(col_name).isJsonNull()) {
									pstmt2.setCharacterStream(i, null);
								} else {
									// obj.get(col_name).getAsCharacter()
									pstmt2.setString(i, obj.get(col_name).getAsString());
								}
								break;
							case "FLOAT":

								if (obj.get(col_name).isJsonNull()) {
									float fnul = (Float) null;
									pstmt2.setFloat(i, fnul);
								} else {
									pstmt2.setFloat(i, obj.get(col_name).getAsFloat());
								}
								break;
							case "NUMBER":

								if (obj.get(col_name).isJsonNull()) {
									pstmt2.setBigDecimal(i, null);
								} else {
									BigDecimal bc = new BigDecimal(obj.get(col_name).getAsString());
									pstmt2.setBigDecimal(i, bc);
								}
								break;
							case "DATE":
								// logger.info("["+n+"]"+obj.get(colList.get(i-1).getCol_name()).getAsString().toString());
								if (obj.get(col_name).isJsonNull()) {
									pstmt2.setTimestamp(i, null);
								} else {
									String str = obj.get(col_name).getAsString();
									java.sql.Timestamp dtt = java.sql.Timestamp.valueOf(str);
									pstmt2.setTimestamp(i, dtt);
								}
								break;
							case "NVARCHAR2":
								// logger.info("["+n+"]"+obj.get(col_name).getAsString().toString());
								if (obj.get(col_name).isJsonNull()) {
									pstmt2.setNString(i, null);
								} else {
									pstmt2.setNString(i, obj.get(col_name).getAsString());
								}
								break;
							case "LONG":
								// logger.info("["+n+"]"+obj.get(col_name).getAsString().toString());
								if (obj.get(col_name).isJsonNull()) {
									String snul = null;
									pstmt2.setString(i, snul);
								} else {
									pstmt2.setString(i, obj.get(col_name).getAsString());
								}

								break;
							/*
							 * case "CLOB" : Clob clo = rs.getClob(i); Reader csr =
							 * clo.getCharacterStream(); pstmt2.setCharacterStream(i, csr); csr.close();
							 * clo.free(); break;
							 */
							case "BLOB":

								if (obj.get(col_name).isJsonNull()) {
									InputStream binstr = null;
									pstmt2.setBinaryStream(i, binstr);
								} else {
									byte[] buff = obj.get(col_name).getAsString().getBytes();
									InputStream binstr = new ByteArrayInputStream(buff);
									pstmt2.setBinaryStream(i, binstr);
									buff = null;
								}

							}
						}
						try {
							// pstmt2.addBatch();
							// pstmt2.executeUpdate();
							pstmt2.execute();
							con_goldi.commit();
							rowcnt += 1;
						} catch (Exception ex) {
							logger.error("error:"+ ex.toString());
							this.stop();
						}
					} else {
						//logger.info("queue size:"+queue.size());
						//logger.info("queue.poll() is null");
						//con_goldi.commit(); 
						//
						 
						// logger.info("["+n+"]["+rowcnt+"]");
						//con_goldi.commit();
					}
					//
				}
				

			} catch (Exception e) {
				System.out.println(e.toString());
				logger.info(e.toString());
			}
			/*
			 * try { con_goldi.close(); } catch (SQLException e) { // TODO Auto-generated
			 * catch block e.printStackTrace(); }
			 */
			try {
				con_goldi.commit();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

		public void stop() {
			logger.info("end");
			running = false;

		}

	}

}
