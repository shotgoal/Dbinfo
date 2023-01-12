package dbinfo.sunjesoft;

public class Tableinfo {

	String owner;
	String t_schema;
	String t_name;
	String tbs_name;
	int row_cnt;
	


	public Tableinfo()
	{
		
	}
	
	public Tableinfo(String owner, String t_schema, String t_name, String tbs_name)
	{
		this.owner = owner;
		this.t_schema = t_schema;
		this.t_name = t_name;
		this.tbs_name = tbs_name;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getT_schema() {
		return t_schema;
	}

	public void setT_schema(String t_schema) {
		this.t_schema = t_schema;
	}

	public String getT_name() {
		return t_name;
	}

	public void setT_name(String t_name) {
		this.t_name = t_name;
	}

	public String getTbs_name() {
		return tbs_name;
	}

	public void setTbs_name(String tbs_name) {
		this.tbs_name = tbs_name;
	}
	
	public int getRow_cnt() {
		return row_cnt;
	}

	public void setRow_cnt(int row_cnt) {
		this.row_cnt = row_cnt;
	}
}
