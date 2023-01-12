package dbinfo.sunjesoft;
import org.apache.commons.lang.StringUtils;
public class Columninfo {

	int col_id;
	String col_name;
	String col_dtype;
	int col_dlength;
	String col_pre;
	String col_null;
	String col_default;
	String col_comments;
	String col_constraint_type;


	
	
	
	public String getCol_constraint_type() {
		return col_constraint_type;
	}

	public void setCol_constraint_type(String col_constraint_type) {
		if(StringUtils.isEmpty(col_constraint_type))
		{
			this.col_constraint_type = "";
		}
		else
		{
			this.col_constraint_type = col_constraint_type;
		}
	}

	public String getCol_default() {
		return col_default;
	}

	public void setCol_default(String col_default) {
		this.col_default = col_default;
	}

	public Columninfo() {
		
	}
	
	public int getCol_id() {
		return col_id;
	}


	public void setCol_id(int col_id) {
		this.col_id = col_id;
	}


	public String getCol_name() {
		return col_name;
	}


	public void setCol_name(String col_name) {
		this.col_name = col_name;
	}


	public String getCol_dtype() {
		return col_dtype;
	}


	public void setCol_dtype(String col_dtype) {
		this.col_dtype = col_dtype;
	}


	public int getCol_dlength() {
		return col_dlength;
	}


	public void setCol_dlength(int col_dlength) {
		this.col_dlength = col_dlength;
	}


	public String getCol_pre() {
		return col_pre;
	}


	public void setCol_pre(String col_pre) {
		this.col_pre = col_pre;
	}


	public String getCol_null() {
		return col_null;
	}


	public void setCol_null(String col_null) {
		this.col_null = col_null;
	}



	public String getCol_comments() {
		return col_comments;
	}


	public void setCol_comments(String col_comments) {
		this.col_comments = col_comments;
	}

}
