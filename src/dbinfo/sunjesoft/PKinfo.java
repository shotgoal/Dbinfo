package dbinfo.sunjesoft;

public class PKinfo {

	String pk_name;
	String pk_colname;

	String pk_type;
	String pk_condition;
	String pk_stat;
	
	public String getPk_colname() {
		return pk_colname;
	}
	public void setPk_colname(String pk_colname) {
		this.pk_colname = pk_colname;
	}
	public String getPk_name() {
		return pk_name;
	}
	public void setPk_name(String pk_name) {
		this.pk_name = pk_name;
	}
	public String getPk_type() {
		return pk_type;
	}
	public void setPk_type(String pk_type) {
		this.pk_type = pk_type;
	}
	public String getPk_condition() {
		return pk_condition;
	}
	public void setPk_condition(String pk_condition) {
		this.pk_condition = pk_condition;
	}
	public String getPk_stat() {
		return pk_stat;
	}
	public void setPk_stat(String pk_stat) {
		this.pk_stat = pk_stat;
	}
	
	
}
