package src.rbt.smsbc.model;

public class SmsbcFileVO {
	private int smsbcfileid;
	private int subfileid;
	private String filename;
	private int maxsend;
	private String appserver;
	private String statdate;
	
	public String getFilename() {
		return filename;
	}
	public void setFilename(String filename) {
		this.filename = filename;
	}
	public int getMaxsend() {
		return maxsend;
	}
	public void setMaxsend(int maxsend) {
		this.maxsend = maxsend;
	}
	public int getSmsbcfileid() {
		return smsbcfileid;
	}
	public void setSmsbcfileid(int smsbcfileid) {
		this.smsbcfileid = smsbcfileid;
	}
	public int getSubfileid() {
		return subfileid;
	}
	public void setSubfileid(int subfileid) {
		this.subfileid = subfileid;
	}
	public String getAppserver() {
		return appserver;
	}
	public void setAppserver(String appserver) {
		this.appserver = appserver;
	}
	public String getStatdate() {
		return statdate;
	}
	public void setStatdate(String statdate) {
		this.statdate = statdate;
	}

}
