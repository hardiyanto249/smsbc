package src.rbt.smsbc.batch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import src.rbt.smsbc.model.SmsbcFileVO;

public class SmsbcScheduler {
	Connection dbConnection;
	PreparedStatement selectFileNameStmt;
	PreparedStatement updateFileNameStmt;
	PreparedStatement selectMsisdnStmt;
	PreparedStatement selectHistorySmsbc;
	private static SimpleDateFormat dateToday;
	private static SimpleDateFormat historyDate;
	private String db_connect_str;
	private String db_passwd;
	private final static String SUFFIX_TEMP = "_smsbc_temp";
	private final static String BLACKLIST_FILE_TEMP = "blacklist_temp";
	private final static String BLACKLIST_FILE = "blacklist";
	private final static String SORTED_FILE = "_sorted";
	private final static String FINAL = "_final";
	private final static String DEFAULT_SERVER = "rbtuser@10.195.30.32:/app1/rbtuser/rbt/push/src/";
	private static Calendar calendar = Calendar.getInstance();
	
	private static String dir_script="";
	private static String dir_source="";
	private static String dir_data="";
	private static int max_loop=0;
	private static int day_history=0;
	private static boolean send_file=true;
	
	private static final Logger log = Logger.getLogger(SmsbcScheduler.class);

	public static void main(String[] args) throws IOException {
		String log4jConfigFile = "./config" + File.separator + "log4j.properties";
		PropertyConfigurator.configure(log4jConfigFile);
		dateToday = new java.text.SimpleDateFormat("yyyyMMdd");
		String targetdate = dateToday.format(calendar.getTime());
		
		SmsbcScheduler sms = new SmsbcScheduler();
		SmsbcFileVO file = new SmsbcFileVO();
		ArrayList<SmsbcFileVO> list = null;
		list = sms.getFileName(targetdate);
		if(list==null || list.size()< 1){
			System.out.println("There is no data to be proccessed");
			System.exit(0);
		}
		calendar.add(calendar.DATE, day_history);
		String histdate = dateToday.format(calendar.getTime());
		sms.processSmsBc(list, file, targetdate, histdate);
	}

	SmsbcScheduler(){
		Properties prop = new Properties();
		FileInputStream stream = null;
		try{
			//ClassLoader loader = Thread.currentThread().getContextClassLoader();           
		    File file = new File("config/smsbc.properties");
			stream = new FileInputStream(file);
			prop.load(stream);
			stream.close();
		}catch(IOException ex){
			ex.printStackTrace();
		}
		dir_script=prop.getProperty("dir_script");
		dir_source=prop.getProperty("dir_source");
		dir_data=prop.getProperty("dir_data");
		max_loop = Integer.parseInt(prop.getProperty("max_loop"));
		day_history = Integer.parseInt(prop.getProperty("day_history"));
		db_connect_str = prop.getProperty("db_connect_str");
		db_passwd = prop.getProperty("db_passwd");
		send_file = Boolean.parseBoolean(prop.getProperty("send_file"));
	}
	
	private String targetServer(String appServer){
		String targetServer = "";
		Properties prop = new Properties();
		FileInputStream stream = null;
		try{
			//ClassLoader loader = Thread.currentThread().getContextClassLoader();           
		    File file = new File("config/smsbc.properties");
			stream = new FileInputStream(file);
			prop.load(stream);
			stream.close();
		}catch(IOException ex){
			ex.printStackTrace();
		}
		targetServer = prop.getProperty("server."+ appServer);
		return targetServer;
	}
	
	private void processSmsBc(ArrayList<SmsbcFileVO> list, SmsbcFileVO file, String targetdate, String histdate) throws IOException{
		String param = "";
		// get file target BC
		for ( int i=0;i<list.size();i++ ){
			file = list.get(i);
			int cnt=i+1;
			System.out.println("============================");
			System.out.println("Processing SMSBC FILE - "+ cnt);
			System.out.println("============================");
			int looping = 1;
			while (looping <= max_loop){
				System.out.print("Proccessing get "+ file.getMaxsend() +" msisdn \t\t\t\t\t ..... ");
				param = dir_source +" "+ dir_data +" "+ targetdate +" "+ file.getMaxsend() +" "+ file.getFilename(); 
				String targetInput = "perl "+ dir_script +"/filterMsisdn.pl target_smsbc "+ param;
				executeCommand(targetInput);
				System.out.println("Done");
				
				// get blacklist
				System.out.print("Processing get blacklist msisdn \t\t\t\t ..... ");
				// sort BlackList File
				String blacklist = "perl "+ dir_script +"/filterMsisdn.pl sort_blacklist";
				File fBL = new File(dir_data +"/"+ BLACKLIST_FILE +".txt");
				if (!fBL.isFile()){
					getMsisdnBlacklist();
					executeCommand(blacklist);				
				}
				System.out.println("Done");
				
				// sort and compare target BC with blacklist
				System.out.print("Processing sort and compare TargetBC with Blacklist \t\t ..... ");
				param = dir_data +" "+ targetdate +" "+ BLACKLIST_FILE +" "+ file.getSubfileid(); 
				String scBlacklist = "perl "+ dir_script +"/filterMsisdn.pl sort_compare_blacklist "+ param;
				executeCommand(scBlacklist);
				File rstBL = new File(dir_data +"/"+ targetdate +"_"+ file.getSubfileid() + SORTED_FILE +".txt");
				int linesBl = 0;
				if (rstBL.isFile()){
					BufferedReader reader = new BufferedReader(new FileReader(dir_data +"/"+ targetdate +"_"+ file.getSubfileid() + SORTED_FILE +".txt"));
					while(reader.readLine()!=null)linesBl ++;
					reader.close();
					System.out.println("Done");
					log.info("After compare Blacklist :: File ["+ targetdate +"_"+ file.getSubfileid() + SORTED_FILE +".txt] :: Source ["+ file.getMaxsend() +"] :: Looping ["+ looping +"] :: Result ["+ linesBl +"] :: Filtered ["+ (file.getMaxsend()-linesBl) +"]");
					deleteFile(dir_data +"/"+ targetdate + SUFFIX_TEMP +".txt" , targetdate + SUFFIX_TEMP +".txt");
				}else{
					System.out.println("Failed to create "+ dir_data +"/"+ targetdate +"_"+ file.getSubfileid() + SORTED_FILE +".txt");
					System.exit(0);
				}
	
				// sort and compare with last 3 days
				compareWithHistoryFile(dir_script, dir_data, targetdate, file.getSubfileid(), histdate);
				
				// append file 
				param = dir_data +" "+ targetdate +" "+ file.getSubfileid(); 
				String appendFile = "perl "+ dir_script +"/filterMsisdn.pl append_file "+ param;
				String finalFile =  dir_data +"/"+ targetdate +"_"+ file.getSubfileid() + SORTED_FILE + FINAL +".txt";
				executeCommand(appendFile);
				BufferedReader reader = new BufferedReader(new FileReader(finalFile));
				int lines = 0;
				while(reader.readLine()!=null)lines ++;
				reader.close();
				log.info("After compare History :: File ["+ targetdate +"_"+ file.getSubfileid() + SORTED_FILE +".txt] :: Source ["+ lines +"] :: Looping ["+ looping +"] :: Result ["+ lines +"] :: Filtered ["+ (linesBl-lines) +"]");
				if(lines > file.getMaxsend()){
					int cut_line = 0;
					//cut file if more than maxsend
					cut_line = lines - file.getMaxsend();
					param = dir_source +" "+ dir_data +" "+ targetdate +" "+ file.getFilename() +" "+ file.getSubfileid() +" "+ cut_line; 
					String cutFile = "perl "+ dir_script +"/filterMsisdn.pl cut_file "+ param;
					executeCommand(cutFile);
					File rstFile = new File(finalFile);
					if(rstFile.isFile()){
						deleteFile(dir_data +"/"+ targetdate +"_"+ file.getSubfileid() + SORTED_FILE + FINAL +".txt", targetdate +"_"+ file.getSubfileid() + SORTED_FILE + FINAL +".txt");
					}
					break;
				}else{
					if(looping==max_loop){
						File rstFile = new File(finalFile);
						if(rstFile.isFile()){
							deleteFile(dir_data +"/"+ targetdate +"_"+ file.getSubfileid() + SORTED_FILE + FINAL +".txt", targetdate +"_"+ file.getSubfileid() + SORTED_FILE + FINAL +".txt");
						}
					}
					looping++;
				}
			}
			// send file to server
			System.out.print("Processing copy file "+ targetdate +"_"+ file.getSubfileid() + SORTED_FILE +".txt to server: "+ file.getAppserver() +"\t\t ..... ");
			//String sendFile = "perl "+ dir_script +"/sendfile.pl "+ dir_data +"/"+ targetdate +"_"+ file.getSubfileid() + SORTED_FILE +" "+ file.getAppserver();
			String target_server = targetServer(file.getAppserver());
			if (target_server == null || target_server.equals(""))
				target_server = DEFAULT_SERVER;
			String sendFile = "/usr/bin/scp "+ dir_data +"/"+ targetdate +"_"+ file.getSubfileid() + SORTED_FILE +".txt "+ target_server + targetdate +"_"+ targetdate +"_"+ file.getSubfileid() + SORTED_FILE +".txt"; //rbtuser@"+ TARGET_SERVER +":/app1/rbtuser/rbt/push/src/"+ targetdate +"_"+ targetdate +"_"+ file.getSubfileid() + SORTED_FILE +".txt";
			if(send_file) executeCommand(sendFile);
			System.out.println("Done");
			
			//update into database
			updateFileName(targetdate +"_"+ file.getSubfileid()+SORTED_FILE, targetdate, file.getSmsbcfileid(), file.getSubfileid());
		}
		deleteFile(dir_data +"/"+ BLACKLIST_FILE +".txt" , BLACKLIST_FILE +".txt");
		System.out.println("Completed");		
	}
	
	private void compareWithHistoryFile(String dir_script, String dir_data, String targetdate, int subfileid, String histdate){
		SmsbcScheduler smshist = new SmsbcScheduler();
		boolean fileExist = true;
		int i = 0;
		SmsbcFileVO fName = new SmsbcFileVO();
		ArrayList<SmsbcFileVO> fn = getHistory(targetdate, histdate);
		for(int x=0; x<fn.size(); x++){
			fName = fn.get(x);
			String fileHistory = fName.getFilename();
			String fileStatdate = fName.getStatdate();
			String fileDir = dir_data+"/"+ fileHistory +".txt";
	    	//Check File History
			File file = new File(fileDir);
            /*	if(!file.isFile()){
				String getFile = "usr/bin/scp -r rbtuser@"+ TARGET_SERVER +":/app1/rbtuser/rbt/push/src/"+ fileStatdate +"_"+ fileHistory +" ";
				smshist.executeCommand(getFile);
			}*/
			if (file.isFile()){
				if(!(targetdate+SORTED_FILE).equals(fileHistory)){
					System.out.print("Processing sort and compare "+ targetdate + SORTED_FILE +" with "+ fileHistory +" \t ..... ");
					String param =  dir_data +" "+ targetdate +" "+ fileHistory +" "+ subfileid;
					String scHistory = "perl "+ dir_script +"/filterMsisdn.pl sort_compare_history "+ param;
					smshist.executeCommand(scHistory);
					System.out.println("Done");
				}
			}else{
				System.out.println(fileHistory +".txt is not found");
			}
		}
	}
	
	private ArrayList<SmsbcFileVO> getFileName(String targetdate){
		PreparedStatement stmt = getSelectFileName(targetdate);
		ArrayList<SmsbcFileVO> ls = new ArrayList();
		SmsbcFileVO f = null;
		try {
			ResultSet rs = stmt.executeQuery();
			int i = 0;
			while(rs.next()){
				f = new SmsbcFileVO();
				f.setSmsbcfileid(rs.getInt("smsbc_file_id"));
				f.setSubfileid(rs.getInt("sub_file_id"));
				f.setFilename(rs.getString("filename"));
				f.setMaxsend(rs.getInt("max_send"));
				f.setAppserver(rs.getString("app_server"));
				ls.add(i, f);
				i++;
			}
		}catch (SQLException e) {
			e.printStackTrace();
		}
		return ls;
	}
	
	private PreparedStatement getSelectFileName(String targetdate) {
		if(selectFileNameStmt == null){
			String sql = "select b.smsbc_file_id, a.sub_file_id, b.filename, a.max_send, a.app_server" +
			" from rbtuser.rbt_smsbc_scheduler a, rbtuser.smsbc_file b" +
			" where a.smsbc_file_id = b.smsbc_file_id AND stat_date = "+ targetdate +
			" and a.filename = 'REGISTER' and a.smsbc_file_id is not null" +
			" order by a.sub_file_id";
			try {
				selectFileNameStmt = getConnection().prepareStatement(sql);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return selectFileNameStmt;
	}
	
	private void getMsisdnBlacklist(){
		PreparedStatement stmt = getSelectMsisdn();
		PrintWriter pw = null;
    	try {
    		String fo = BLACKLIST_FILE_TEMP +".txt";
			try {
				pw = new PrintWriter(new FileWriter(fo));
			} catch (IOException e) {
				e.printStackTrace();
			}
    		
			ResultSet rs = stmt.executeQuery();
			while(rs.next()){
				pw.println(rs.getString("msisdn"));
				pw.checkError();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(pw!=null){try{pw.close();}catch(Exception e){}}
		}
	}
	
	private PreparedStatement getSelectMsisdn() {
		if(selectMsisdnStmt == null){
			String sql = "SELECT msisdn FROM rbtuser.blacklist_postcall union all select 'done' from dual";
			try {
				selectMsisdnStmt = getConnection().prepareStatement(sql);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return selectMsisdnStmt;
	}
	
	private ArrayList<SmsbcFileVO> getHistory(String todayDate, String prev3Day){
		PreparedStatement stmt = getHistorySmsbc(todayDate, prev3Day);
		ArrayList<SmsbcFileVO> list = new ArrayList();
		SmsbcFileVO fl = null;
		try {
			ResultSet rs = stmt.executeQuery();
			int i = 0;
			while(rs.next()){
				fl = new SmsbcFileVO();
				fl.setStatdate(rs.getString("stat_date"));
				fl.setFilename(rs.getString("filename"));
				list.add(i,fl);
				i++;
			}
		}catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	private PreparedStatement getHistorySmsbc(String todayDate, String prev3Day) {
		if(selectHistorySmsbc == null){
			String sql = "SELECT stat_date, filename FROM RBTUSER.RBT_SMSBC_SCHEDULER  WHERE stat_date >= '"+ prev3Day +"'" +
			             "AND smsbc_file_id IS NOT NULL AND filename NOT IN ('REGISTER') ORDER BY stat_date";
			try {
				selectHistorySmsbc = getConnection().prepareStatement(sql);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return selectHistorySmsbc;
	}
	
	private int updateFileName(String filename, String statdate, int smsbcfileid, int subfileid){
		PreparedStatement stmt = updateSmsbcFileName();
		int rs = 0;
		try {
			stmt.setString(1, filename);
			stmt.setString(2, statdate);
			stmt.setInt(3, smsbcfileid);
			stmt.setInt(4, subfileid);
			rs = stmt.executeUpdate();			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rs;
	}
	
	private PreparedStatement updateSmsbcFileName() {
		if(updateFileNameStmt == null){
			String sql = "update RBTUSER.RBT_SMSBC_SCHEDULER set filename =?" +
			" where stat_date =? and smsbc_file_id=? and sub_file_id=? and filename = 'REGISTER'"  ;
			try {
				updateFileNameStmt = getConnection().prepareStatement(sql);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return updateFileNameStmt;
	}
	
	private Connection getConnection(){
		if(dbConnection == null){
		    try {
				// Create a connection to the database
		    	Class.forName("oracle.jdbc.driver.OracleDriver");
		    	dbConnection = DriverManager.getConnection(
		    			db_connect_str,
						"crbt",
						db_passwd);
			} catch (ClassNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
		}
		return dbConnection;
	}

	// execute unix command
	private void executeCommand(String command) {
		 
        String returnCode = "";
		Process p;
		try {
			
			p = Runtime.getRuntime().exec(command);
			p.waitFor();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

    //Delete file
    public static void deleteFile(String deleteFile, String nameFile)
    {
    	//Check File
		File file = new File(deleteFile);
		if (file.isFile())
		{
			   if (file.delete())
					 System.out.println("Deleted "+ nameFile +" file.");
		}
    }

    public void closeAll(){
	    	try {
				if(dbConnection!=null)dbConnection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}    	
	    }
}

