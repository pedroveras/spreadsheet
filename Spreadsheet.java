import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import com.google.gdata.client.spreadsheet.FeedURLFactory;
import com.google.gdata.client.spreadsheet.SpreadsheetService;
import com.google.gdata.data.spreadsheet.CellEntry;
import com.google.gdata.data.spreadsheet.CellFeed;
import com.google.gdata.data.spreadsheet.ListEntry;
import com.google.gdata.data.spreadsheet.ListFeed;
import com.google.gdata.data.spreadsheet.WorksheetEntry;
import com.google.gdata.data.spreadsheet.WorksheetFeed;
import com.google.gdata.util.AuthenticationException;
import com.google.gdata.util.ServiceException;
import com.mysql.jdbc.PreparedStatement;

/**
 * 
 * @author Pedro Henriquw
 * @email pedroveras@gmail.com
 *
 */
public class Spreadsheet {

	private SpreadsheetService service;
	private String spreadsheetKey;
	private FeedURLFactory factory;
	private Connection c;
	private PreparedStatement pstm;
	
	public Spreadsheet(SpreadsheetService service, String spreadsheetKey) {
		this.service = service;
		this.factory = FeedURLFactory.getDefault();
		this.spreadsheetKey = spreadsheetKey;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Scanner s = new Scanner(System.in);
		String hostname = null;
		String username = null;
		String password = null;
		String database = null;
		String sql = null;
		String key = null;
		String userGoogle = null;
		String passGoogle = null;

		System.out.println("MySql Hostname:");
		hostname = s.nextLine();
		
		System.out.println("MySql database:");
		database = s.nextLine();

		System.out.println("MySql Username:");
		username = s.nextLine();

		System.out.println("MySql Password:");
		password = s.nextLine();

		System.out.println("SQL");
		sql = s.nextLine();

		System.out.println("Spreadsheet key:");
		key = s.nextLine();
		
		System.out.println("Google username:");
		userGoogle = s.nextLine();
		
		System.out.println("Google password:");
		passGoogle = s.nextLine();
		
		Spreadsheet spreadsheet = new Spreadsheet(new SpreadsheetService("Spreadsheet"),key);
		spreadsheet.login(userGoogle, passGoogle);
		ResultSet rs = spreadsheet.connectAndLoad(hostname, database,username, password, sql);
		spreadsheet.process(rs);
	}

	public ResultSet connectAndLoad(String hostname, String database, String username,
			String password, String sql) {
		ResultSet rs = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			c = DriverManager.getConnection("jdbc:mysql://"
					+ hostname + ":3306/"+database+"?user=" + username + "&password="
					+ password);
			pstm = (PreparedStatement) c
					.prepareStatement(sql);
			 rs = pstm.executeQuery();
			
		    return rs;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return rs;
	}

	public void login(String username, String password) {
		try {
			service.setUserCredentials(username, password);
		} catch (AuthenticationException e) {
			e.printStackTrace();
		}
	}
	
	public void process(ResultSet rs) {
		ResultSetMetaData rsMetaData = null;
		try {
			URL worksheetFeedlistFeed = factory.getWorksheetFeedUrl(spreadsheetKey, "private", "full");
			WorksheetFeed feed = service.getFeed(worksheetFeedlistFeed, WorksheetFeed.class);
			WorksheetEntry worksheet = feed.getEntries().get(0);
			URL cellFeedUrl = worksheet.getCellFeedUrl();
			rsMetaData = rs.getMetaData();
			int numberOfColumns = rsMetaData.getColumnCount();
			
			URL listFeedUrl = worksheet.getListFeedUrl();
			ListFeed listFeed = service.getFeed(listFeedUrl, ListFeed.class);
			List<ListEntry> rows = listFeed.getEntries();
			Iterator<ListEntry> it = rows.iterator();
			while (it.hasNext())
			{
					ListEntry row = it.next();
					row.delete();
			}
			int i = 1;
			int j = 2;
			
			while (rs.next())
			{
				while (i <= numberOfColumns)
				{
					CellEntry newEntry = new CellEntry(j, i, rs.getString(i));
					CellFeed cellFeed = service.getFeed(listFeedUrl, CellFeed.class);
					service.insert(cellFeedUrl, newEntry);
					i++;
				}
				i = 1;
				j++;
			}
			
			rs.close();
			pstm.close();
			c.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ServiceException e) {
			e.printStackTrace();
		}
		

	}
}
