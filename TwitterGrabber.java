import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import twitter4j.Query;
import twitter4j.Query.ResultType;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;


public class TwitterGrabber {
	public Connection connection = null;
	public Statement statement = null;
	public ResultSet resultsSet = null;

	Twitter twitter = null;

	public TwitterGrabber() {
		//Initialize our connection to twitter:
		try {
			ConfigurationBuilder cb = new ConfigurationBuilder();
			cb.setDebugEnabled(true).setOAuthConsumerKey(LoginCredentials.twitterConsumerKey).setOAuthConsumerSecret(LoginCredentials.twitterConsumerSecret).setOAuthAccessToken(LoginCredentials.twitterAccessToken).setOAuthAccessTokenSecret(LoginCredentials.twitterAccessSecret);
			TwitterFactory factory = new TwitterFactory(cb.build());
			twitter = factory.getInstance();

		} catch (Exception te) {
			te.printStackTrace();
		}

		//Initialize our conenction with the oracle DB:
		try {
			connection = DriverManager.getConnection("jdbc:oracle:thin:@oracle.cise.ufl.edu:1521:orcl", LoginCredentials.oracleUsername, LoginCredentials.oraclePassword);
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}

		//Initialize a statement so we can send statements to the DB:
		try {
			statement = connection.createStatement();
		} catch (SQLException e) {
			System.out.println(e.toString());
		}
	}

	//////////////////////////////////////////////////////////
	// Functions start for the oracle DB:                   //
	//////////////////////////////////////////////////////////
	
	//Drops and purges a given table name
	public String deleteOracleTable(String table) {
		try {
			resultsSet = statement.executeQuery("drop table " + table);
			resultsSet = statement.executeQuery("purge table " + table);
			//resultsSet = statement.executeQuery("create table TwitterDB(student_id integer,student_name varchar(25) not null,state varchar(2) not null,date_of_birth date,account_balance float,primary key (student_id))");
			if (resultsSet.next()) {
				System.out.println("Details: " + resultsSet.getString(1));
				return resultsSet.getString(1);
			}

		} catch (SQLException e) {
			System.out.println(e.toString());
		}

		return null;
	}
	
	//Creates a table given a string of the table + schema:
	public String createOracleTable(String table) {
		try {
			resultsSet = statement.executeQuery("create table " + table);
			if (resultsSet.next()) {
				System.out.println("Details: " + resultsSet.getString(1));
				return resultsSet.getString(1);
			}

		} catch (SQLException e) {
			System.out.println(e.toString());
		}

		return null;
	}
	
	//Inserts a value into the given table
	public String insertIntoTable(String table, String values) {
		
		try {
			resultsSet = statement.executeQuery("insert into " + table + " values(" + values + ")");
			if (resultsSet.next()) {
				System.out.println("Details: " + resultsSet.getString(1));
				return resultsSet.getString(1);
			}

		} catch (SQLException e) {
			System.out.println(e.toString());
		}

		return null;
	}
	
	///////////////////////////////////
	//End of functions for Oracle DB //
	///////////////////////////////////

	//////////////////////////////////////////////////////////
	// Start of functions for Twitter grabbing:             //
	//////////////////////////////////////////////////////////
	
	//This function returns an array list of statuses with the 1000 most recent tweets with a given hash tag:
	public ArrayList<Status> getTweetsWithHashtag(String hashTag, String fromDate, String toDate, long max_id, boolean decreasing, long last_id) {
		
		ArrayList<Status> tweetData = new ArrayList<Status>();
		
		//Try to query for hashtag:
		// get the 1000 most recent tweets tagged #debatenight
		for (int page = 1; page <= 1; page++) {
			Query query = new Query(hashTag);
			query.count(100);
			query.setSince(fromDate);
			query.setUntil(toDate);
			if (max_id != -1) {
				if (decreasing) {
					query.setMaxId(max_id);
				} else {
					query.setSinceId(max_id);
				}
			}
			//query.resultType(ResultType.mixed);
			try {
			QueryResult qr = twitter.search(query);
			Status last = null;
			while (qr.hasNext()) {

				List<Status> qrTweets = qr.getTweets();

				// break out if there are no more tweets
				//Or if were over 5000 tweets:
				if(qrTweets.size() == 0 || tweetData.size() > 10000) break;
			for(Status t : qrTweets) {
				System.out.println(t.getText());
				//If this is a retweet, then get the tweet that was retweeted:
				if (t.isRetweet()) {
					tweetData.add(t.getRetweetedStatus());
				} else {
					tweetData.add(t);
				}
				
			}
			qr = twitter.search(qr.nextQuery());
			}
			} catch (Exception e) {
				System.out.println(e.toString());
			}
			
		}
		return tweetData;
	}

	public String selectItem(String table, String valueToCheck, Long value) {
		
		try {
			resultsSet = statement.executeQuery("select * from " + table + " where " + valueToCheck + " = " + value);
			if (resultsSet.next()) {
				System.out.println("Details: " + resultsSet.getString(1));
				return resultsSet.getString(1);
			}

		} catch (SQLException e) {
			System.out.println(e.toString());
		}

		return null;
		
	}
}
