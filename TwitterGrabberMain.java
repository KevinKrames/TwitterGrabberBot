import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

//This is the main file which just runs the program.
//TwitterGrabber.java connects to twitter and the oracle database
public class TwitterGrabberMain {

	public static void main(String[] args) throws Exception {

		//Main function that first gets called:
		//Init our non-static twitter Grabber:
		TwitterGrabber twitterGrabber = new TwitterGrabber();
		//twitterGrabber.deleteOracleTable("Tweet");
		//twitterGrabber.deleteOracleTable("TwitterUser");
		//twitterGrabber.createOracleTable(LoginCredentials.tweetSchema);
		//twitterGrabber.createOracleTable(LoginCredentials.twitterUserSchema);


		//Loop to look for tweets:
		long max_id = -1;
		long min_id = -1;
		long last_id = -1;
		boolean decreasing = true;

		//first tweet of the day: 778775801642651648
		
		for (int j = 0; j < 10000; j++) {
			ArrayList<Status> tweets;
			if (decreasing) {
				tweets = twitterGrabber.getTweetsWithHashtag("#debatenight", "2016-09-27", "2016-09-30", max_id, decreasing, last_id);
			} else {
				tweets = twitterGrabber.getTweetsWithHashtag("#debatenight", "2016-09-27", "2016-09-30", min_id, decreasing, last_id);
			}
			if (tweets.size() > 0) {
			if (last_id == tweets.get(tweets.size()-1).getId()) {
				//decreasing = !decreasing;
				//min_id = tweets.get(tweets.size()-1).getId();
				//max_id = tweets.get(tweets.size()-1).getId();
				min_id = -1;
				max_id = -1;
				System.out.println("****************SWITCHING DIRECTION**************");
				Thread.sleep(5000);
			}

			last_id = tweets.get(tweets.size()-1).getId();
			} else {
				//decreasing = !decreasing;
				//min_id = tweets.get(tweets.size()-1).getId();
				//max_id = tweets.get(tweets.size()-1).getId();
				min_id = -1;
				max_id = -1;
				System.out.println("****************SWITCHING DIRECTION**************");
				Thread.sleep(5000);
			}
			Thread.sleep(1000);
			for (int i = 0; i < tweets.size(); i++) {
				//If the tweet we just grabbed arent in the database, add them:
				String test = twitterGrabber.selectItem("Tweet", "status_id", tweets.get(i).getId());
				if (decreasing) {
					max_id = tweets.get(i).getId();
				} else {
					min_id = tweets.get(i).getId();
				}
				
				/*if (tweets.size() == 1) {
					decreasing = !decreasing;
					min_id = tweets.get(i).getId();
					max_id = tweets.get(i).getId();
					System.out.println("****************SWITCHING DIRECTION**************");
					Thread.sleep(5000);
				}*/
				
				if (test == null) {
					//Since the test is null, it doesnt exist in the database, add it:
					//Also if it's not a retweet
					if (!tweets.get(i).isRetweet()) {

						String tweet = tweets.get(i).getText().replaceAll("'", "''");
						String place = null;
						if (tweets.get(i).getPlace() != null) {
							place = tweets.get(i).getPlace().getName();
						}
						twitterGrabber.insertIntoTable("Tweet", tweets.get(i).getId() +", "+ tweets.get(i).getUser().getId() +", '"+  tweet +"', "+  tweets.get(i).getInReplyToUserId() +", "+  tweets.get(i).getInReplyToStatusId() +", " + tweets.get(i).getRetweetCount() + ", " + tweets.get(i).getFavoriteCount() + ", '" + tweets.get(i).getLang() + "', '" + place + "', TO_TIMESTAMP('" + (new Timestamp(tweets.get(i).getCreatedAt().getTime()).toString().substring(0, 19)) + "' ,'yyyy-mm-dd HH24:MI:SS')");

						String test2 = twitterGrabber.selectItem("TwitterUser", "user_id", tweets.get(i).getUser().getId());
						if (test2 == null) {
							String name = tweets.get(i).getUser().getName().replaceAll("'", "''");
							String location = tweets.get(i).getUser().getLocation().replaceAll("'", "''");
							twitterGrabber.insertIntoTable("TwitterUser", tweets.get(i).getUser().getId() +", '"+ name +"', '"+  tweets.get(i).getUser().getLang() +"', "+  tweets.get(i).getUser().getFollowersCount() +", '" + location + "', " + "TO_TIMESTAMP('" + (new Timestamp(tweets.get(i).getCreatedAt().getTime()).toString().substring(0, 19)) + "' ,'yyyy-mm-dd HH24:MI:SS')");
						}
					}
				}
			}

		}
	}
}
