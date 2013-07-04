package crawler;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

public class TweetCrawler {

	private static final String jdbcPro = "jdbc.properties";
	private static final String connStrKey = "connStr";
	private static final String intervalKey = "interval";
	private static final String logKey = "log";

	public static void main(String[] args) throws Exception {

		Properties prop = new Properties();
		prop.load(new FileInputStream(jdbcPro));
		// prop.load(new FileInputStream(args[0]));
		String connStr = prop.getProperty(connStrKey);
		int interval = Integer.parseInt(prop.getProperty(intervalKey));
		String log = prop.getProperty(logKey);

		Logger logger = Logger.getLogger("log");
		FileHandler handler = new FileHandler(log, true);
		handler.setFormatter(new LogFormatter());
		logger.addHandler(handler);

		logger.info("Connection string: " + connStr);
		logger.info("Interval: " + interval);
		logger.info("Log: " + log);

		Twitter twitter = new TwitterFactory().getInstance();

		try {

			Connection conn = DriverManager.getConnection(connStr);
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT ID,TITLE FROM MOVIE");

			Map<Integer, Query> queries = new HashMap<Integer, Query>();

			while (rs.next()) {
				Query query = new Query(rs.getNString(2));
				query.setCount(100);
				queries.put(rs.getInt(1), query);
				logger.info("Movie: " + rs.getInt(1) + " " + rs.getNString(2));
			}

			rs.close();
			stmt.close();

			PreparedStatement pstmt = conn
					.prepareStatement("INSERT INTO TWEET VALUES (?,?,?,?,?,?,?)");

			Query query;
			QueryResult result;
			List<Status> tweets;

			while (true) {
				// System.out.println(new Date());

				for (int id : queries.keySet()) {

					query = queries.get(id);
					result = twitter.search(query);
					query.setSinceId(result.getMaxId());
					tweets = result.getTweets();
					logger.info("Tweet: " + query.getQuery() + " "
							+ tweets.size() + " " + query.getSinceId());

					for (Status tweet : tweets) {
						/*
						 * System.out.println(tweet.getId() + "#$%" + id + "#$%"
						 * + format.format(tweet.getCreatedAt()) + "#$%" +
						 * (tweet.getGeoLocation() != null ? tweet
						 * .getGeoLocation().getLatitude() + "#$%" +
						 * tweet.getGeoLocation().getLongitude() : "#$%") +
						 * "#$%" + tweet.getUser().getScreenName() + "#$%" +
						 * tweet.getText() + "%$#");
						 */
						pstmt.setLong(1, tweet.getId());
						pstmt.setInt(2, id);
						pstmt.setTimestamp(3, new Timestamp(tweet
								.getCreatedAt().getTime()));
						if (tweet.getGeoLocation() != null) {
							pstmt.setDouble(4, tweet.getGeoLocation()
									.getLatitude());
							pstmt.setDouble(5, tweet.getGeoLocation()
									.getLongitude());
						} else {
							pstmt.setNull(4, Types.DECIMAL);
							pstmt.setNull(5, Types.DECIMAL);
						}
						pstmt.setNString(6, tweet.getUser().getScreenName());
						pstmt.setNString(7, tweet.getText());
						try {
							pstmt.execute();
						} catch (SQLIntegrityConstraintViolationException e) {
						}
					}
				}

				// System.out.println(new Date());
				Thread.sleep(interval);
			}

			// pstmt.close();
			// conn.close();

		} catch (TwitterException te) {
			te.printStackTrace();
			logger.severe(te.getMessage());
			// System.out.println("Failed to search tweets: " +
			// te.getMessage());
		}
	}
}
