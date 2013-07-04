package crawler;

import it.jtomato.JTomato;
import it.jtomato.gson.Movie;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLIntegrityConstraintViolationException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;

public class MovieCrawler {

	private static final String propertyFile = "tomatoes.properties";
	private static final String apiKey = "RottenTomatoesApiKey";

	private static final String jdbcPro = "jdbc.properties";
	private static final String connStrKey = "connStr";

	public static void main(String[] args) throws Exception {

		Properties prop = new Properties();
		prop.load(new FileInputStream(propertyFile));
		String rottenKey = prop.getProperty(apiKey);
		JTomato rottenClient = new JTomato(rottenKey);

		prop.load(new FileInputStream(jdbcPro));
		// prop.load(new FileInputStream(args[0]));
		String connStr = prop.getProperty(connStrKey);

		Connection conn = DriverManager.getConnection(connStr);
		PreparedStatement stmt = conn
				.prepareStatement("INSERT INTO MOVIE (ID,TITLE,YEAR,MPAA_RATING,RUNTIME,CRITICS_CONSENSUS,RELEASE_DATE,SYNOPSIS,POSTER) VALUES (?,?,?,?,?,?,?,?,?)");

		List<Movie> movies = rottenClient.getOpeningMovies(null, 0);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		for (Movie movie : movies) {

			/*
			 * System.out.println(movie.id + "#$%" + movie.title + "#$%" +
			 * movie.year + "#$%" + movie.mpaaRating + "#$%" + movie.runtime +
			 * "#$%" + movie.criticsConsensus + "#$%" +
			 * movie.releaseDate.theater + "#$%" + movie.synopsis + "#$%" +
			 * movie.posters.detailed);
			 */

			stmt.setInt(1, Integer.parseInt(movie.id));
			stmt.setNString(2, movie.title);
			stmt.setInt(3, Integer.parseInt(movie.year));
			stmt.setNString(4, movie.mpaaRating);
			stmt.setNString(5, movie.runtime);
			stmt.setNString(6, movie.criticsConsensus);
			stmt.setDate(7, new Date(sdf.parse(movie.releaseDate.theater)
					.getTime()));
			stmt.setNString(8, movie.synopsis);
			stmt.setNString(9, movie.posters.detailed);
			try {
				stmt.execute();
				System.out.println(movie.id + " " + movie.title
						+ " is inserted.");
			} catch (SQLIntegrityConstraintViolationException e) {
				System.out.println(movie.id + " " + movie.title
						+ " already exists.");
			}

		}

		stmt.close();
		conn.close();
	}
}
