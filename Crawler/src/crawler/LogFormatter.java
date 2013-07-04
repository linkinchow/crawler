package crawler;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class LogFormatter extends Formatter {

	@Override
	public String format(LogRecord record) {
		return formatDate(record.getMillis()) + " " + record.getLevel() + " " + record.getMessage() + "\n";
	}

	private String formatDate(long current) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		Date date = new Date(current);
		return format.format(date);
	}
}
