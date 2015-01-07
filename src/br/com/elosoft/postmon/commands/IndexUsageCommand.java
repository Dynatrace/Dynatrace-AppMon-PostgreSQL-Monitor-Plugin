package br.com.elosoft.postmon.commands;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.logging.Logger;

import br.com.elosoft.postmon.exception.PostMonException;
import br.com.elosoft.postmon.model.Metric;

public class IndexUsageCommand implements Command<Object> {

	private static final String RELNAME = "relname";

	private static final String ROWS_IN_TABLE = "rows_in_table";

	private static final String PERCENT_OF_TIMES_INDEX_USED = "percent_of_times_index_used";

	private static final String SQL_SELECT_INDEX_USAGE = "SELECT relname, 100 * idx_scan / (seq_scan + idx_scan) percent_of_times_index_used, n_live_tup rows_in_table FROM pg_stat_user_tables WHERE seq_scan + idx_scan > 0 ORDER BY n_live_tup DESC";

	private static final Logger log = Logger.getLogger(IndexUsageCommand.class
			.getName());

	private Connection conn = null;

	public IndexUsageCommand(Connection conn) {

		this.conn = conn;
	}

	@Override
	public Map<String, LinkedList<Metric>> execute() {
		return retrieve();
	}

	private Map<String, LinkedList<Metric>> retrieve() {

		Map<String, LinkedList<Metric>> resultMap = new HashMap<String, LinkedList<Metric>>();

		try {

			PreparedStatement st = this.conn
					.prepareStatement(SQL_SELECT_INDEX_USAGE);

			ResultSet rs = st.executeQuery();

			while (rs.next()) {

				LinkedList<Metric> resultList = new LinkedList<Metric>();

				resultList.add(new Metric(PERCENT_OF_TIMES_INDEX_USED, rs
						.getBigDecimal(PERCENT_OF_TIMES_INDEX_USED)));
				resultList.add(new Metric(ROWS_IN_TABLE, rs
						.getBigDecimal(ROWS_IN_TABLE)));

				resultMap.put(rs.getString(RELNAME), resultList);

			}
			rs.close();
			st.close();
		} catch (Exception se) {
			log.severe("Threw a Exception creating the list of metrics. Error: " + se.getMessage());
			log.severe(PostMonException.stackTraceToString(se));
		}

		return resultMap;

	}

}