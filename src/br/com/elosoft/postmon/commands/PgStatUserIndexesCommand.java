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

public class PgStatUserIndexesCommand implements Command<Object> {

	private static final String IDX = "idx";

	private static final String IDX_TUP_FETCH = "idx_tup_fetch";

	private static final String IDX_TUP_READ = "idx_tup_read";

	private static final String IDX_SCAN = "idx_scan";

	private static final String SQL_SELECT_PG_STAT_USER_INDEXES = "select relname || ' [Index: ' || indexrelname || ']' idx, sum(idx_scan) idx_scan, sum(idx_tup_read) idx_tup_read, sum(idx_tup_fetch) idx_tup_fetch from pg_stat_user_indexes group by relname || ' [Index: ' || indexrelname || ']'";

	private static final Logger log = Logger
			.getLogger(PgStatUserIndexesCommand.class.getName());

	private Connection conn = null;

	public PgStatUserIndexesCommand(Connection conn) {

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
					.prepareStatement(SQL_SELECT_PG_STAT_USER_INDEXES);

			ResultSet rs = st.executeQuery();

			while (rs.next()) {

				LinkedList<Metric> resultList = new LinkedList<Metric>();

				resultList
						.add(new Metric(IDX_SCAN, rs.getBigDecimal(IDX_SCAN)));
				resultList.add(new Metric(IDX_TUP_READ, rs
						.getBigDecimal(IDX_TUP_READ)));
				resultList.add(new Metric(IDX_TUP_FETCH, rs
						.getBigDecimal(IDX_TUP_FETCH)));

				resultMap.put(rs.getString(IDX), resultList);

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