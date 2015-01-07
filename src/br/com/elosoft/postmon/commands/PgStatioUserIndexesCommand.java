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

public class PgStatioUserIndexesCommand implements Command<Object> {

	private static final String IDX = "idx";

	private static final String IDX_BLKS_HIT = "idx_blks_hit";

	private static final String IDX_BLKS_READ = "idx_blks_read";

	private static final String SQL_SELECT_PG_STATIO_USER_INDEXES = "select relname || ' [Index: ' || indexrelname || ']' idx, sum(idx_blks_read) idx_blks_read, sum(idx_blks_hit) idx_blks_hit from pg_statio_user_indexes group by relname, indexrelname";

	private static final Logger log = Logger
			.getLogger(PgStatioUserIndexesCommand.class.getName());

	private Connection conn = null;

	public PgStatioUserIndexesCommand(Connection conn) {

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
					.prepareStatement(SQL_SELECT_PG_STATIO_USER_INDEXES);

			ResultSet rs = st.executeQuery();

			while (rs.next()) {

				LinkedList<Metric> resultList = new LinkedList<Metric>();

				resultList.add(new Metric(IDX_BLKS_READ, rs
						.getBigDecimal(IDX_BLKS_READ)));
				resultList.add(new Metric(IDX_BLKS_HIT, rs
						.getBigDecimal(IDX_BLKS_HIT)));

				resultMap.put(rs.getString(IDX), resultList);

			}
			rs.close();
			st.close();
		} catch (Exception se) {
			log.severe("Threw a Exception creating the list of metrics. Error: " + se.getMessage());
			log.severe(PostMonException.stackTraceToString(se));;
		}

		return resultMap;

	}

}