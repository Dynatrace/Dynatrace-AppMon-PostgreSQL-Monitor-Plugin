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

public class PgStatioUserTablesCommand implements Command<Object> {

	private static final String RELNAME = "relname";

	private static final String TIDX_BLKS_HIT = "tidx_blks_hit";

	private static final String TIDX_BLKS_READ = "tidx_blks_read";

	private static final String TOAST_BLKS_HIT = "toast_blks_hit";

	private static final String TOAST_BLKS_READ = "toast_blks_read";

	private static final String IDX_BLKS_HIT = "idx_blks_hit";

	private static final String IDX_BLKS_READ = "idx_blks_read";

	private static final String HEAP_BLKS_HIT = "heap_blks_hit";

	private static final String HEAP_BLKS_READ = "heap_blks_read";

	private static final String SQL_SELECT_PG_STATIO_USER_TABLES = "select relname, sum(heap_blks_read) heap_blks_read, sum(heap_blks_hit) heap_blks_hit, sum(idx_blks_read) idx_blks_read, sum(idx_blks_hit) idx_blks_hit, sum(toast_blks_read) toast_blks_read, sum(toast_blks_hit) toast_blks_hit, sum(tidx_blks_read) tidx_blks_read, sum(tidx_blks_hit) tidx_blks_hit from pg_statio_user_tables group by relname";

	private static final Logger log = Logger
			.getLogger(PgStatioUserTablesCommand.class.getName());

	private Connection conn = null;

	public PgStatioUserTablesCommand(Connection conn) {

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
					.prepareStatement(SQL_SELECT_PG_STATIO_USER_TABLES);

			ResultSet rs = st.executeQuery();

			while (rs.next()) {

				LinkedList<Metric> resultList = new LinkedList<Metric>();

				resultList.add(new Metric(HEAP_BLKS_READ, rs
						.getBigDecimal(HEAP_BLKS_READ)));
				resultList.add(new Metric(HEAP_BLKS_HIT, rs
						.getBigDecimal(HEAP_BLKS_HIT)));
				resultList.add(new Metric(IDX_BLKS_READ, rs
						.getBigDecimal(IDX_BLKS_READ)));
				resultList.add(new Metric(IDX_BLKS_HIT, rs
						.getBigDecimal(IDX_BLKS_HIT)));
				resultList.add(new Metric(TOAST_BLKS_READ, rs
						.getBigDecimal(TOAST_BLKS_READ)));
				resultList.add(new Metric(TOAST_BLKS_HIT, rs
						.getBigDecimal(TOAST_BLKS_HIT)));
				resultList.add(new Metric(TIDX_BLKS_READ, rs
						.getBigDecimal(TIDX_BLKS_READ)));
				resultList.add(new Metric(TIDX_BLKS_HIT, rs
						.getBigDecimal(TIDX_BLKS_HIT)));

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