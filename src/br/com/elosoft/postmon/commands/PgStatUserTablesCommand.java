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

public class PgStatUserTablesCommand implements Command<Object> {

	private static final String RELNAME = "relname";

	private static final String AUTOANALYZE_COUNT = "autoanalyze_count";

	private static final String ANALYZE_COUNT = "analyze_count";

	private static final String AUTOVACUUM_COUNT = "autovacuum_count";

	private static final String VACUUM_COUNT = "vacuum_count";

	private static final String N_DEAD_TUP = "n_dead_tup";

	private static final String N_LIVE_TUP = "n_live_tup";

	private static final String N_TUP_HOT_UPD = "n_tup_hot_upd";

	private static final String N_TUP_DEL = "n_tup_del";

	private static final String N_TUP_UPD = "n_tup_upd";

	private static final String N_TUP_INS = "n_tup_ins";

	private static final String IDX_TUP_FETCH = "idx_tup_fetch";

	private static final String IDX_SCAN = "idx_scan";

	private static final String SEQ_TUP_READ = "seq_tup_read";

	private static final String SEQ_SCAN = "seq_scan";

	private static final String SQL_SELECT_PG_STAT_USER_TABLES = "select relname, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd, n_live_tup, n_dead_tup, vacuum_count, autovacuum_count, analyze_count, autoanalyze_count from pg_stat_user_tables";

	private static final Logger log = Logger
			.getLogger(PgStatUserTablesCommand.class.getName());

	private Connection conn = null;

	public PgStatUserTablesCommand(Connection conn) {

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
					.prepareStatement(SQL_SELECT_PG_STAT_USER_TABLES);

			ResultSet rs = st.executeQuery();

			while (rs.next()) {

				LinkedList<Metric> resultList = new LinkedList<Metric>();

				resultList
						.add(new Metric(SEQ_SCAN, rs.getBigDecimal(SEQ_SCAN)));
				resultList.add(new Metric(SEQ_TUP_READ, rs
						.getBigDecimal(SEQ_TUP_READ)));
				resultList
						.add(new Metric(IDX_SCAN, rs.getBigDecimal(IDX_SCAN)));
				resultList.add(new Metric(IDX_TUP_FETCH, rs
						.getBigDecimal(IDX_TUP_FETCH)));
				resultList.add(new Metric(N_TUP_INS, rs
						.getBigDecimal(N_TUP_INS)));
				resultList.add(new Metric(N_TUP_UPD, rs
						.getBigDecimal(N_TUP_UPD)));
				resultList.add(new Metric(N_TUP_DEL, rs
						.getBigDecimal(N_TUP_DEL)));
				resultList.add(new Metric(N_TUP_HOT_UPD, rs
						.getBigDecimal(N_TUP_HOT_UPD)));
				resultList.add(new Metric(N_LIVE_TUP, rs
						.getBigDecimal(N_LIVE_TUP)));
				resultList.add(new Metric(N_DEAD_TUP, rs
						.getBigDecimal(N_DEAD_TUP)));
				resultList.add(new Metric(VACUUM_COUNT, rs
						.getBigDecimal(VACUUM_COUNT)));
				resultList.add(new Metric(AUTOVACUUM_COUNT, rs
						.getBigDecimal(AUTOVACUUM_COUNT)));
				resultList.add(new Metric(ANALYZE_COUNT, rs
						.getBigDecimal(ANALYZE_COUNT)));
				resultList.add(new Metric(AUTOANALYZE_COUNT, rs
						.getBigDecimal(AUTOANALYZE_COUNT)));

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