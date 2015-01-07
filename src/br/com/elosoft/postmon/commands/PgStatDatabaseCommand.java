package br.com.elosoft.postmon.commands;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import br.com.elosoft.postmon.exception.PostMonException;
import br.com.elosoft.postmon.model.Metric;

import com.dynatrace.diagnostics.pdk.MonitorEnvironment;

public class PgStatDatabaseCommand implements Command<Object> {

	private static final String BLK_WRITE_TIME = "blk_write_time";

	private static final String BLK_READ_TIME = "blk_read_time";

	private static final String DEADLOCKS = "deadlocks";

	private static final String TEMP_BYTES = "temp_bytes";

	private static final String CONFLICTS = "conflicts";

	private static final String TUP_DELETED = "tup_deleted";

	private static final String TUP_UPDATED = "tup_updated";

	private static final String TUP_INSERTED = "tup_inserted";

	private static final String TUP_FETCHED = "tup_fetched";

	private static final String TUP_RETURNED = "tup_returned";

	private static final String BLKS_HIT = "blks_hit";

	private static final String BLKS_READ = "blks_read";

	private static final String XACT_ROLLBACK = "xact_rollback";

	private static final String XACT_COMMIT = "xact_commit";

	private static final String NUMBACKENDS = "numbackends";

	private static final String DB_NAME = "dbName";

	private static final String SQL_SELECT_PG_STAT_DATABASE = "select numbackends, xact_commit, xact_rollback, blks_read, blks_hit, tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted, conflicts, temp_bytes, deadlocks, blk_read_time, blk_write_time from pg_stat_database where datname = ?";

	private static final Logger log = Logger
			.getLogger(PgStatDatabaseCommand.class.getName());

	private Connection conn = null;
	private MonitorEnvironment env = null;

	public PgStatDatabaseCommand(Connection conn, MonitorEnvironment env) {

		this.conn = conn;
		this.env = env;
	}

	@Override
	public List<Metric> execute() {
		return retrieve();
	}

	private List<Metric> retrieve() {

		List<Metric> resultList = new LinkedList<Metric>();

		try {

			PreparedStatement st = this.conn
					.prepareStatement(SQL_SELECT_PG_STAT_DATABASE);

			st.setString(1, env.getConfigString(DB_NAME));
			ResultSet rs = st.executeQuery();

			while (rs.next()) {

				resultList.add(new Metric(NUMBACKENDS, rs
						.getBigDecimal(NUMBACKENDS)));
				resultList.add(new Metric(XACT_COMMIT, rs
						.getBigDecimal(XACT_COMMIT)));
				resultList.add(new Metric(XACT_ROLLBACK, rs
						.getBigDecimal(XACT_ROLLBACK)));
				resultList.add(new Metric(BLKS_READ, rs
						.getBigDecimal(BLKS_READ)));
				resultList
						.add(new Metric(BLKS_HIT, rs.getBigDecimal(BLKS_HIT)));
				resultList.add(new Metric(TUP_RETURNED, rs
						.getBigDecimal(TUP_RETURNED)));
				resultList.add(new Metric(TUP_FETCHED, rs
						.getBigDecimal(TUP_FETCHED)));
				resultList.add(new Metric(TUP_INSERTED, rs
						.getBigDecimal(TUP_INSERTED)));
				resultList.add(new Metric(TUP_UPDATED, rs
						.getBigDecimal(TUP_UPDATED)));
				resultList.add(new Metric(TUP_DELETED, rs
						.getBigDecimal(TUP_DELETED)));
				resultList.add(new Metric(CONFLICTS, rs
						.getBigDecimal(CONFLICTS)));
				resultList.add(new Metric(TEMP_BYTES, rs
						.getBigDecimal(TEMP_BYTES)));
				resultList.add(new Metric(DEADLOCKS, rs
						.getBigDecimal(DEADLOCKS)));
				resultList.add(new Metric(BLK_READ_TIME, rs
						.getBigDecimal(BLK_READ_TIME)));
				resultList.add(new Metric(BLK_WRITE_TIME, rs
						.getBigDecimal(BLK_WRITE_TIME)));

			}
			rs.close();
			st.close();
		} catch (Exception se) {
			log.severe("Threw a Exception creating the list of metrics. Error: " + se.getMessage());
			log.severe(PostMonException.stackTraceToString(se));
		}

		return resultList;

	}

}