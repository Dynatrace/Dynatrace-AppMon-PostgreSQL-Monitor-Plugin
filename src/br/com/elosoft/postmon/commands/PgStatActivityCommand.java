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

public class PgStatActivityCommand implements Command<Object> {

	private static final String COUNTER = "counter";

	private static final String DB_NAME = "dbName";

	private static final String SQL_SELECT_PG_STAT_ACTIVITY = "select count(1) counter from pg_stat_activity where state = 'active' and datname = ?";

	private static final Logger log = Logger
			.getLogger(PgStatActivityCommand.class.getName());

	private Connection conn = null;
	private MonitorEnvironment env = null;

	public PgStatActivityCommand(Connection conn, MonitorEnvironment env) {

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
					.prepareStatement(SQL_SELECT_PG_STAT_ACTIVITY);

			st.setString(1, env.getConfigString(DB_NAME));
			ResultSet rs = st.executeQuery();

			while (rs.next()) {

				resultList.add(new Metric(COUNTER, rs.getBigDecimal(COUNTER)));

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