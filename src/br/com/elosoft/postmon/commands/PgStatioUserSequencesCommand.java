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

public class PgStatioUserSequencesCommand implements Command<Object> {

	private static final String RELNAME = "relname";

	private static final String BLKS_HIT = "blks_hit";

	private static final String BLKS_READ = "blks_read";

	private static final String SQL_SELECT_PG_STATIO_USER_SEQUENCES = "select relname, blks_read, blks_hit from pg_statio_user_sequences";

	private static final Logger log = Logger
			.getLogger(PgStatioUserSequencesCommand.class.getName());

	private Connection conn = null;

	public PgStatioUserSequencesCommand(Connection conn) {

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
					.prepareStatement(SQL_SELECT_PG_STATIO_USER_SEQUENCES);

			ResultSet rs = st.executeQuery();

			while (rs.next()) {

				LinkedList<Metric> resultList = new LinkedList<Metric>();

				resultList.add(new Metric(BLKS_READ, rs
						.getBigDecimal(BLKS_READ)));
				resultList
						.add(new Metric(BLKS_HIT, rs.getBigDecimal(BLKS_HIT)));

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