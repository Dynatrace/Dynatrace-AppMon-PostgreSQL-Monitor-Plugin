package br.com.elosoft.postmon.commands;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import br.com.elosoft.postmon.exception.PostMonException;
import br.com.elosoft.postmon.model.Metric;

public class CacheHitRateCommand implements Command<Object> {

	private static final String RATIO = "ratio";

	private static final String HEAP_HIT = "heap_hit";

	private static final String HEAP_READ = "heap_read";

	private static final String SQL_SELECT_CACHE_HIT_RATE = "SELECT sum(heap_blks_read) as heap_read, sum(heap_blks_hit)  as heap_hit, (sum(heap_blks_hit) / (sum(heap_blks_hit) + sum(heap_blks_read))) * 100 as ratio FROM pg_statio_user_tables";

	private static final Logger log = Logger
			.getLogger(CacheHitRateCommand.class.getName());

	private Connection conn = null;

	public CacheHitRateCommand(Connection conn) {
		this.conn = conn;
	}

	@Override
	public List<Metric> execute() {
		return retrieve();
	}

	private List<Metric> retrieve() {

		List<Metric> resultList = new LinkedList<Metric>();

		try {

			PreparedStatement st = this.conn
					.prepareStatement(SQL_SELECT_CACHE_HIT_RATE);

			ResultSet rs = st.executeQuery();

			while (rs.next()) {

				resultList.add(new Metric(HEAP_READ, rs
						.getBigDecimal(HEAP_READ)));
				resultList
						.add(new Metric(HEAP_HIT, rs.getBigDecimal(HEAP_HIT)));
				resultList.add(new Metric(RATIO, rs.getBigDecimal(RATIO)));

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