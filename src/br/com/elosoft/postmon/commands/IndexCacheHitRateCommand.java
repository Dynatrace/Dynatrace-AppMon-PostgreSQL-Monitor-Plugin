package br.com.elosoft.postmon.commands;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import br.com.elosoft.postmon.exception.PostMonException;
import br.com.elosoft.postmon.model.Metric;

public class IndexCacheHitRateCommand implements Command<Object> {

	private static final String RATIO = "ratio";

	private static final String IDX_HIT = "idx_hit";

	private static final String IDX_READ = "idx_read";

	private static final String SQL_SELECT_INDEX_CACHE_HIT_RATE = "SELECT sum(idx_blks_read) as idx_read, sum(idx_blks_hit)  as idx_hit, ((sum(idx_blks_hit) - sum(idx_blks_read)) / sum(idx_blks_hit)) * 100 as ratio FROM pg_statio_user_indexes";

	private static final Logger log = Logger
			.getLogger(IndexCacheHitRateCommand.class.getName());

	private Connection conn = null;

	public IndexCacheHitRateCommand(Connection conn) {
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
					.prepareStatement(SQL_SELECT_INDEX_CACHE_HIT_RATE);

			ResultSet rs = st.executeQuery();

			while (rs.next()) {

				resultList
						.add(new Metric(IDX_READ, rs.getBigDecimal(IDX_READ)));
				resultList.add(new Metric(IDX_HIT, rs.getBigDecimal(IDX_HIT)));
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