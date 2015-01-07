package br.com.elosoft.postmon.metrics;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import br.com.elosoft.postmon.commands.CacheHitRateCommand;
import br.com.elosoft.postmon.commands.IndexCacheHitRateCommand;
import br.com.elosoft.postmon.commands.IndexUsageCommand;
import br.com.elosoft.postmon.commands.PgStatActivityCommand;
import br.com.elosoft.postmon.commands.PgStatDatabaseCommand;
import br.com.elosoft.postmon.commands.PgStatUserIndexesCommand;
import br.com.elosoft.postmon.commands.PgStatUserTablesCommand;
import br.com.elosoft.postmon.commands.PgStatioUserIndexesCommand;
import br.com.elosoft.postmon.commands.PgStatioUserSequencesCommand;
import br.com.elosoft.postmon.commands.PgStatioUserTablesCommand;
import br.com.elosoft.postmon.model.Metric;

import com.dynatrace.diagnostics.pdk.MonitorEnvironment;
import com.dynatrace.diagnostics.pdk.MonitorMeasure;

public class MetricLoader {

	private static final String TABLES = "Tables";

	private static final String TABLES_I_O = "Tables I/O";

	private static final String SEQUENCES = "Sequences";

	private static final String INDEXES = "Indexes";

	private static final String INDEXES_I_O = "Indexes I/O";

	private static final String INDEX_CACHE_AND_HIT_RATE = "Index Cache and Hit Rate";

	private static final String INDEX_USAGE = "Index Usage";

	private static final String CACHE_AND_HIT_RATE = "Cache and Hit Rate";

	private static final String SEQUENCES_I_O_STATISTICS = "Sequences I/O Statistics";

	private static final String INDEXES_I_O_STATISTICS = "Indexes I/O Statistics";

	private static final String INDEXES_STATISTICS = "Indexes Statistics";

	private static final String TABLES_I_O_STATISTICS = "Tables I/O Statistics";

	private static final String ACTIVITIES_STATISTICS = "Activities Statistics";

	private static final String TABLES_STATISTICS = "Tables Statistics";

	private static final String DATABASE_STATISTICS = "Database Statistics";

	private Connection conn = null;

	private List<Metric> databaseCommandMetrics = new LinkedList<Metric>();

	private List<Metric> activityCommandMetrics = new LinkedList<Metric>();

	private List<Metric> cacheHitRateCommandMetrics = new LinkedList<Metric>();

	private List<Metric> indexCacheHitRateCommandMetrics = new LinkedList<Metric>();

	private Map<String, LinkedList<Metric>> tablesCommandMetrics = new HashMap<String, LinkedList<Metric>>();

	private Map<String, LinkedList<Metric>> indexesCommandMetrics = new HashMap<String, LinkedList<Metric>>();

	private Map<String, LinkedList<Metric>> indexesIOCommandMetrics = new HashMap<String, LinkedList<Metric>>();

	private Map<String, LinkedList<Metric>> tablesIOCommandMetrics = new HashMap<String, LinkedList<Metric>>();

	private Map<String, LinkedList<Metric>> sequencesIOCommandMetrics = new HashMap<String, LinkedList<Metric>>();

	private Map<String, LinkedList<Metric>> indexUsageCommandMetrics = new HashMap<String, LinkedList<Metric>>();

	public MetricLoader(Connection conn) {
		this.conn = conn;
	}

	private Double check_return(BigDecimal value) {

		Double new_value = 0.0;

		if (value != null) {
			new_value = value.doubleValue();
		}

		return new_value;

	}

	private void loadActivityMetric(MonitorMeasure metric,
			MonitorEnvironment env) {

		Metric dbMetric = new Metric(metric.getMetricName(), new BigDecimal(0));

		Double value = check_return(activityCommandMetrics.get(
				activityCommandMetrics.indexOf(dbMetric)).getValue());

		metric.setValue(value);

	}

	private void loadCacheHitRateMetrics(MonitorMeasure metric,
			MonitorEnvironment env) {

		Metric dbMetric = new Metric(metric.getMetricName(), new BigDecimal(0));

		Double value = check_return(cacheHitRateCommandMetrics.get(
				cacheHitRateCommandMetrics.indexOf(dbMetric)).getValue());

		metric.setValue(value);

	}

	private void loadDatabaseMetric(MonitorMeasure metric) {

		Metric dbMetric = new Metric(metric.getMetricName(), new BigDecimal(0));

		Double value = check_return(databaseCommandMetrics.get(
				databaseCommandMetrics.indexOf(dbMetric)).getValue());

		metric.setValue(value);

	}

	private void loadIndexCacheHitRateMetrics(MonitorMeasure metric,
			MonitorEnvironment env) {

		Metric dbMetric = new Metric(metric.getMetricName(), new BigDecimal(0));

		Double value = check_return(indexCacheHitRateCommandMetrics.get(
				indexCacheHitRateCommandMetrics.indexOf(dbMetric)).getValue());

		metric.setValue(value);

	}

	private void loadIndexesIOMetrics(MonitorMeasure metric,
			MonitorEnvironment env) {

		Metric dbMetric = new Metric(metric.getMetricName(), new BigDecimal(0));

		for (String index : indexesIOCommandMetrics.keySet()) {

			LinkedList<Metric> idxCmdMetric = indexesIOCommandMetrics
					.get(index);

			Metric m = idxCmdMetric.get(idxCmdMetric.indexOf(dbMetric));
			MonitorMeasure d = env.createDynamicMeasure(metric, INDEXES_I_O,
					index);
			d.setValue(check_return(m.getValue()));

		}

	}

	private void loadIndexesMetrics(MonitorMeasure metric,
			MonitorEnvironment env) {

		Metric dbMetric = new Metric(metric.getMetricName(), new BigDecimal(0));

		for (String index : indexesCommandMetrics.keySet()) {

			Metric m = indexesCommandMetrics.get(index).get(
					indexesCommandMetrics.get(index).indexOf(dbMetric));
			MonitorMeasure d = env.createDynamicMeasure(metric, INDEXES,
					index);
			d.setValue(check_return(m.getValue()));

		}

	}

	private void loadIndexUsageMetrics(MonitorMeasure metric,
			MonitorEnvironment env) {

		Metric dbMetric = new Metric(metric.getMetricName(), new BigDecimal(0));

		for (String index : indexUsageCommandMetrics.keySet()) {

			Metric m = indexUsageCommandMetrics.get(index).get(
					indexUsageCommandMetrics.get(index).indexOf(dbMetric));
			MonitorMeasure d = env.createDynamicMeasure(metric, INDEX_USAGE,
					index);
			d.setValue(check_return(m.getValue()));

		}

	}

	public void retrieveMetric(MonitorEnvironment env) {
		
		retrieveDatabaseMetrics(conn, env);
		retrieveTablesMetrics(conn, env);
		retrieveActivityMetrics(conn, env);
		retrieveTablesIOMetrics(conn, env);
		retrieveIndexesMetrics(conn, env);
		retrieveIndexesIOMetrics(conn, env);
		retrieveSequencesIOMetrics(conn, env);
		retrieveCacheHitRateMetrics(conn, env);
		retrieveIndexUsageMetrics(conn, env);
		retrieveIndexCacheHitRateMetrics(conn, env);
		
	}
	
	public void loadMetric(MonitorMeasure metric, MonitorEnvironment env) {

		switch (metric.getMetricGroupName()) {
		case DATABASE_STATISTICS:
			loadDatabaseMetric(metric);
			break;
		case TABLES_STATISTICS:
			loadTablesMetrics(metric, env);
			break;
		case ACTIVITIES_STATISTICS:
			loadActivityMetric(metric, env);
			break;
		case TABLES_I_O_STATISTICS:
			loadTablesIOMetrics(metric, env);
			break;
		case INDEXES_STATISTICS:
			loadIndexesMetrics(metric, env);
			break;
		case INDEXES_I_O_STATISTICS:
			loadIndexesIOMetrics(metric, env);
			break;
		case SEQUENCES_I_O_STATISTICS:
			loadSequencesIOMetrics(metric, env);
			break;
		case CACHE_AND_HIT_RATE:
			loadCacheHitRateMetrics(metric, env);
			break;
		case INDEX_USAGE:
			loadIndexUsageMetrics(metric, env);
			break;
		case INDEX_CACHE_AND_HIT_RATE:
			loadIndexCacheHitRateMetrics(metric, env);
			break;
		default:
			break;
		}

	}

	private void loadSequencesIOMetrics(MonitorMeasure metric,
			MonitorEnvironment env) {

		Metric dbMetric = new Metric(metric.getMetricName(), new BigDecimal(0));

		for (String table : sequencesIOCommandMetrics.keySet()) {

			Metric m = sequencesIOCommandMetrics.get(table).get(
					sequencesIOCommandMetrics.get(table).indexOf(dbMetric));
			MonitorMeasure d = env.createDynamicMeasure(metric, SEQUENCES,
					table);
			d.setValue(check_return(m.getValue()));

		}

	}

	private void loadTablesIOMetrics(MonitorMeasure metric,
			MonitorEnvironment env) {

		Metric dbMetric = new Metric(metric.getMetricName(), new BigDecimal(0));

		for (String table : tablesIOCommandMetrics.keySet()) {

			Metric m = tablesIOCommandMetrics.get(table).get(
					tablesIOCommandMetrics.get(table).indexOf(dbMetric));
			MonitorMeasure d = env.createDynamicMeasure(metric, TABLES_I_O,
					table);
			d.setValue(check_return(m.getValue()));

		}

	}

	private void loadTablesMetrics(MonitorMeasure metric, MonitorEnvironment env) {

		Metric dbMetric = new Metric(metric.getMetricName(), new BigDecimal(0));

		for (String table : tablesCommandMetrics.keySet()) {

			Metric m = tablesCommandMetrics.get(table).get(
					tablesCommandMetrics.get(table).indexOf(dbMetric));
			MonitorMeasure d = env
					.createDynamicMeasure(metric, TABLES, table);

			d.setValue(check_return(m.getValue()));

		}

	}

	private void retrieveActivityMetrics(Connection conn, MonitorEnvironment env) {

		PgStatActivityCommand activityCommand = new PgStatActivityCommand(conn,
				env);

		this.activityCommandMetrics = activityCommand.execute();

	}

	private void retrieveCacheHitRateMetrics(Connection conn,
			MonitorEnvironment env) {

		CacheHitRateCommand cacheHitRateCommand = new CacheHitRateCommand(conn);

		this.cacheHitRateCommandMetrics = cacheHitRateCommand.execute();

	}

	private void retrieveDatabaseMetrics(Connection conn, MonitorEnvironment env) {

		PgStatDatabaseCommand databaseCommand = new PgStatDatabaseCommand(conn,
				env);

		this.databaseCommandMetrics = databaseCommand.execute();

	}

	private void retrieveIndexCacheHitRateMetrics(Connection conn,
			MonitorEnvironment env) {

		IndexCacheHitRateCommand indexCacheHitRateCommand = new IndexCacheHitRateCommand(
				conn);

			this.indexCacheHitRateCommandMetrics = indexCacheHitRateCommand
				.execute();

	}

	private void retrieveIndexesIOMetrics(Connection conn,
			MonitorEnvironment env) {

		PgStatioUserIndexesCommand indexesIOCommand = new PgStatioUserIndexesCommand(
				conn);
		
			this.indexesIOCommandMetrics = indexesIOCommand.execute();

	}

	private void retrieveIndexesMetrics(Connection conn, MonitorEnvironment env) {

		PgStatUserIndexesCommand indexesCommand = new PgStatUserIndexesCommand(
				conn);
		
			this.indexesCommandMetrics = indexesCommand.execute();

	}

	private void retrieveIndexUsageMetrics(Connection conn,
			MonitorEnvironment env) {

		IndexUsageCommand indexUsageCommand = new IndexUsageCommand(conn);
		
			this.indexUsageCommandMetrics = indexUsageCommand.execute();

	}

	private void retrieveSequencesIOMetrics(Connection conn,
			MonitorEnvironment env) {

		PgStatioUserSequencesCommand sequencesIOCommand = new PgStatioUserSequencesCommand(
				conn);
		
			this.sequencesIOCommandMetrics = sequencesIOCommand.execute();

	}

	private void retrieveTablesIOMetrics(Connection conn, MonitorEnvironment env) {

		PgStatioUserTablesCommand tablesIOCommand = new PgStatioUserTablesCommand(
				conn);
		
			this.tablesIOCommandMetrics = tablesIOCommand.execute();

	}

	private void retrieveTablesMetrics(Connection conn, MonitorEnvironment env) {

		PgStatUserTablesCommand tablesCommand = new PgStatUserTablesCommand(
				conn);
		
			this.tablesCommandMetrics = tablesCommand.execute();

	}

}