package br.com.elosoft.postmon;

import java.sql.Connection;
import java.util.logging.Logger;

import br.com.elosoft.postmon.exception.PostMonException;
import br.com.elosoft.postmon.metrics.MetricLoader;

import com.dynatrace.diagnostics.pdk.MonitorEnvironment;
import com.dynatrace.diagnostics.pdk.MonitorMeasure;
import com.dynatrace.diagnostics.pdk.Status;

public class PostPlugin {

	private Connection conn = null;
	private MetricLoader metricLoader = null;
	private static final Logger log = Logger.getLogger(PostMon.class.getName());

	public Status setup(MonitorEnvironment env) throws Exception {

		br.com.elosoft.postmon.jdbc.Connection c = new br.com.elosoft.postmon.jdbc.Connection();
		conn = c.getConnection(env.getConfigString("hostName"),
				env.getConfigString("dbPort"), env.getConfigString("dbName"),
				env.getConfigString("dbUsername"),
				env.getConfigPassword("dbPassword"));

		metricLoader = new MetricLoader(conn);

		return new Status(Status.StatusCode.Success);
	}
	
	public Status execute(MonitorEnvironment env) throws Exception {

		try {
			
			metricLoader.retrieveMetric(env);
			
			for (MonitorMeasure metric : env.getMonitorMeasures()) {
				metricLoader.loadMetric(metric, env);
			}
		} catch (Exception e) {
			log.severe("Threw a Exception executing the load of metrics. Error: " + e.getMessage());
			log.severe(PostMonException.stackTraceToString(e));			
		}

		return new Status(Status.StatusCode.Success);
	}

	public void teardown(MonitorEnvironment env) throws Exception {

		if (conn != null) {
			conn.close();
		}

	}

}
