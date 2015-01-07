package br.com.elosoft.postmon;

import com.dynatrace.diagnostics.pdk.Monitor;
import com.dynatrace.diagnostics.pdk.MonitorEnvironment;
import com.dynatrace.diagnostics.pdk.Status;

/**
 * PostgreSQL Statistics Metrics Plugin
 * 
 * @author Fernando Lewandowski Albuquerque (fernando.lewandowski@gmail.com)
 * @version 1.0.0
 * @since 1.0.0
 * @link http://community.compuwareapm.com/
 * @link http://community.compuwareapm.com/plugins/contribute/
 * @see br.com.elosoft.postmon.PostPlugin
 * 
 **/
public class PostMon extends PostPlugin implements Monitor {

	@Override
	public Status execute(MonitorEnvironment env) throws Exception {
		return super.execute(env);
	}

	@Override
	public Status setup(MonitorEnvironment env) throws Exception {
		return super.setup(env);
	}

	@Override
	public void teardown(MonitorEnvironment env) throws Exception {
		super.teardown(env);
	}

}