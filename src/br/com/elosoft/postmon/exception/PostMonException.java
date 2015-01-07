package br.com.elosoft.postmon.exception;

import java.io.PrintWriter;
import java.io.StringWriter;

public class PostMonException {

	public static String stackTraceToString(Throwable e) {
		StringWriter sw = new StringWriter();
		e.printStackTrace(new PrintWriter(sw));
		return "StackTrace >>> " + sw.toString();
	}
	
}
