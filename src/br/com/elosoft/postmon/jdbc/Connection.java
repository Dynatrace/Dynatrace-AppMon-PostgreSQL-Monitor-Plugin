package br.com.elosoft.postmon.jdbc;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Logger;

import br.com.elosoft.postmon.exception.PostMonException;

public class Connection {

	private java.sql.Connection conn = null;

	private static final Logger log = Logger.getLogger(Connection.class
			.getName());

	public Connection() {
		super();
	}

	public java.sql.Connection getConnection(String hostname, String dbPort,
			String dbName, String username, String password) {

		try {

			verifyForName();

			conn = DriverManager.getConnection("jdbc:postgresql://" + hostname
					+ ":" + dbPort + "/" + dbName, username, password);

			if (conn != null) {
				log.info("Connected!");
			} else {
				log.severe("Failed to make connection.");
			}

			return conn;

		} catch (SQLException e) {
			log.severe("Connection Failed! Check output console");
			log.severe(PostMonException.stackTraceToString(e));
			return null;

		}

	}

	private void verifyForName() {

		log.info("-------- PostgreSQL "
				+ "JDBC Connection Testing ------------");

		try {

			Class.forName("org.postgresql.Driver");

		} catch (ClassNotFoundException e) {
			log.severe("Where is your PostgreSQL JDBC Driver? "
					+ "Include in your library path!");
			log.severe(PostMonException.stackTraceToString(e));
			return;

		}

		log.info("PostgreSQL JDBC Driver Registered!");

	}

}