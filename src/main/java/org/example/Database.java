package org.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;

import java.sql.Connection;
import java.sql.SQLException;
public class Database {
    private static final String DB_URL = "jdbc:postgresql://localhost:5412/java-db";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "password";
    private static final HikariConfig config = new HikariConfig();
    private static final HikariDataSource ds;
    private static Database database;

    static {
        config.setJdbcUrl(DB_URL);
        config.setUsername(DB_USER);
        config.setPassword(DB_PASSWORD);
        ds = new HikariDataSource(config);
        Flyway flyway = Flyway.configure()
                .dataSource(ds)
                .locations("db/migrations")
                .load();
        flyway.migrate();
    }

    private Database() {
    }

    public static Database getInstance() {
        if (database == null) {
            database = new Database();
        }
        return database;
    }


    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

}
