package org.example;

import java.sql.*;
import java.util.List;

public class ClientService {
    private static final String INSERT_INTO_CLIENT_TABLE =
            "INSERT INTO client(name) VALUES (?)";
    private static final String GET_BY_ID_FROM_CLIENT_TABLE =
            "SELECT name FROM client WHERE id = ?";
    private static final String UPDATE_CLIENT_NAME_TABLE =
            "UPDATE client SET name= ? WHERE id = ?";
    private static final String DELETE_FROM_CLIENT_TABLE =
            "DELETE FROM client WHERE id = ?";
    private static final String GET_ALL_FROM_CLIENT_TABLE =
            "SELECT * FROM client";

    public long create(String name) throws SQLException {
        long id = 1;
        Connection connection = Database.getInstance().getConnection();
        try {
            connection.setAutoCommit(false);
            if (name == null || name.length() <= 1 || name.length() >= 1000) {
                throw new SQLException("Invalid Client Name");
            }
            PreparedStatement preparedStatement = connection.prepareStatement(INSERT_INTO_CLIENT_TABLE,
                    Statement.RETURN_GENERATED_KEYS);
            preparedStatement.setString(1, name);
            preparedStatement.executeUpdate();
            ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
            while (generatedKeys.next()) {
                id = generatedKeys.getLong(1);
            }
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw new RuntimeException(e);
        } finally {
            connection.setAutoCommit(true);
        }
        return id;
    }

    public String getById(long id) throws SQLException {
        String resultReturn = null;
        Connection connection = Database.getInstance().getConnection();
        try {
            connection.setAutoCommit(false);
            if (id == 0) {
                throw new SQLException("Invalid Client Id");
            }
            PreparedStatement preparedStatement = connection.prepareStatement(GET_BY_ID_FROM_CLIENT_TABLE);
            preparedStatement.setLong(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                resultReturn = resultSet.getString(1);
            }
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw new RuntimeException(e);
        } finally {
            connection.setAutoCommit(true);
        }
        return resultReturn;
    }

    public void setName(long id, String name) throws SQLException {
        String resultReturn = null;
        Connection connection = Database.getInstance().getConnection();
        try {
            connection.setAutoCommit(false);
            if (id == 0) {
                throw new SQLException("Invalid Client Id");
            }
            if (name == null || name.length() <= 1 || name.length() >= 1000) {
                throw new SQLException("Invalid Client Name");
            }
            PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_CLIENT_NAME_TABLE);
            preparedStatement.setString(1, name);
            preparedStatement.setLong(2, id);
            preparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw new RuntimeException(e);
        } finally {
            connection.setAutoCommit(true);
        }
    }

    public void deleteById(long id) throws SQLException {
        Connection connection = Database.getInstance().getConnection();
        try {
            connection.setAutoCommit(false);
            if (id == 0) {
                throw new SQLException("Invalid Client Id");
            }
            PreparedStatement preparedStatement = connection.prepareStatement(DELETE_FROM_CLIENT_TABLE);
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw new RuntimeException(e);
        } finally {
            connection.setAutoCommit(true);
        }
    }

    public List<Client> listAll() throws SQLException {
        List<Client> listOfClients = new java.util.ArrayList<>(List.of());
        Connection connection = Database.getInstance().getConnection();
        try {
            connection.setAutoCommit(false);

            PreparedStatement preparedStatement = connection.prepareStatement(GET_ALL_FROM_CLIENT_TABLE);

            preparedStatement.executeQuery();

            ResultSet resultSet = preparedStatement.getResultSet();

            while (resultSet.next()) {
                listOfClients.add(new Client(resultSet.getObject(1,Long.class),
                        resultSet.getObject(2,String.class)));
            }

            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw new RuntimeException(e);
        } finally {
            connection.setAutoCommit(true);
        }

        return listOfClients;
    }
}
