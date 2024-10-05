package org.example;

import org.example.dto.LongestProject;
import org.example.dto.MaxProjectCountClient;
import org.example.dto.ProjectPrice;
import org.example.dto.YoungestAndEldestWorker;
import org.example.utility.SqlFileReaderUtility;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class DatabaseQueryService {

    public LongestProject findLongestProject() throws Exception {
        LongestProject longestProject =new LongestProject();
        Path path= Path.of("sql/find_longest_project.sql");
        SqlFileReaderUtility sqlFileReaderUtility = new SqlFileReaderUtility();
        String sqlSript = sqlFileReaderUtility.convertSqlScriptToString(path);
        Connection connection=Database.getInstance().getConnection();
        try (PreparedStatement statement = connection.prepareStatement(sqlSript)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                longestProject.setId(resultSet.getLong("id"));
                longestProject.setStartDate(resultSet.getString("start_date"));
                longestProject.setFinishDate(resultSet.getString("finish_date"));
                longestProject.setDurationInMonths(resultSet.getLong("duration_in_months"));
            }
        }
        return longestProject;
    }
    public List<MaxProjectCountClient> maxProjectCountClientList() throws Exception {
        List<MaxProjectCountClient> maxProjectCountClientList=new ArrayList<>() ;
        Path path= Path.of("sql/find_max_projects_client.sql");
        SqlFileReaderUtility sqlFileReaderUtility = new SqlFileReaderUtility();
        String sqlSript = sqlFileReaderUtility.convertSqlScriptToString(path);
        Connection connection=Database.getInstance().getConnection();
        try (PreparedStatement statement = connection.prepareStatement(sqlSript)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                MaxProjectCountClient maxProjectCountClient=new MaxProjectCountClient();
                maxProjectCountClient.setClientName(resultSet.getString("name"));
                maxProjectCountClient.setProjectQuantity(resultSet.getLong("project_count"));
                maxProjectCountClientList.add(maxProjectCountClient);
            }
        }
        return maxProjectCountClientList;
    }
    public List<YoungestAndEldestWorker> findYoungestAndEldestWorkers() throws Exception {
        List<YoungestAndEldestWorker> youngestAndEldestWorkers=new ArrayList<>() ;
        Path path= Path.of("sql/find_youngest_eldest_workers.sql");
        SqlFileReaderUtility sqlFileReaderUtility = new SqlFileReaderUtility();
        String sqlSript = sqlFileReaderUtility.convertSqlScriptToString(path);
        Connection connection=Database.getInstance().getConnection();
        try (PreparedStatement statement = connection.prepareStatement(sqlSript)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                YoungestAndEldestWorker youngestAndEldestWorker=new YoungestAndEldestWorker();
                youngestAndEldestWorker.setType(resultSet.getString("type"));
                youngestAndEldestWorker.setName(resultSet.getString("name"));
                youngestAndEldestWorker.setYearOfBirth(resultSet.getLong("birthday"));
                youngestAndEldestWorkers.add(youngestAndEldestWorker);
            }
        }
        return youngestAndEldestWorkers;
    }
    public List<ProjectPrice> findProjectPrice() throws Exception {
        List<ProjectPrice> projectPrices=new ArrayList<>() ;
        Path path= Path.of("sql/print_project_prices.sql");
        SqlFileReaderUtility sqlFileReaderUtility = new SqlFileReaderUtility();
        String sqlSript = sqlFileReaderUtility.convertSqlScriptToString(path);
        Connection connection=Database.getInstance().getConnection();
        try (PreparedStatement statement = connection.prepareStatement(sqlSript)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                ProjectPrice projectPrice =new ProjectPrice();
                projectPrice.setProjectId(resultSet.getLong("project_id"));
                projectPrice.setStartDate(resultSet.getString("start_date"));
                projectPrice.setFinishDate(resultSet.getString("finish_date"));
                projectPrice.setProjectCost(resultSet.getLong("project_cost"));
                projectPrices.add(projectPrice);
            }
        }
        return projectPrices;
    }


    public static void main(String[] args) throws Exception {
        DatabaseQueryService databaseQueryService = new DatabaseQueryService();
        ClientService clientService=new ClientService();
//        LongestProject longestProject = databaseQueryService.findLongestProject();
//        System.out.println(longestProject.getId());
//        System.out.println(longestProject.getDurationInMonths());
//        System.out.println(longestProject.getFinishDate());
//        System.out.println(longestProject.getStartDate());
//        List<MaxProjectCountClient> maxProjectCountClientList = databaseQueryService.maxProjectCountClientList();
//        maxProjectCountClientList.forEach(maxProjectCountClient->System.out.println(maxProjectCountClient.getClientName()));
//        List<YoungestAndEldestWorker> youngestAndEldestWorkers = databaseQueryService.findYoungestAndEldestWorkers();
//        youngestAndEldestWorkers.forEach(youngestAndEldestWorker -> System.out.println(youngestAndEldestWorker.getType()));
//        List<ProjectPrice> projectPrice = databaseQueryService.findProjectPrice();
//        projectPrice.forEach(System.out::println);
//        System.out.println(clientService.create("vano"));
//        System.out.println(clientService.getById(3));
        clientService.setName(6,"nikita");
    }
}
