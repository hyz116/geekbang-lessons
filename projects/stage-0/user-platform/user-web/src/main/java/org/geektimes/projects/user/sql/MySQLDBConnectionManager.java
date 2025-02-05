package org.geektimes.projects.user.sql;

import com.google.common.collect.Lists;
import org.geektimes.projects.user.domain.User;

import java.io.PrintWriter;
import java.sql.*;
import java.util.Collections;
import java.util.List;

/**
 * MySQL 版本DB链接管理器
 *
 * @author <a href="mailto:huangyuze.hyz@taobao.com">chuye</a> <br>
 * @date 2025/2/5 13:41
 * @since 1.0.0
 */
public class MySQLDBConnectionManager {
    private static String DATABASE_URL = "jdbc:mysql://127.0.0.1:3304/user_center";
    private static String USERNAME = "root";
    private static String PASSWORD = "hyz992116";


    public static void main(String[] args) throws Exception {
        Connection connection;

        // get Connection
        connection = getConnection();

        System.out.println("------------------------");

        // add some record
//        executeUpdate(connection);

        // query all record
//        executeQuery(connection);

        // query by id
//        System.out.println("-------query by condition--------");
//        executeQueryWithCondition(1L, connection);

        // update record
//        executeUpdateRecord(4L, connection);
//        System.out.println("-------after update--------");
//        executeQueryWithCondition(4L, connection);

        // clearing parameters
//        executeQueryForInStatement(connection);

        // metaData
//        showMetaData(connection);

        // SQLWarning
        showSQLWarning(connection);

        // close
        close(connection);
    }

    private static void showSQLWarning(Connection connection) throws SQLException {
        /**
         * SQLException用于处理必须立即解决的严重错误
         * SQLWarning用于处理可以容忍但需要注意的警告信息
         * 两者设计体现了异常设计的分级思想，使得程序可以更为灵活的处理不同级别的问题
         */

        try {
            PreparedStatement preparedStatement = connection.prepareStatement("UPDATE member set name = 'chuye' WHERE ID = ?");

            // 执行前置检查警告
            checkWarning(connection.getWarnings());

            preparedStatement.setObject(1, 1L);
            int result = preparedStatement.executeUpdate();

            checkWarning(preparedStatement.getWarnings());
            System.out.println("result: " + result);

            preparedStatement.close();
        } catch (SQLException e) {
            // 处理严重的错误
            System.err.println("Error: " + e.getMessage());
        }

    }

    private static void checkWarning(SQLWarning sqlWarning) {
        while (sqlWarning != null) {
            System.out.println("Warning: " + sqlWarning.getMessage());
            System.out.println("SQLState: " + sqlWarning.getSQLState());
            System.out.println("ErrorCode: " + sqlWarning.getErrorCode());

            sqlWarning = sqlWarning.getNextWarning();
        }
    }

    private static void showMetaData(Connection connection) throws SQLException {
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        System.out.println("URL: " + databaseMetaData.getURL());
        System.out.println("driverName: " + databaseMetaData.getDriverName());
        System.out.println("driverVersion: " + databaseMetaData.getDriverVersion());
        System.out.println("jdbcMajorVersion: " + databaseMetaData.getJDBCMajorVersion());
        System.out.println("jdbcMinorVersion: " + databaseMetaData.getJDBCMinorVersion());
        System.out.println("userName :" + databaseMetaData.getUserName());

        System.out.println("---------------------------------------------");
        String queryUserDMLSQL = "SELECT id,name,password,email,phoneNumber FROM users WHERE id = ? and name like ?";
        PreparedStatement statement = connection.prepareStatement(queryUserDMLSQL);
        statement.setObject(1, 2L);
        statement.setObject(2, "%C%");

        //ResultSet resultSet = statement.executeQuery(queryUserDMLSQL);
        // ParameterMetaData
        System.out.println("-----------------------------------");
//        ResultSetMetaData resultSetMetaData = statement.getMetaData();
        // mysql have error todo
//        ParameterMetaData parameterMetaData = statement.getParameterMetaData();
//        int parameterCount = parameterMetaData.getParameterCount();
//        for (int i = 1; i <= parameterCount; i++) {
//            System.out.println("parameter className: " + parameterMetaData.getParameterClassName(i));
//            System.out.println("parameter type: " + parameterMetaData.getParameterType(i));
//            System.out.println("parameter type name: " + parameterMetaData.getParameterTypeName(i));
//            System.out.println("parameter mode: " + parameterMetaData.getParameterMode(i));
//            System.out.println("***********************");
//        }


        // ResultSetMetaData
        ResultSetMetaData resultSetMetaData = statement.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        System.out.println("columnCount : " + columnCount);
        for (int i = 1; i <= columnCount; i++) {
            System.out.println("colName: " + resultSetMetaData.getColumnName(i));
            System.out.println("colType: " + resultSetMetaData.getColumnType(i));
            System.out.println("colTypeName: " + resultSetMetaData.getColumnTypeName(i));
            System.out.println("colClassName: " + resultSetMetaData.getColumnClassName(i));
            System.out.println("colLabel: " + resultSetMetaData.getColumnLabel(i));
            System.out.println("col is autoIncrement: " + resultSetMetaData.isAutoIncrement(i));
            System.out.println("col is nullable: " + resultSetMetaData.isNullable(i));
            System.out.println("col isCaseSensitive: " + resultSetMetaData.isCaseSensitive(i));
            System.out.println("col catalogName: " + resultSetMetaData.getCatalogName(i));
            System.out.println("schemaName: " + resultSetMetaData.getSchemaName(i));
            System.out.println("tableName" + resultSetMetaData.getTableName(i));
            System.out.println("*******************");
        }
    }

    private static void executeQueryForInStatement(Connection connection) throws SQLException {
        List<Long> ids = Lists.newArrayList(1L, 2L);
        // Create placeholders (?,?,?) based on list size
        String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));

        String queryUserDMLSQL = "SELECT id,name,password,email,phoneNumber FROM users WHERE id in (" + placeholders + ")";
        PreparedStatement preparedStatement = connection.prepareStatement(queryUserDMLSQL);
        for (int i = 1; i <= ids.size(); i++) {
            preparedStatement.setObject(i, ids.get(i - 1));
        }
        ResultSet resultSet = preparedStatement.executeQuery();
        printUserInfo(resultSet);

        // 修改查询条件
        ids = Lists.newArrayList(3L, 4L);

        // clear parameters
        preparedStatement.clearParameters();

        System.out.println("----------------------");
        for (int i = 1; i <= ids.size(); i++) {
            preparedStatement.setObject(i, ids.get(i - 1));
        }
        resultSet = preparedStatement.executeQuery();
        printUserInfo(resultSet);

        preparedStatement.close();
    }

    private static void executeUpdateRecord(Long id, Connection connection) throws SQLException {
        String updateUserDMLSQL = "UPDATE users set password = ? WHERE id = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(updateUserDMLSQL);
        preparedStatement.setString(1, "123456");
//        preparedStatement.setObject(1, "123456");
//        preparedStatement.setObject(2, 4);
        preparedStatement.setLong(2, id);
        int result = preparedStatement.executeUpdate();
        System.out.println("result is " + result);
        preparedStatement.close();
    }

    public static void executeQueryWithCondition(Long id, Connection connection) throws SQLException {
        String queryUserDMLSQL = "SELECT id,name,password,email,phoneNumber FROM users WHERE id = ?";

        PreparedStatement preparedStatement = connection.prepareStatement(queryUserDMLSQL);
        preparedStatement.setObject(1, id);

        ResultSet resultSet = preparedStatement.executeQuery();
        printUserInfo(resultSet);

        preparedStatement.close();
    }


    public static void executeUpdate(Connection connection) throws SQLException {
        String insertUserDMLSQL = "INSERT INTO users(name,password,email,phoneNumber) VALUES " +
                "('A', '*********', 'a@gmail.com', '1')" +
                ",('B', '*********', 'b@gmail.com', '2')" +
                ",('C', '*********', 'c@gmail.com', '3')" +
                ",('D', '*********', 'd@gmail.com', '4')";

        Statement statement = connection.createStatement();
        int result = statement.executeUpdate(insertUserDMLSQL);
        System.out.println("result is " + result);
        statement.close();
    }

    public static void executeQuery(Connection connection) throws SQLException {
        String queryUserDMLSQL = "SELECT id,name,password,email,phoneNumber FROM users";

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(queryUserDMLSQL);
        printUserInfo(resultSet);

        // 先打开的后关闭
        statement.close();
    }


    private static Connection getConnection() throws SQLException {
        String mysqlJDBDriverClassName = "com.mysql.cj.jdbc.Driver";
        // 项目已经使用了derby 数据库，在文件“src/main/resources/META-INF/services/java.sql.Driver”
        // 中指定了org.apache.derby.jdbc.EmbeddedDriver，此处通过代码执行MySQL驱动


        //方式一 通过设置系统属性“jdbc.drivers”
//        System.setProperty("jdbc.drivers", mysqlJDBDriverClassName);

        // 方式二：通过SPI加载
        // MySQL驱动jar中已经设置SPI相关文件 /mysql-connector-j/9.2.0/mysql-connector-j-9.2.0.jar!/META-INF/services/java.sql.Driver
        // 参考文件：src/main/resources/META-INF/services/java.sql.Driver

        // 方式三：显示
//        Class.forName(mysqlJDBDriverClassName);

        DriverManager.setLogWriter(new PrintWriter(System.out));

        Connection connection = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);

        System.out.printf("connection is: %s\n", connection.toString());

        return connection;
    }

    public static void close(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }


    private static void printUserInfo(ResultSet resultSet) throws SQLException {
        if (resultSet == null) {
            return;
        }

        User user;
        while (resultSet.next()) {
            user = new User();
            user.setId(resultSet.getLong("id"));
            user.setName(resultSet.getString("name"));
            user.setPassword(resultSet.getString("password"));
            user.setEmail(resultSet.getString("email"));
            user.setPhoneNumber(resultSet.getString("phoneNumber"));

            System.out.println(user);
        }

        resultSet.close();
    }
}
