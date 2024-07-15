import java.sql.*;

public class Main {
    private final static String PROTOCOL = "jdbc:postgresql://";
    private final static String DRIVER = "org.postgresql.Driver";
    private final static String URL_LOCALE_NAME = "localhost/";

    private final static String DATABASE_NAME = "Motorcyclists";

    private final static String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    private final static String USER_NAME = "postgres";
    private final static String DATABASE_PASSWORD = "admin";

    public static void main(String[] args) {
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASSWORD)){
//            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
//            getAllBrands(connection); System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
//            getModel(connection, "BMW"); System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
//            getAllUsers(connection); System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
//            getAllEngine(connection); System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
//            changeUser(connection, 1, "Ivan", "Zolo"); System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
//            deleteModel(connection, "S", "Black"); System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
//            changeVolume(connection, 1, 250); System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
//            changeColor(connection, 2, "Green"); System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
//            deleteUser(connection, 1); System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
//            addUser(connection, 2, "Никита", "", 3); System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
//            changePrice(connection, 3, 343344.99); System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            getMotorcycleByEngine(connection); System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            getCountEngineByHP(connection, 68);

        } catch (SQLException e) {
            if (e.getSQLState().startsWith("23")){
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }

    public static void checkDriver(){
        try{
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e){
            System.out.println("Нет JDBS-драйвера!");
            throw new RuntimeException(e);
        }
    }
    public static void checkDB(){
        try{
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASSWORD);
        } catch (SQLException e){
            System.out.println("Нет базы данных или неправильные данные пароля и логина");
            throw new RuntimeException(e);
        }
    }

    public static void getAllBrands(Connection connection) throws SQLException{
        String columnName0 = "brand";
        String param0 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT brand FROM characteristics");
        while (rs.next()){
            param0 = rs.getString(columnName0);
            System.out.println(param0);
        }
    }

    public static void getModel(Connection connection, String brand) throws SQLException{
        if (brand == null || brand.isBlank()) return;
        System.out.println(brand + ":");
        String columnName0 = "model";

        PreparedStatement statement = connection.prepareStatement("SELECT model " +
                "FROM characteristics " +
                "WHERE brand = ?");
        statement.setString(1, brand);
        ResultSet rs = statement.executeQuery();
        while (rs.next()){
            System.out.println(rs.getString(columnName0));
        }
    }

    public static void getAllUsers(Connection connection) throws SQLException{
        String columnName0 = "first_name";
        String columnName1 = "last_name";
        String columnName2 = "id_user";
        String columnName3 = "id_motorcycle";

        String param0 = null;
        String param1 = null;
        String param2 = null;
        String param3 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT id_user, first_name, last_name, id_motorcycle FROM users");

        while (rs.next()){
            param0 = rs.getString(columnName0);
            param1 = rs.getString(columnName1);
            param2 = rs.getString(columnName2);
            param3 = rs.getString(columnName3);
            System.out.println(param2 + "  |  " + param0 + " " + param1 + "  |  " + param3);
        }
    }

    public static void getAllEngine(Connection connection) throws SQLException{

        String columnName0 = "id";
        String columnName1 = "name";
        String columnName2 = "type";
        String columnName3 = "volume";
        String columnName4 = "hp";

        String param0 = null; String param1 = null; String param2 = null; String param3 = null; String param4 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT id, name, type, volume, hp FROM engine");

        while (rs.next()){
            param0 = rs.getString(columnName0);
            param1 = rs.getString(columnName1);
            param2 = rs.getString(columnName2);
            param3 = rs.getString(columnName3);
            param4 = rs.getString(columnName4);
            System.out.println(param0 + "  |  " + param1 + "  |  " + param2 + "  |  " + param3 + "  |  " + param4);
        }
    }

    public static void changeUser(Connection connection, int id_user, String first_name, String last_name) throws SQLException{
        if (id_user <= 0 || first_name == null || last_name == null || first_name.isBlank() ) return;


        PreparedStatement statement = connection.prepareStatement("UPDATE users SET first_name = ?, last_name = ? WHERE id_user = ?");
        statement.setString(1, first_name);
        statement.setString(2, last_name);
        statement.setInt(3, id_user);
        statement.executeUpdate();

        System.out.println("UPDATED");
    }

    public static void deleteModel(Connection connection, String model, String color)throws SQLException{
        if(model == null || model.isBlank() || color == null || color.isBlank()) return;
        PreparedStatement statement = connection.prepareStatement("DELETE FROM characteristics WHERE model = ? AND color = ?");
        statement.setString(1, model);
        statement.setString(2, color);
        statement.executeUpdate();

        System.out.println("DELETE");
    }

    public static void changeColor(Connection connection, int id, String color) throws SQLException{
        if(id <= 0) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE characteristics SET color = ? WHERE id = ?");
        statement.setString(1, color);
        statement.setInt(2, id);

        statement.executeUpdate();

        System.out.println("COLOR UPDATE");
    }

    public static void changeVolume(Connection connection, int id, int volume) throws SQLException{
        if (id <= 0 || volume <= 0) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE engine SET volume = ? WHERE id = ?");
        statement.setInt(1,volume);
        statement.setInt(2, id);
        statement.executeUpdate();

        System.out.println("VOLUME UPDATE");
    }

    public static void deleteUser(Connection connection, int id_user)throws SQLException{
        if (id_user <= 0) return;

        PreparedStatement statement = connection.prepareStatement("DELETE FROM users WHERE id_user = ?");
        statement.setInt(1, id_user);
        statement.executeUpdate();

        System.out.println("USER DELETE");
    }

    public static void addUser(Connection connection, int id_user, String first_name, String last_name, int id_motorcycle)throws SQLException{
        if (id_user <= 0 || first_name == null || first_name.isBlank() || id_motorcycle <= 0) return;

        PreparedStatement statement = connection.prepareStatement("INSERT INTO users (id_user, first_name, last_name, id_motorcycle) VALUES (?, ?, ?, ?)");
        statement.setInt(1, id_user);
        statement.setString(2, first_name);
        statement.setString(3, last_name);
        statement.setInt(4, id_motorcycle);
        statement.executeUpdate();
        System.out.println(id_user + "  |  " + first_name + " " + last_name + "  |  " + id_motorcycle );
    }

    public static void changePrice(Connection connection, int id, double price) throws SQLException{
        if (id <= 0 || price < 0) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE characteristics SET price = ? WHERE id = ?");
        statement.setDouble(1, price);
        statement.setInt(2, id);
        statement.executeUpdate();

        System.out.println("PRICE UPDATE");
    }
    public static void getMotorcycleByEngine(Connection connection)throws SQLException{
        String columnName0 = "brand";
        String columnName1 = "model";
        String columnName2 = "color";
        String columnName3 = "price";
        String columnName4 = "name";

        String param0 = null; String param1 = null; String param2 = null; String param3 = null; String param4 = null;

        PreparedStatement statement = connection.prepareStatement(
                "SELECT characteristics.brand, characteristics.model, characteristics.color, characteristics.price, engine.name " +
                        "FROM characteristics " +
                        "JOIN engine ON characteristics.id_engine = engine.id"
        );

        ResultSet rs = statement.executeQuery();
        while (rs.next()){
            param0 = rs.getString(columnName0);
            param1 = rs.getString(columnName1);
            param2 = rs.getString(columnName2);
            param3 = rs.getString(columnName3);
            param4 = rs.getString(columnName4);
            System.out.println(param0 + "  |  " + param1 + "  |  " + param2 + "  |  " + param3 + "  |  " + param4);
        }
    }

    public static void getCountEngineByHP(Connection connection, int HP)throws SQLException{
        if (HP <= 0) return;
        int count = 0;
        PreparedStatement statement = connection.prepareStatement("SELECT hp FROM engine WHERE hp >= ?");
        statement.setInt(1, HP);
        ResultSet rs = statement.executeQuery();
        while(rs.next()){
            count += 1;
        }
        System.out.println(count);
    }
}