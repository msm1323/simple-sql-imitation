package com.digdes.school;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class App {

    /**
     * Комментарии к реализации:
     * - при инициализации таблицы имена колонок задаются в нижнем регистре
     * - в имени колонки и строковых значениях не может быть символа одинарной кавычки
     * - поддерживается только один вид логического оператора в выражении
     *
     * Также предусмотрен костыль на случий юнит тестов для метода execute(String request)
     * с вашей стороны - автоинициализация таблицы колонками из примера
     */

    public static void main(String[] args) {

        Map<String, Class<?>> tableColumns = new HashMap<>(5);
        tableColumns.put("id", Long.class);
        tableColumns.put("lastname", String.class);
        tableColumns.put("age", Long.class);
        tableColumns.put("cost", Double.class);
        tableColumns.put("active", Boolean.class);

        try {
            JavaSchoolStarter starter = new JavaSchoolStarter();
            starter.init(tableColumns);

            //Вставка строки в коллекцию
            List<Map<String, Object>> result1 = starter.execute("INSERT VALUES 'lastName' = 'Федоров' , 'id'=3, 'age'=40, 'active'=true");
            System.out.println(result1);
            List<Map<String, Object>> result2 = starter.execute("INSERT VALUES 'lastName' = 'Петров', 'cost'=17.1 , 'id'=1, 'age'=30, 'active'=true");
            System.out.println(result2);
            List<Map<String, Object>> result3 = starter.execute("INSERT VALUES 'active'=false, 'id'=2, 'age'=25, 'lastName' = 'Иванов' ");
            System.out.println(result3);

            //Изменение значения которое выше записывали
            List<Map<String, Object>> result4 = starter.execute("UPDATE VALUES 'active'=false, 'cost'=10.1 where 'cost' > 5 and 'active'=true" );
            System.out.println("result4 = " + result4);

            //Получение всех данных из коллекции (т.е. в данном примере вернется 1 запись)
            List<Map<String, Object>> result5 = starter.execute("SELECT where 'cost' = 10.1");
            System.out.println("result5 = " + result5);

            List<Map<String, Object>> result6 = starter.execute("delete where 'cost' = 10.1");
            System.out.println("result6 = " + result6);

            System.out.println();
            starter.printTable();
            System.out.println();

        } catch (Exception ex) {
            ex.printStackTrace();
        }


    }
}
