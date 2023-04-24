package com.digdes.school;

import java.lang.reflect.Field;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataManagementLanguageImpl {

    private final Map<String, Class<?>> columnsTypes;
    private final List<Map<String, Object>> table;
    private final String valuesRgx, insertRgx, updateRgx, selectRgx, deleteRgx;

    private static final String columnNameRgx, columnValueRgx, pairRgx, conditionRgx, subConditionRgx;
    private static final String comparisonStringPairRgx, comparisonDigitPairRgx;
    private static final String stringValueRgx, digitValueRgx, longValueRgx, boolValueRgx;
    private static final String logicalOpsRgx, comparisonOpsRgx;
    private static String exQueryMessage = "Некорректный запрос: ";

    static {
        stringValueRgx = "'[^']+'";
        digitValueRgx = "\\d+(\\.\\d+)?";
        longValueRgx = "\\d+";
        boolValueRgx = "(true|false)";

        columnNameRgx = "'[^']+'";
        columnValueRgx = "(" + stringValueRgx + "|" + digitValueRgx + "|" + boolValueRgx + "|null)";
        pairRgx = columnNameRgx + "\\s*=\\s*" + columnValueRgx;

        logicalOpsRgx = "(AND|OR)";
        comparisonOpsRgx = "((=|!=)|i?like|(>|<)=?)";

        String comparisonPairRgx = columnNameRgx + "\\s*(=|!=)\\s*" + columnValueRgx;
        comparisonStringPairRgx = columnNameRgx + "\\s*i?like\\s*" + stringValueRgx;
        comparisonDigitPairRgx = columnNameRgx + "\\s*(>|<)=?\\s*" + digitValueRgx;

        subConditionRgx = String.format("(%s|%s|%s)", comparisonPairRgx, comparisonStringPairRgx, comparisonDigitPairRgx);
        conditionRgx = "\\s+where\\s+" + subConditionRgx + "(\\s+" + logicalOpsRgx + "\\s+" + subConditionRgx + ")*";

    }

    DataManagementLanguageImpl(Map<String, Class<?>> columnsTypes) {
        this.columnsTypes = columnsTypes;
        valuesRgx = "\\s+values\\s+" + pairRgx + "(\\s*,\\s*" + pairRgx + "){0," + (columnsTypes.size() - 1) + "}\\s*";
        insertRgx = "(?iu)\\s*INSERT" + valuesRgx;
        updateRgx = "(?iu)\\s*UPDATE" + valuesRgx + "(" + conditionRgx + ")?\\s*";
        selectRgx = "(?iu)\\s*SELECT" + "(" + conditionRgx + ")?\\s*";
        deleteRgx = "(?iu)\\s*DELETE" + "(" + conditionRgx + ")?\\s*";
        table = new ArrayList<>();
    }

    String getRgx(String name) {
        if (!name.contains("Rgx")) {
            System.err.println("Класс не предоставляет доступ к этому полю или же поля с таким именем в классе нет.");
            return null;
        }
        try {
            Class<?> clazz = this.getClass();
            Field field = clazz.getDeclaredField(name);
            return (String) field.get(this);
        } catch (NoSuchFieldException ex) {
            System.err.println("Поля с таким именем в классе нет.");
            return null;
        } catch (IllegalAccessException e) {
            System.err.println(e.getMessage());
            return null;
        }
    }

    public List<Map<String, Object>> insert(String query) throws Exception {
        validateByRgx(query, insertRgx, exQueryMessage + query);
        Map<String, Object> newRow = getRowWithNewValues(query);
        if (isAllValuesNull(newRow)) {
            throw new Exception("Все значенияв новой записи не могут быть пустыми!");
        }
        table.add(newRow);
        List<Map<String, Object>> insertRes = new ArrayList<>();
        insertRes.add(newRow);
        return insertRes;
    }

    public List<Map<String, Object>> update(String query) throws Exception {
        validateByRgx(query, updateRgx, exQueryMessage + query);

        List<Map<String, Object>> updatedRows = new ArrayList<>();
        Map<String, Object> newValuesRow = getRowWithNewValues(query);
        Iterator<Map<String, Object>> it = table.iterator();

        if (!hasConditions(query)) {
            while (it.hasNext()) {
                Map<String, Object> row = it.next();
                row.putAll(newValuesRow);
                if (isAllValuesNull(row)) {
                    it.remove();
                }
            }
            updatedRows = table;
            return updatedRows;
        }

        List<ArrayList<String>> subConditionsListsToSum = conditionDecomposition(query);
        while (it.hasNext()) {
            Map<String, Object> row = it.next();
            for (ArrayList<String> subConditionsList : subConditionsListsToSum) {
                int i;
                for (i = 0; i < subConditionsList.size(); i++) {
                    if (!conditionFulfillment(row, subConditionsList.get(i))) {
                        break;
                    }
                }
                if (i == subConditionsList.size()) {
                    row.putAll(newValuesRow);
                    updatedRows.add(row);
                    if (isAllValuesNull(row)) {
                        it.remove();
                    }
                }
            }
        }
        return updatedRows;
    }

    public List<Map<String, Object>> delete(String query) throws Exception {
        validateByRgx(query, deleteRgx, exQueryMessage + query);
        List<Map<String, Object>> deletedRows = new ArrayList<>();
        if (!hasConditions(query)) {
            deletedRows = table;
            table.clear();
            return deletedRows;
        }

        List<ArrayList<String>> subConditionsListsToSum = conditionDecomposition(query);
        ListIterator<Map<String, Object>> it = table.listIterator();
        while (it.hasNext()) {
            Map<String, Object> row = it.next();
            for (ArrayList<String> subConditionsList : subConditionsListsToSum) {
                int i;
                for (i = 0; i < subConditionsList.size(); i++) {
                    if (!conditionFulfillment(row, subConditionsList.get(i))) {
                        break;
                    }
                }
                if (i == subConditionsList.size()) {
                    deletedRows.add(row);
                    it.remove();
                }
            }
        }
        return deletedRows;
    }

    public List<Map<String, Object>> select(String query) throws Exception {
        validateByRgx(query, selectRgx, exQueryMessage + query);

        if (!hasConditions(query)) {
            return table;
        }
        List<Map<String, Object>> selectedRows = new ArrayList<>();
        List<ArrayList<String>> subConditionsListsToSum = conditionDecomposition(query);
        for (Map<String, Object> row : table) {
            for (ArrayList<String> subConditionsList : subConditionsListsToSum) {
                int i;
                for (i = 0; i < subConditionsList.size(); i++) {
                    if (!conditionFulfillment(row, subConditionsList.get(i))) {
                        break;
                    }
                }
                if (i == subConditionsList.size()) {
                    selectedRows.add(row);
                }
            }
        }
        return selectedRows;
    }

    private List<ArrayList<String>> conditionDecomposition(String query) {
        Matcher conditionM = getMatcher(conditionRgx, query);
        conditionM.find();
        String condition = conditionM.group();
        Matcher subConditionM = getMatcher(subConditionRgx, condition);
        Matcher logOpM = getMatcher(logicalOpsRgx, condition);

        ArrayList<String> logOps = new ArrayList<>();
        while (logOpM.find()) {
            logOps.add(logOpM.group());
        }

        ArrayList<String> subConditions = new ArrayList<>();
        while (subConditionM.find()) {
            subConditions.add(subConditionM.group());
        }

        List<ArrayList<String>> subConditionsListsToSum = new ArrayList<>();
        ArrayList<String> subConditionsToMultiply = new ArrayList<>();

        subConditionsToMultiply.add(subConditions.get(0));
        subConditionsListsToSum.add(subConditionsToMultiply);
        for (int i = 0; i < logOps.size(); i++) {
            if (logOps.get(i).equalsIgnoreCase("AND")) {
                subConditionsToMultiply.add(subConditions.get(i + 1));
            } else {
                subConditionsToMultiply = new ArrayList<>();
                subConditionsListsToSum.add(subConditionsToMultiply);
                subConditionsToMultiply.add(subConditions.get(i + 1));
            }
        }
        return subConditionsListsToSum;
    }

    public void printTable() {
        table.forEach(System.out::println);
    }

    private boolean isAllValuesNull(Map<String, Object> row) {
        for (Object value : row.values()) {
            if (value != null) {
                return false;
            }
        }
        return true;
    }

    private Map<String, Object> getRowWithNewValues(String query) throws Exception {
        Map<String, Object> newRow = new HashMap<>();
        Matcher valuesM = getMatcher(valuesRgx, query);
        valuesM.find();
        String values = valuesM.group();
        Matcher pairRgxM = getMatcher(pairRgx, values);
        while (pairRgxM.find()) {
            String curPair = pairRgxM.group();

            String columnName = getColumn(curPair);
            Object value = getValue(curPair, columnName);

            newRow.put(columnName, value);
        }
        return newRow;
    }

    private boolean hasConditions(String query) { //todo тогда там, где оно есть, будет производится повторный поиск мэтчера
        Matcher whereM = getMatcher(conditionRgx, query);
        return whereM.find();
    }

    private boolean conditionFulfillment(Map<String, Object> row, String curSubCondition) throws Exception {
        String columnName = getColumn(curSubCondition);

        Matcher comparisonOpM = getMatcher(comparisonOpsRgx, curSubCondition);
        comparisonOpM.find();
        String comparisonOp = comparisonOpM.group();

        Object value = getValue(curSubCondition, columnName);
        if (value == null) {
            throw new RuntimeException("\"null\" не может использоваться в качестве значения для сравнения!");
        }

        Object rowValue = row.get(columnName);
        if (rowValue == null) {
            return false;
        }

        if (comparisonOp.equals("=")) {
            return rowValue.equals(value);
        }
        if (comparisonOp.equals("!=")) {
            return !rowValue.equals(value);
        }

        String exOpMessage = "Применения оператора \"" + comparisonOp + "\" недоступно для колонки '" + columnName + "'.";
        if (comparisonOp.matches("i?like")) {
            validateByRgx(curSubCondition, comparisonStringPairRgx, exOpMessage);
            String valueRgx = value.toString();
            if (valueRgx.charAt(0) == '%') {
                valueRgx = ".*" + valueRgx.substring(1);
            }
            if (valueRgx.charAt(valueRgx.length() - 1) == '%') {
                valueRgx = valueRgx.substring(0, valueRgx.length() - 1) + ".*";
            }
            if (comparisonOp.equalsIgnoreCase("ilike")) {
                valueRgx = "(?iu)" + valueRgx;
            }
            return rowValue.toString().matches(valueRgx);
        }

        if (comparisonOp.matches("[><]=?")) {
            validateByRgx(curSubCondition, comparisonDigitPairRgx, exOpMessage);
            boolean isDouble = columnsTypes.get(columnName).getSimpleName().equals("Double");
            switch (comparisonOp) {
                case "<":
                    return isDouble ? (Double) rowValue < (Double) value : (Long) rowValue < (Long) value;
                case "<=":
                    return isDouble ? (Double) rowValue <= (Double) value : (Long) rowValue <= (Long) value;
                case ">":
                    return isDouble ? (Double) rowValue > (Double) value : (Long) rowValue > (Long) value;
                case ">=":
                    return isDouble ? (Double) rowValue >= (Double) value : (Long) rowValue >= (Long) value;
            }
        }
        return false;
    }

    private Matcher getMatcher(String rgx, String source) {  //todo сделать пометку в названии или агруметы флагов?
        return Pattern.compile(rgx, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE).matcher(source);
    }

    private String getColumn(String source) throws Exception {
        Matcher columnM = getMatcher(columnNameRgx, source);
        columnM.find();
        String columnName = columnM.group().substring(1, columnM.end() - 1).toLowerCase();
        validateColumn(columnName);
        return columnName;
    }

    private Object getValue(String source, String columnName) throws Exception {
        source = source.replaceFirst("(?iu)'" + columnName + "'", "");
        Matcher valueM = getMatcher(columnValueRgx, source);
        valueM.find();
        String value = valueM.group();
        if (value.equals("null")) {
            return null;
        }
        String valueClassName = columnsTypes.get(columnName).getSimpleName();
        String exValMessage = "Неккоректный запрос - неверный тип значения \"" + value + "\" для колонки '" + columnName + "'";
        switch (valueClassName) {
            case "Long":
                validateByRgx(value, longValueRgx, exValMessage);
                return Long.parseLong(value);
            case "Double":
                validateByRgx(value, digitValueRgx, exValMessage);
                return Double.parseDouble(value);
            case "Boolean":
                validateByRgx(value, boolValueRgx, exValMessage);
                return Boolean.parseBoolean(value);
            default:
                validateByRgx(value, stringValueRgx, exValMessage);
                return value;
        }
    }

    private void validateColumn(String columnName) throws Exception {
        if (!columnsTypes.containsKey(columnName)) {
            throw new Exception("Неккоректный запрос - колонки '" + columnName + "' в таблице нет.");
        }
    }

    public static void validateByRgx(String str, String regex, String exMessage) throws Exception {
        if (!str.matches(regex)) {
            System.err.println("РВ:" + regex);
            throw new Exception(exMessage);
        }
    }

}