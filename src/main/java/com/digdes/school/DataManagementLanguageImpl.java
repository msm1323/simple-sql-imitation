package com.digdes.school;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataManagementLanguageImpl {

    private final Map<String, Class<?>> columnsTypes;
    private final List<Map<String, Object>> table;

    String insertRgx, updateRgx, selectRgx, deleteRgx;
    String columnNameRgx, columnValueRgx, pairRgx, valuesRgx, conditionRgx, subConditionRgx;
    String comparisonPairRgx, comparisonStringPairRgx, comparisonDigitPairRgx;
    String stringValueRgx, digitValueRgx, longValueRgx, boolValueRgx;
    String logicalOpsRgx, comparisonOpsRgx;

    {

        stringValueRgx = "'[^']+'";
        digitValueRgx = "\\d+(\\.\\d+)?";
        longValueRgx = "\\d+";
        boolValueRgx = "(true|false)";

        columnNameRgx = "'[^']+'";
        columnValueRgx = "(" + stringValueRgx + "|" + digitValueRgx + "|" + boolValueRgx + "|null)";
        pairRgx = columnNameRgx + "\\s*=\\s*" + columnValueRgx;

        logicalOpsRgx = "(AND|OR)";
        comparisonOpsRgx = "((=|!=)|i?like|(>|<)=?)";

        comparisonPairRgx = columnNameRgx + "\\s*(=|!=)\\s*" + columnValueRgx;
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

    public List<Map<String, Object>> insert(String query) throws Exception {
        validateQuery(query, insertRgx);
        Map<String, Object> newRow = getRowWithNewValues(query);
        table.add(newRow);
        List<Map<String, Object>> insertRes = new ArrayList<>();
        insertRes.add(newRow);
        return insertRes;
    }

    public List<Map<String, Object>> update(String query) throws Exception {
        validateQuery(query, updateRgx);

        List<Map<String, Object>> updatedRows = new ArrayList<>();
        Map<String, Object> newValuesRow = getRowWithNewValues(query);
        if (!hasConditions(query)) {
            table.forEach(map -> map.putAll(newValuesRow));
            updatedRows = table;
            return updatedRows;
        }

        List<ArrayList<String>> subConditionsListsToSum = conditionProcess(query);
        for (Map<String, Object> row : table) {
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
                }
            }
        }
        return updatedRows;
    }

    public List<Map<String, Object>> delete(String query) throws Exception {
        validateQuery(query, deleteRgx);
        List<Map<String, Object>> deletedRows = new ArrayList<>();
        if (!hasConditions(query)) {
            deletedRows = table;
            table.clear();
            return deletedRows;
        }

        List<ArrayList<String>> subConditionsListsToSum = conditionProcess(query);
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
        validateQuery(query, selectRgx);

        if (!hasConditions(query)) {
            return table;
        }
        List<Map<String, Object>> selectedRows = new ArrayList<>();
        List<ArrayList<String>> subConditionsListsToSum = conditionProcess(query);
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

    private List<ArrayList<String>> conditionProcess(String query) {
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
        if (value == null) {    //todo могу это контролить на начальном уровне валидации запроса через рв
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

        if (comparisonOp.matches("i?like")) {
            validateOperatorByColumnType(curSubCondition, comparisonStringPairRgx, comparisonOp, columnName);
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
            validateOperatorByColumnType(curSubCondition, comparisonDigitPairRgx, comparisonOp, columnName);
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
        switch (valueClassName) {
            case "Long":
                validateValue(longValueRgx, columnName, value);
                return Long.parseLong(value);
            case "Double":
                validateValue(digitValueRgx, columnName, value);
                return Double.parseDouble(value);
            case "Boolean":
                validateValue(boolValueRgx, columnName, value);
                return Boolean.parseBoolean(value);
            default:
                validateValue(stringValueRgx, columnName, value);
                return value;
        }
    }

    private void validateColumn(String columnName) throws Exception {
        if (!columnsTypes.containsKey(columnName)) {
            throw new Exception("Неккоректный запрос - колонки '" + columnName + "' в таблице нет.");
        }
    }

    private void validateValue(String valueRgx, String columnName, String value) throws Exception {
        if (!value.matches(valueRgx)) {
            throw new Exception("Неккоректный запрос - неверный тип значения \"" + value + "\" для колонки '" + columnName + "'");
        }
    }

    private void validateOperatorByColumnType(String curCondition, String pairRgx, String op, String columnName) throws
            Exception {
        if (!curCondition.matches(pairRgx)) {
            throw new Exception("Применения оператора \"" + op + "\" недоступно для колонки '" + columnName + "'.");
        }
    }

    private void validateQuery(String query, String regex) throws Exception {
        if (!query.matches(regex)) {
            System.err.println(regex);
            throw new Exception("Некорректный запрос: " + query);
        }
    }

}