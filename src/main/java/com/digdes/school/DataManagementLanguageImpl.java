package com.digdes.school;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataManagementLanguageImpl {

    private Map<String, Class<?>> columnsTypes;
    private final long columnNum;
    private List<Map<String, Object>> table;

    String columnNameRgx, columnValueRgx, pairRgx, valuesRgx, whereRgx, conditionRgx;
    String comparisonPairRgx, comparisonStringPairRgx, comparisonDigitPairRgx;
    String stringValueRgx, digitValueRgx, boolValueRgx;
    String logicalOpsRgx, comparisonOpsRgx;

    {
        stringValueRgx = "'[^']+'";
        digitValueRgx = "\\d+(\\.\\d+)?";
        boolValueRgx = "(true|false)";

        columnNameRgx = "'[^']+'";
        columnValueRgx = "(" + stringValueRgx + "|" + digitValueRgx + "|" + boolValueRgx + "|null)";
        pairRgx = columnNameRgx + "\\s*=\\s*" + columnValueRgx;

        logicalOpsRgx = "(AND|OR)";
        comparisonOpsRgx = "((=|!=)|i?like|(>|<)=?)";

        comparisonPairRgx = columnNameRgx + "\\s*(=|!=)\\s*" + columnValueRgx;
        comparisonStringPairRgx = columnNameRgx + "\\s*i?like\\s*" + stringValueRgx;
        comparisonDigitPairRgx = columnNameRgx + "\\s*(>|<)=?\\s*" + digitValueRgx;

        conditionRgx = String.format("(%s|%s|%s)", comparisonPairRgx, comparisonStringPairRgx, comparisonDigitPairRgx);
        whereRgx = "(\\s+where\\s+" + conditionRgx + "(\\s+" + logicalOpsRgx + "\\s+" + conditionRgx + ")*)?\\s*";
    }

    DataManagementLanguageImpl(Map<String, Class<?>> columnsTypes) {
        this.columnsTypes = columnsTypes;
        this.columnNum = columnsTypes.size();
        valuesRgx = "\\s+values\\s+" + pairRgx + "(\\s*,\\s*" + pairRgx + "){0," + (columnNum - 1) + "}\\s*";
        table = new ArrayList<>();
    }

    public List<Map<String, Object>> insert(String request) throws Exception {
        validate(request);
        Map<String, Object> newRow = getRowWithNewValues(request);
        table.add(newRow);
        List<Map<String, Object>> insertRes = new ArrayList<>();
        insertRes.add(newRow);
        return insertRes;
    }

    public void printTable() {
        table.forEach(System.out::println);
    }

    private Map<String, Object> getRowWithNewValues(String request) throws Exception {
        Map<String, Object> newRow = new HashMap<>();
        Matcher pairRgxM = Pattern.compile(pairRgx, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE).matcher(request);
        if (pairRgxM.groupCount() > columnNum) {
            throw new Exception("Неккоректный запрос - новых значений для колонок больше, чем количество колонок.");
        }
        while (pairRgxM.find()) {
            String newPair = pairRgxM.group();
            Matcher columnM = Pattern.compile(columnNameRgx, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE).matcher(newPair);
            columnM.find();
            Matcher valueM = Pattern.compile(columnValueRgx, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE).matcher(newPair.substring(columnM.end()));
            valueM.find();
            String column = columnM.group().substring(1, columnM.end() - 1).toLowerCase();
            String value = valueM.group();

            validateColumn(column);

            if (value.equals("null")) {
                newRow.put(column, null);
            } else {
                try {
                    if (columnsTypes.get(column).getName().equals(Long.class.getName())) {
                        newRow.put(column, Long.parseLong(value));
                    } else if (columnsTypes.get(column).getName().equals(Double.class.getName())) {
                        newRow.put(column, Double.parseDouble(value));
                    } else {
                        newRow.put(column, value);
                    }
                } catch (NumberFormatException e) {
                    throw new Exception("Неккоректный запрос - неверный тип значения \"" + value + "\" для колонки '" + column + "'");
                }
            }

        }
        return newRow;
    }

    public List<Map<String, Object>> update(String request) throws Exception {
        validate(request);

        List<Map<String, Object>> updatedRows = new ArrayList<>();
        Map<String, Object> newValuesRow = getRowWithNewValues(request);

        if (hasConditions(request)) {
            HashSet<Integer> mapIndexesToUpdate = findIndexesToUpdateByConditions(request);
            for (int i = 0; i < table.size(); i++) {
                Map<String, Object> map = table.get(i);
                if (mapIndexesToUpdate.contains(i)) {
                    System.out.println("contains");
                    map.putAll(newValuesRow);
                    updatedRows.add(map);
                }
            }
        } else {
            table.forEach(map -> map.putAll(newValuesRow));
            updatedRows = table;
        }
        return updatedRows;
    }

    private boolean hasConditions(String request) {
        Matcher whereM = Pattern.compile("where", Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE).matcher(request);
        return whereM.find();
    }

    private HashSet<Integer> findIndexesToUpdateByConditions(String request) throws Exception {
        HashSet<Integer> mapIndexesToUpdate = new HashSet<>();
        String conditions = request.replaceAll("(?iu).+where", "");

        Matcher conditionsM = Pattern.compile(conditionRgx, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE).matcher(conditions);
        Matcher logOpsM = Pattern.compile(logicalOpsRgx, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE).matcher(conditions);

        boolean hasLogOps = logOpsM.find();
        boolean isAnd = hasLogOps && logOpsM.group().equalsIgnoreCase("and");

        while (conditionsM.find()) {
            String curCondition = conditionsM.group();
            Matcher columnM = Pattern.compile(columnNameRgx, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE).matcher(curCondition);
            columnM.find();
            String column = columnM.group().substring(1, columnM.end() - 1).toLowerCase();

            validateColumn(column);

            Matcher comparisonOpM = Pattern.compile(comparisonOpsRgx, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE).matcher(curCondition);
            comparisonOpM.find();
            String comparisonOp = comparisonOpM.group();

            IndexesFinder indexesFinder = null;

            if (comparisonOp.matches("!?=")) {
                String val = getValueFromCondition(curCondition.substring(columnM.end()), columnValueRgx);
                indexesFinder = (set) -> commonFilter(set, comparisonOp, column, val);
            } else if (comparisonOp.matches("i?like")) {
                validateOperatorByColumnType(curCondition, comparisonStringPairRgx, comparisonOp, column);

                String valTemplate = getValueFromCondition(curCondition.substring(columnM.end()), stringValueRgx + "|null");
                if (valTemplate.charAt(0) == '%') {
                    valTemplate = ".*" + valTemplate.substring(1);
                }
                if (valTemplate.charAt(valTemplate.length() - 1) == '%') {
                    valTemplate = valTemplate.substring(0, valTemplate.length() - 1) + ".*";
                }
                String finalValTemplate = comparisonOp.equalsIgnoreCase("ilike") ? "(?iu)" + valTemplate : valTemplate;
                System.out.println("finalValTemplate = " + finalValTemplate);

                indexesFinder = (set) -> strFilter(set, column, finalValTemplate);

            } else if (comparisonOp.matches("[><]=?")) {
                validateOperatorByColumnType(curCondition, comparisonDigitPairRgx, comparisonOp, column);
                Double val = Double.valueOf(getValueFromCondition(curCondition.substring(columnM.end()), digitValueRgx + "|null"));
                indexesFinder = (set) -> digitFilter(set, comparisonOp, column, val);
            }

            assert indexesFinder != null;
            if (isAnd && !mapIndexesToUpdate.isEmpty()) {
                HashSet<Integer> mapIndexesToUpdateTemp = new HashSet<>();
                indexesFinder.find(mapIndexesToUpdateTemp);
                mapIndexesToUpdate.removeIf(l -> !mapIndexesToUpdateTemp.contains(l));
            } else {
                indexesFinder.find(mapIndexesToUpdate);
            }

        }
        return mapIndexesToUpdate;
    }

    private String getValueFromCondition(String condition, String valueRgx) {
        Matcher valCondM = Pattern.compile(valueRgx, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE).matcher(condition);
        if (!valCondM.find()) {
            throw new RuntimeException("Не найдено значение для сравнения!");
        }
        String valCond = valCondM.group();
        if (valCond.equals("null")) {
            throw new RuntimeException("\"null\" не может использоваться в качестве значения для сравнения!");
        }
        return valCond.replace("'", "");
    }

    private interface IndexesFinder {
        void find(HashSet<Integer> indexes);
    }

    private void commonFilter(HashSet<Integer> indexes, String op, String column, String val) {
        for (int i = 0; i < table.size(); i++) {
            Map<String, Object> map = table.get(i);
            Object arg1 = map.get(column);
            if (arg1 == null && op.equals("!=")) {
                indexes.add(i);
            }
            if (arg1 != null && op.equals("=") == arg1.toString().equals(val)) {
                indexes.add(i);
            }
        }
    }

    private void strFilter(HashSet<Integer> indexes, String column, String valRgx) {
        for (int i = 0; i < table.size(); i++) {
            Map<String, Object> map = table.get(i);
            Object arg1 = map.get(column);
            if (arg1 != null) {
                String curVal = arg1.toString();
                if (curVal.substring(1, curVal.length() - 1).matches(valRgx)) {
                    indexes.add(i);
                }
            }
        }
    }

    private void digitFilter(HashSet<Integer> indexes, String op, String column, Double val) {
        for (int i = 0; i < table.size(); i++) {
            Map<String, Object> map = table.get(i);
            Object arg1 = map.get(column);
            if (arg1 != null && applyOperator(op, arg1,
                    columnsTypes.get(column).getName().equals(Double.class.getName()),  //todo как вытащить класс в преобразование сразу?
                    val)) {
                indexes.add(i);
            }
        }
    }

    private boolean applyOperator(String op, Object arg1, boolean isDouble, Double arg2) {
        switch (op) {
            case "<":
                return isDouble ? (Double) arg1 < arg2 : (Long) arg1 < arg2;
            case "<=":
                return isDouble ? (Double) arg1 <= arg2 : (Long) arg1 <= arg2;
            case ">":
                return isDouble ? (Double) arg1 > arg2 : (Long) arg1 > arg2;
            case ">=":
                return isDouble ? (Double) arg1 >= arg2 : (Long) arg1 >= arg2;
        }
        throw new RuntimeException("Ошибка определения оператора \"" + op + "\"!");
    }

    public List<Map<String, Object>> delete(String request) throws Exception {
        validate(request);
        List<Map<String, Object>> updatedRows = new ArrayList<>();

        if (hasConditions(request)) {
            HashSet<Integer> mapIndexesToUpdate = findIndexesToUpdateByConditions(request);

            Iterator<Map<String, Object>> it = table.listIterator();
            int i = 0;
            while (it.hasNext()) {
                Map<String, Object> map = it.next();
                if (mapIndexesToUpdate.contains(i)) {
                    updatedRows.add(map);
                    it.remove();
                }
                i++;
            }
        } else {
            updatedRows = table;
            table.clear();
        }
        return updatedRows;
    }

    public List<Map<String, Object>> select(String request) throws Exception {
        validate(request);

        List<Map<String, Object>> updatedRows = new ArrayList<>();

        if (hasConditions(request)) {
            HashSet<Integer> mapIndexesToUpdate = findIndexesToUpdateByConditions(request);
            for (int i = 0; i < table.size(); i++) {
                Map<String, Object> map = table.get(i);
                if (mapIndexesToUpdate.contains(i)) {
                    updatedRows.add(map);
                }
            }
        } else {
            updatedRows = table;
        }
        return updatedRows;
    }

    private void validateColumn(String column) throws Exception {
        if (!columnsTypes.containsKey(column)) {
            throw new Exception("Неккоректный запрос - колонки '" + column + "' в таблице нет.");
        }
    }

    private void validateOperatorByColumnType(String curCondition, String pairRgx, String op, String column) throws Exception {
        if (!curCondition.matches(pairRgx)) {
            throw new Exception("Применения оператора \"" + op + "\" недоступно для колонки '" + column + "'.");
        }
    }

    private void validate(String request) throws Exception {
        String regex = null;
        if (request.matches("\\s*(?iu)INSERT.+")) {
            regex = "(?iu)\\s*INSERT" + valuesRgx;
        } else if (request.matches("\\s*(?iu)UPDATE.+")) {
            regex = "\\s*(?iu)UPDATE" + valuesRgx + whereRgx;
        } else if (request.matches("\\s*(?iu)DELETE.*")) {
            regex = "\\s*(?iu)DELETE" + whereRgx;
        } else if (request.matches("\\s*(?iu)SELECT.*")) {
            regex = "\\s*(?iu)SELECT" + whereRgx;
        }
        if (regex == null || !request.matches(regex)) {
            System.err.println(regex);
            if (regex != null) {
                System.err.println(request.matches(regex));
            }
            throw new Exception("Некорректный запрос: " + request);   // todo
        }
    }

}
