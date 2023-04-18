package com.digdes.school;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JavaSchoolStarter {
    DataManagementLanguageImpl dml;

    public JavaSchoolStarter() {
    }

    public void init(Map<String, Class<?>> columnsTypes) throws Exception { // todo или ловеркейсить насильно пришедшие данные?
        for (Map.Entry<String, Class<?>> entry : columnsTypes.entrySet()) {
            if (!entry.getValue().getSimpleName().matches("Long|Double|Boolean|String")) {
                throw new Exception("Непподерживаемый тип значений для колонки: " + entry.getValue().getName());
            }
        }
        dml = new DataManagementLanguageImpl(columnsTypes);
    }

    public List<Map<String, Object>> execute(String request) throws Exception {
        try {
            switch (getRequestType(request)) {
                case INSERT:
                    return dml.insert(request);
                case UPDATE:
                    return dml.update(request);
                case DELETE:
                    return dml.delete(request);
                case SELECT:
                    return dml.select(request);
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(e.getMessage() + "\nНеподдерживаемый тип запроса в '" + request + "'");
        }
        return null;
    }

    public void printTable() {
        dml.printTable();
    }

    private REQUEST_TYPE getRequestType(String request) {
        Pattern p = Pattern.compile("INSERT|UPDATE|DELETE|SELECT", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(request);
        m.find();   // todo прокинуть исключение если false
        return REQUEST_TYPE.valueOf(m.group().toUpperCase());
    }

}
