package org.example.utility;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;

public class SqlFileReaderUtility {
    public String convertSqlScriptToString(Path path) throws IOException {

        BufferedReader br = new BufferedReader(new FileReader(path.toFile()));

        StringBuilder query = new StringBuilder();
        String line;

        while ((line = br.readLine()) != null) {

            if (line.trim().startsWith("-- ")) {
                continue;
            }

            query.append(line).append(" ");
        }
        return query.toString();
    }
}
