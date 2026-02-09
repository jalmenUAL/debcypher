package neo4jdeb;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;

import org.neo4j.cypherdsl.core.Comparison;
import org.neo4j.cypherdsl.core.Node;
import org.neo4j.cypherdsl.core.NodeLabel;
import org.neo4j.cypherdsl.core.Property;
import org.neo4j.cypherdsl.core.Relationship;
import org.neo4j.cypherdsl.core.Relationship.Direction;
import org.neo4j.cypherdsl.core.Statement;
import org.neo4j.cypherdsl.parser.CypherParser;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Record;

public class CypherDebugger {

    private final Driver driver;

    public CypherDebugger(String uri, String user, String password) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    public void close() {
        driver.close();
    }

    public void debug(String cypherQuery) {
        System.out.println("\n--- 🔍 STARTING CYPHER DEBUG SESSION ---");
        System.out.println("Query: " + cypherQuery);

        Statement statement;
        try {
            statement = CypherParser.parse(cypherQuery);
        } catch (Exception e) {
            System.err.println("❌ SYNTAX ERROR: The query could not be parsed. Check brackets and keywords.");
            return;
        }

        Set<String> properties = new HashSet<>();
        Set<String> labels = new HashSet<>();
        Set<String> relationshipTypes = new HashSet<>();
        List<String> conditions = new ArrayList<>();

        // 1. Extract Components and Syntactic Boolean Conditions
        statement.accept(segment -> {
            if (segment instanceof Property p) {
                properties.add(p.getName());
            } else if (segment instanceof NodeLabel l) {
                labels.add(l.getValue());
            } else if (segment instanceof Relationship.Details d) {
                relationshipTypes.addAll(d.getTypes());
            } else if (segment instanceof Comparison) {
                // 1. Get the raw string: Comparison{cypher=m.year > 2010}
                String raw = segment.toString();

                // 2. Extract only what is inside cypher=...
                if (raw.contains("cypher=")) {
                    String cleanCondition = raw.substring(raw.indexOf("cypher=") + 7, raw.lastIndexOf("}"));
                    conditions.add(cleanCondition);
                } else {
                    // Fallback for different versions/formats
                    conditions.add(raw);
                }
            }
        });

        try (Session session = driver.session()) {
            // 2. Schema Validation
            validateSchema(session, labels, "CALL db.labels()", "Label/Class");
            validateSchema(session, relationshipTypes, "CALL db.relationshipTypes()", "Relationship Type");
            validateSchema(session, properties, "CALL db.propertyKeys()", "Property Key");

            // 3. Boolean Condition Validation
            validateBooleanConditions(session, conditions);

            // 4. Directional check
            checkPathConnectivity(session, cypherQuery);
        }

        System.out.println("--- 🏁 DEBUG SESSION COMPLETE ---\n");
    }

    private void checkPathConnectivity(Session session, String query) {
        Statement statement = CypherParser.parse(query);

        // Lista para evitar procesar la misma relación varias veces
        Set<String> processedPaths = new HashSet<>();

        statement.accept(segment -> {
            if (segment instanceof Relationship rel) {
            String leftLabel = extractLabel(rel.getLeft());
            String rightLabel = extractLabel(rel.getRight());
            String relType = rel.getDetails().getTypes().isEmpty() ? null 
                             : rel.getDetails().getTypes().get(0);

            // Determine logical Source and Target based on the arrow
            String sourceLabel, targetLabel;
            Direction dir = rel.getDetails().getDirection();

            if (dir == Direction.LTR) { // (a)-[:REL]->(b)
                sourceLabel = leftLabel;
                targetLabel = rightLabel;
            } else if (dir == Direction.RTL) { // (a)<-[:REL]-(b)
                sourceLabel = rightLabel;
                targetLabel = leftLabel;
            } else { // Undirected (a)-[:REL]-(b)
                sourceLabel = leftLabel;
                targetLabel = rightLabel;
            }

            if (sourceLabel != null && targetLabel != null && relType != null) {
                runDeepConnectivityCheck(session, sourceLabel, relType, targetLabel);
            }
        }
        });
    }

    private String extractLabel(Node node) {
        return node.getLabels() // 1. Obtiene la colección de etiquetas del nodo
                .stream() // 2. Abre un flujo para procesarlas
                .map(l -> l.getValue()) // 3. Convierte el objeto 'NodeLabel' a su valor String (ej: "Person")
                .findFirst() // 4. Toma la primera etiqueta encontrada
                .orElse(null); // 5. Si el nodo no tiene etiqueta (ej: '(n)'), devuelve null
    }

    private void runDeepConnectivityCheck(Session session, String start, String type, String end) {
        System.out.println(String.format("\n🧐 Analyzing: (:%s)-[:%s]-(:%s)", start, type, end));

        // 1. Test for Reversed Direction
        String reverseQ = String.format("MATCH (a:%s)<-[:%s]-(b:%s) RETURN count(*) > 0 AS ok LIMIT 1", start, type,
                end);
        System.out.println("   🔄 Checking for reversed relationship...");
        System.out.println("      Query: " + reverseQ);
        if (session.run(reverseQ).single().get("ok").asBoolean()) {
            System.err.println("   ❌ DIRECTION ERROR: The relationship exists but the arrow is reversed.");
            System.err.println(String.format("      👉 FIX: (:%s)<-[:%s]-(:%s)", start, type, end));
            return;
        }

        // 2. Test for Indirect Connection (The "Bridge" check)
        // We look for any intermediate node (m) that connects 'a' and 'b'
        String bridgeQ = String.format(
                "MATCH (a:%s)-[r1]-(m)-[r2]-(b:%s) " +
                        "RETURN labels(m)[0] AS midLabel, type(r1) AS t1, type(r2) AS t2 LIMIT 1",
                start, end);

        Result res = session.run(bridgeQ);
        if (res.hasNext()) {
            Record rec = res.next();
            String mid = rec.get("midLabel").asString();
            String t1 = rec.get("t1").asString();
            String t2 = rec.get("t2").asString();

            System.err.println("   ⚠️ INDIRECT RELATIONSHIP DETECTED:");
            System.err.println(
                    String.format("      Nodes are not linked by [:%s], but they share a (:%s) node.", type, mid));
            System.err.println(
                    String.format("      👉 SUGGESTED MATCH: (:%s)-[:%s]-(:%s)-[:%s]-(:%s)", start, t1, mid, t2, end));
        } else {
            System.err.println("   💔 DISCONNECTED: No direct or indirect connection found within 2 hops.");
        }
    }

    private void validateBooleanConditions(Session session, List<String> conditions) {
        for (String rawCondition : conditions) {
            // 1. Clean the string
            String cleanCondition = rawCondition;
            if (rawCondition.contains("cypher=")) {
                cleanCondition = rawCondition.substring(rawCondition.indexOf("cypher=") + 7,
                        rawCondition.lastIndexOf("}"));
            }

            // 2. Identify Variable and Property (e.g., "m.year")
            String variable = "n";
            String propertyName = null;
            if (cleanCondition.contains(".")) {
                String[] parts = cleanCondition.split("\\.");
                variable = parts[0].replaceAll("[^a-zA-Z0-9]", "");
                // Extract property name before the operator (e.g., "year" from "year > 2010")
                propertyName = parts[1].split("[\\s>=<!]")[0];
            }

            // 3. Logic & Type Check
            if (propertyName != null) {
                checkPropertyTypeMismatch(session, propertyName, cleanCondition);
            }

            // 4. Satisfiability Check
            String testQuery = String.format(
                    "MATCH (n) WITH n AS %s WHERE %s RETURN count(%s) > 0 AS possible",
                    variable, cleanCondition, variable);

            try {
                boolean isPossible = session.run(testQuery).single().get("possible").asBoolean();
                if (!isPossible) {
                    System.err.println("❌ LOGIC ERROR: [" + cleanCondition + "] returns 0 results.");
                }
            } catch (Exception e) {
                System.err.println("⚠️ Complex condition: " + cleanCondition);
            }
        }
    }

    private void checkPropertyTypeMismatch(Session session, String propertyName, String condition) {
        // We query the database for the type of this property on any node that has it
        String typeQuery = String.format(
                "MATCH (n) WHERE n.%s IS NOT NULL RETURN apoc.meta.type(n.%s) AS type LIMIT 1",
                propertyName, propertyName);

        try {
            Result res = session.run(typeQuery);
            if (res.hasNext()) {
                String actualType = res.next().get("type").asString(); // e.g., "INTEGER" or "STRING"

                // Basic detection: if the condition has quotes but the DB is INTEGER
                if (actualType.equals("INTEGER") && (condition.contains("'") || condition.contains("\""))) {
                    System.err.println("⚠️  TYPE MISMATCH: Property '" + propertyName +
                            "' is an INTEGER in the DB, but you are comparing it to a STRING.");
                }
                // Basic detection: if the condition is numeric but the DB is STRING
                else if (actualType.equals("STRING") && condition.matches(".*\\.\\w+\\s*[=><!]+\\s*\\d+.*")) {
                    System.err.println("⚠️  TYPE MISMATCH: Property '" + propertyName +
                            "' is a STRING in the DB, but you are comparing it to a NUMBER.");
                }
            }
        } catch (Exception e) {
            // If APOC is not installed, we skip the specific type check
        }
    }

    private void validateSchema(Session session, Set<String> items, String fetchQuery, String typeLabel) {
        List<String> existing = session.run(fetchQuery).list(r -> r.get(0).asString());
        for (String item : items) {
            if (existing.contains(item)) {
                System.out.println("✅ " + typeLabel + ": '" + item + "' exists.");
            } else {
                String suggestion = findClosestMatch(item, existing);
                String msg = "❌ " + typeLabel + " ERROR: '" + item + "' not found.";
                if (suggestion != null)
                    msg += " -> Did you mean '" + suggestion + "'?";
                System.err.println(msg);
            }
        }
    }

    private String findClosestMatch(String target, List<String> options) {
        String closest = null;
        int minDistance = Integer.MAX_VALUE;
        for (String option : options) {
            int distance = calculateLevenshtein(target.toLowerCase(), option.toLowerCase());
            if (distance < minDistance && distance <= 3) {
                minDistance = distance;
                closest = option;
            }
        }
        return closest;
    }

    private int calculateLevenshtein(String x, String y) {
        int[][] dp = new int[x.length() + 1][y.length() + 1];
        for (int i = 0; i <= x.length(); i++) {
            for (int j = 0; j <= y.length(); j++) {
                if (i == 0)
                    dp[i][j] = j;
                else if (j == 0)
                    dp[i][j] = i;
                else {
                    dp[i][j] = Math.min(Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1),
                            dp[i - 1][j - 1] + (x.charAt(i - 1) == y.charAt(j - 1) ? 0 : 1));
                }
            }
        }
        return dp[x.length()][y.length()];
    }

    public static void main(String[] args) {
        CypherDebugger debugger = new CypherDebugger("bolt://localhost:7687", "neo4j", "neo4jalmeria00");

        // Example with a logical condition that might fail (e.g., movies from the
        // future)
        String testQuery = "MATCH (:Person {name:'Keanu Reeves'})-[r:!ACTED_IN]->(m:Movie)\n" + //
                "RETURN type(r) AS type, m.title AS movies";

        debugger.debug(testQuery);
        debugger.close();
    }
}