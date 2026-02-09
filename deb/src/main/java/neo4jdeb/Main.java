package neo4jdeb;

import java.util.List;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;


public class Main {
    public static void main(String[] args) {
        String uri = System.getenv().getOrDefault("NEO4J_URI", "bolt://localhost:7687");
        String user = System.getenv().getOrDefault("NEO4J_USER", "neo4j");
        String password = System.getenv().getOrDefault("NEO4J_PASSWORD", "neo4jalmeria00");

        System.out.println("Connecting to Neo4j at " + uri + " as " + user);

        try (Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
             Session session = driver.session()) {
            // Example: print all movies in the database with full properties
            List<Record> movies = session.executeRead(tx -> {
                Result r = tx.run("MATCH (m:Movie) RETURN m ORDER BY m.year");
                return r.list();
            });

         

             
        } catch (Exception e) {
            System.err.println("Failed to connect/query Neo4j: " + e.getMessage());
            e.printStackTrace();
        }

        

            /* 
            // Build a Cypher-DSL statement that refers to properties
            var movieNode = node("Movie").named("m");
            // Create a MERGE with properties (using named parameters)
            Statement stmt = merge(movieNode.withProperties(mapOf("title", parameter("title"), "year", parameter("year")))).returning(literalOf(1)).build();

            // Render to Cypher string
            String cypher = Renderer.getDefaultRenderer().render(stmt);
            System.out.println("Rendered Cypher: " + cypher);

            // Extract property names inside the node property map { ... }
            Pattern p = Pattern.compile("\\{([^}]*)\\}");
            Matcher m = p.matcher(cypher);
            System.out.println("Properties occurring in the query:");
            while (m.find()) {
                String inside = m.group(1); // e.g. "title: $title, year: $year"
                String[] parts = inside.split(",");
                for (String part : parts) {
                    String[] kv = part.split(":");
                    if (kv.length >= 1) {
                        String key = kv[0].trim();
                        // remove possible backticks
                        key = key.replace("`", "");
                        System.out.println("- " + key);
                    }
                }
            }*/

            /*    
            String cypherQuery = "MATCH (u:User)-[:WORKS_AT]->(c:Company) " +
                                 "WHERE u.status = 'active' AND c.revenue > 1000000 " +
                                 "RETURN u.email, c.name, u.createdAt";

            Statement statement = CypherParser.parse(cypherQuery);

        Set<String> propertyNames = new HashSet<>();
        Set<String> labels = new HashSet<>();
        Set<String> relationshipTypes = new HashSet<>();

        statement.accept(segment -> {
            // 1. Extract Properties
            if (segment instanceof Property property) {
                propertyNames.add(property.getName());
            } 
            
            // 2. Extract Labels
            // The parser visits NodeLabel objects specifically
            else if (segment instanceof NodeLabel label) {
                labels.add(label.getValue());
            } 
            
            // 3. Extract Relationship Types
            // The parser visits Relationship.Details which contains the types
            else if (segment instanceof Relationship.Details details) {
                relationshipTypes.addAll(details.getTypes());
            }
        });

        System.out.println("Properties:    " + propertyNames);
        System.out.println("Labels:        " + labels);
        System.out.println("Relationships: " + relationshipTypes);
        
        }*/

      
    }

}