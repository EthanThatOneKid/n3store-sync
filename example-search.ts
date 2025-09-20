#!/usr/bin/env -S deno run --allow-all

/**
 * Example script demonstrating full-text and vector search capabilities
 * using OramaStore with SPARQL data insertion.
 *
 * This script shows how to:
 * 1. Insert RDF data using SPARQL queries
 * 2. Perform full-text search on the indexed data
 * 3. Perform vector search using embeddings
 * 4. Perform hybrid search combining both approaches
 * 5. Compare different search modes and their results
 * 6. Handle errors gracefully and provide comprehensive examples
 */

import { QueryEngine } from "@comunica/query-sparql";
import { OramaStore } from "./orama-store.ts";

const queryEngine = new QueryEngine();

/**
 * Helper function to safely execute search operations with error handling
 */
async function safeSearch(
  searchFn: () => Promise<any>,
  operation: string,
  query: string,
) {
  try {
    return await searchFn();
  } catch (error) {
    console.error(`   ❌ Error in ${operation} for "${query}":`, error.message);
    return { count: 0, hits: [] };
  }
}

/**
 * Helper function to display search results in a consistent format
 */
function displayResults(results: any, showType = false) {
  if (results.count === 0) {
    console.log("   No results found.");
    return;
  }

  results.hits.forEach((hit: any, index: number) => {
    const doc = hit.document;
    console.log(`   ${index + 1}. Score: ${hit.score.toFixed(3)}`);
    console.log(`      Subject: ${doc.subject}`);
    console.log(`      Predicate: ${doc.predicate}`);
    console.log(`      Object: ${doc.object}`);
    if (showType) {
      console.log(`      Type: ${doc.objectType}`);
    }
  });
}

async function main() {
  console.log("🚀 Starting OramaStore Search Example\n");

  // Create a new OramaStore instance
  const oramaStore = new OramaStore();

  console.log("📊 Initial state:");
  console.log(`   SyncStore size: ${oramaStore.getSize()}`);
  console.log(`   Orama documents: ${await oramaStore.getDocumentCount()}`);
  console.log(`   In sync: ${await oramaStore.isInSync()}\n`);

  // Insert sample RDF data using SPARQL
  console.log("📝 Inserting sample RDF data using SPARQL...");

  try {
    await queryEngine.queryVoid(
      `PREFIX ex: <http://example.org/>
       PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       PREFIX foaf: <http://xmlns.com/foaf/0.1/>
       PREFIX dc: <http://purl.org/dc/elements/1.1/>
       PREFIX schema: <http://schema.org/>
       
       INSERT DATA {
         # People
         ex:alice rdf:type ex:Person ;
                  foaf:name "Alice Johnson" ;
                  ex:age 28 ;
                  ex:occupation "Software Engineer" ;
                  ex:skills "JavaScript, TypeScript, React" ;
                  ex:description "Passionate about web development and machine learning" .
         
         ex:bob rdf:type ex:Person ;
                foaf:name "Bob Smith" ;
                ex:age 35 ;
                ex:occupation "Data Scientist" ;
                ex:skills "Python, R, TensorFlow, PyTorch" ;
                ex:description "Expert in machine learning and statistical analysis" .
         
         ex:carol rdf:type ex:Person ;
                  foaf:name "Carol Davis" ;
                  ex:age 42 ;
                  ex:occupation "Product Manager" ;
                  ex:skills "Agile, Scrum, User Research" ;
                  ex:description "Experienced in product strategy and team leadership" .
         
         # Projects
         ex:project1 rdf:type ex:Project ;
                     dc:title "AI-Powered Search Engine" ;
                     dc:description "A semantic search engine using vector embeddings and RDF knowledge graphs" ;
                     ex:technologies "Python, Orama, SPARQL, Vector Search" ;
                     ex:status "In Progress" .
         
         ex:project2 rdf:type ex:Project ;
                     dc:title "Web Application Dashboard" ;
                     dc:description "A responsive dashboard for data visualization and analytics" ;
                     ex:technologies "React, TypeScript, D3.js, Node.js" ;
                     ex:status "Completed" .
         
         # Relationships
         ex:alice ex:worksOn ex:project1 .
         ex:bob ex:worksOn ex:project1 .
         ex:carol ex:manages ex:project1 .
         ex:alice ex:worksOn ex:project2 .
       }`,
      { sources: [oramaStore.getStore()] },
    );

    console.log("✅ Data inserted successfully!\n");
  } catch (error) {
    console.error("❌ Error inserting data:", error);
    throw error;
  }

  console.log("📊 After insertion:");
  console.log(`   SyncStore size: ${oramaStore.getSize()}`);
  console.log(`   Orama documents: ${await oramaStore.getDocumentCount()}`);
  console.log(`   In sync: ${await oramaStore.isInSync()}\n`);

  // Wait a moment for synchronization
  await new Promise((resolve) => setTimeout(resolve, 100));

  // 1. Full-text search examples
  console.log("🔍 FULL-TEXT SEARCH EXAMPLES");
  console.log("=".repeat(50));

  const searchQueries = [
    "Alice",
    "machine learning",
    "JavaScript",
    "project",
    "dashboard",
  ];

  for (const query of searchQueries) {
    console.log(`\n🔎 Searching for: "${query}"`);
    const results = await safeSearch(
      () => oramaStore.search({ term: query, limit: 5 }),
      "full-text search",
      query,
    );
    console.log(`   Found ${results.count} results:`);
    displayResults(results, true);
  }

  // 2. Vector search examples
  console.log("\n\n🧠 VECTOR SEARCH EXAMPLES");
  console.log("=".repeat(50));

  const vectorQueries = [
    "artificial intelligence and data science",
    "web development and user interface",
    "project management and team leadership",
    "programming languages and frameworks",
  ];

  for (const query of vectorQueries) {
    console.log(`\n🔎 Vector search for: "${query}"`);
    const results = await safeSearch(
      () => oramaStore.vectorSearch(query, "combinedEmbedding", 0.7, 5),
      "vector search",
      query,
    );
    console.log(`   Found ${results.count} results:`);
    displayResults(results);
  }

  // 3. Hybrid search examples
  console.log("\n\n⚡ HYBRID SEARCH EXAMPLES");
  console.log("=".repeat(50));

  const hybridQueries = [
    "software engineer with AI skills",
    "data visualization project",
    "team management and product strategy",
  ];

  for (const query of hybridQueries) {
    console.log(`\n🔎 Hybrid search for: "${query}"`);

    // Default hybrid search (equal weights)
    const defaultResults = await safeSearch(
      () => oramaStore.hybridSearch(query, "combinedEmbedding", 0.7, 5),
      "hybrid search (default)",
      query,
    );
    console.log(`   Default weights (50/50): ${defaultResults.count} results`);

    // Text-heavy hybrid search
    const textHeavyResults = await safeSearch(
      () =>
        oramaStore.hybridSearch(query, "combinedEmbedding", 0.7, 5, {
          text: 0.8,
          vector: 0.2,
        }),
      "hybrid search (text-heavy)",
      query,
    );
    console.log(`   Text-heavy (80/20): ${textHeavyResults.count} results`);

    // Vector-heavy hybrid search
    const vectorHeavyResults = await safeSearch(
      () =>
        oramaStore.hybridSearch(query, "combinedEmbedding", 0.7, 5, {
          text: 0.2,
          vector: 0.8,
        }),
      "hybrid search (vector-heavy)",
      query,
    );
    console.log(`   Vector-heavy (20/80): ${vectorHeavyResults.count} results`);

    // Show top result from default search
    if (defaultResults.hits.length > 0) {
      const topHit = defaultResults.hits[0];
      const doc = topHit.document;
      console.log(`   Top result (score: ${topHit.score.toFixed(3)}):`);
      console.log(`      ${doc.subject} → ${doc.predicate} → ${doc.object}`);
    }
  }

  // 4. Search by specific vector properties
  console.log("\n\n🎯 VECTOR PROPERTY-SPECIFIC SEARCH");
  console.log("=".repeat(50));

  console.log("\n🔎 Searching in subject embeddings:");
  const subjectResults = await safeSearch(
    () => oramaStore.vectorSearch("Alice Johnson", "subjectEmbedding", 0.8, 3),
    "subject vector search",
    "Alice Johnson",
  );
  console.log(`   Found ${subjectResults.count} results in subjects`);

  console.log("\n🔎 Searching in predicate embeddings:");
  const predicateResults = await safeSearch(
    () =>
      oramaStore.vectorSearch(
        "name occupation skills",
        "predicateEmbedding",
        0.7,
        3,
      ),
    "predicate vector search",
    "name occupation skills",
  );
  console.log(`   Found ${predicateResults.count} results in predicates`);

  console.log("\n🔎 Searching in object embeddings:");
  const objectResults = await safeSearch(
    () =>
      oramaStore.vectorSearch(
        "software engineer data scientist",
        "objectEmbedding",
        0.7,
        3,
      ),
    "object vector search",
    "software engineer data scientist",
  );
  console.log(`   Found ${objectResults.count} results in objects`);

  // 5. Performance comparison
  console.log("\n\n⏱️  PERFORMANCE COMPARISON");
  console.log("=".repeat(50));

  const testQuery = "machine learning and web development";
  const iterations = 5;

  console.log(`\nRunning performance test with query: "${testQuery}"`);
  console.log(`Averaging over ${iterations} iterations...`);

  // Full-text search timing
  const textStart = performance.now();
  for (let i = 0; i < iterations; i++) {
    await oramaStore.search({ term: testQuery, limit: 10 });
  }
  const textTime = (performance.now() - textStart) / iterations;

  // Vector search timing
  const vectorStart = performance.now();
  for (let i = 0; i < iterations; i++) {
    await oramaStore.vectorSearch(testQuery, "combinedEmbedding", 0.7, 10);
  }
  const vectorTime = (performance.now() - vectorStart) / iterations;

  // Hybrid search timing
  const hybridStart = performance.now();
  for (let i = 0; i < iterations; i++) {
    await oramaStore.hybridSearch(testQuery, "combinedEmbedding", 0.7, 10);
  }
  const hybridTime = (performance.now() - hybridStart) / iterations;

  console.log(`\nAverage search times (${iterations} iterations):`);
  console.log(`   Full-text search: ${textTime.toFixed(2)}ms`);
  console.log(`   Vector search: ${vectorTime.toFixed(2)}ms`);
  console.log(`   Hybrid search: ${hybridTime.toFixed(2)}ms`);

  // 6. Demonstrate embedding generation
  console.log("\n\n🔧 EMBEDDING GENERATION DEMO");
  console.log("=".repeat(50));

  const sampleTexts = [
    "Alice Johnson is a software engineer",
    "Machine learning and artificial intelligence",
    "Web development with React and TypeScript",
  ];

  for (const text of sampleTexts) {
    try {
      const embedding = oramaStore.generateEmbedding(text);
      console.log(`\nText: "${text}"`);
      console.log(`Embedding dimension: ${embedding.length}`);
      console.log(
        `First 5 values: [${
          embedding.slice(0, 5).map((v) => v.toFixed(3)).join(", ")
        }...]`,
      );
    } catch (error) {
      console.error(
        `   Error generating embedding for "${text}":`,
        error.message,
      );
    }
  }

  // 7. Error handling demonstration
  console.log("\n\n🛡️  ERROR HANDLING DEMONSTRATION");
  console.log("=".repeat(50));

  console.log("\nTesting with invalid parameters:");

  // Test with invalid similarity threshold
  await safeSearch(
    () => oramaStore.vectorSearch("test", "combinedEmbedding", 2.0, 5),
    "vector search with invalid similarity",
    "test (similarity > 1.0)",
  );

  // Test with invalid vector property
  await safeSearch(
    () => oramaStore.vectorSearch("test", "invalidProperty", 0.7, 5),
    "vector search with invalid property",
    "test (invalid property)",
  );

  // Test with empty query
  await safeSearch(
    () => oramaStore.search({ term: "", limit: 5 }),
    "full-text search with empty query",
    "empty string",
  );

  console.log("\n\n✅ Example completed successfully!");
  console.log("\nThis example demonstrated:");
  console.log("• SPARQL data insertion into OramaStore");
  console.log("• Full-text search capabilities");
  console.log("• Vector search with embeddings");
  console.log("• Hybrid search combining both approaches");
  console.log("• Different search modes and weight configurations");
  console.log("• Performance comparisons between search types");
  console.log("• Embedding generation for custom queries");
  console.log("• Robust error handling and graceful degradation");
  console.log("• Property-specific vector search capabilities");
}

// Run the example directly
main().catch((error) => {
  console.error("❌ Error running example:", error);
  console.error("Stack trace:", error.stack);
  Deno.exit(1);
});
