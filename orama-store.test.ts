import { assertEquals } from "@std/assert";
import { QueryEngine } from "@comunica/query-sparql";
import { OramaStore } from "./orama-store.ts";

const queryEngine = new QueryEngine();

Deno.test("OramaStore - Basic synchronization", async () => {
  // We create an OramaStore that handles synchronization automatically.
  const oramaStore = new OramaStore();

  // We insert RDF data into the SyncStore and verify it appears in Orama.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       ex:person1 rdf:type ex:Person ;
                  ex:name "Alice" ;
                  ex:age 25 .
       ex:person2 rdf:type ex:Person ;
                  ex:name "Bob" ;
                  ex:age 30 .
     }`,
    { sources: [oramaStore.getStore()] },
  );

  // We verify that the SyncStore and Orama database have the same data.
  assertEquals(oramaStore.getSize(), 6); // SyncStore has 6 quads
  const oramaCount = await oramaStore.getDocumentCount();
  assertEquals(oramaCount, 6); // Orama database has 6 documents

  // We verify that the stores are in sync.
  const isInSync = await oramaStore.isInSync();
  assertEquals(isInSync, true);

  // We verify that specific data from SyncStore appears in Orama.
  const aliceResults = await oramaStore.search({
    term: "Alice",
  });
  assertEquals(aliceResults.count, 1);

  const bobResults = await oramaStore.search({
    term: "Bob",
  });
  assertEquals(bobResults.count, 1);
});

Deno.test("OramaStore - Deletion synchronization", async () => {
  // We create an OramaStore that handles synchronization automatically.
  const oramaStore = new OramaStore();

  // We insert initial data.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       ex:person1 rdf:type ex:Person ;
                  ex:name "John Doe" ;
                  ex:age 30 .
       ex:person2 rdf:type ex:Person ;
                  ex:name "Jane Smith" ;
                  ex:age 25 .
     }`,
    { sources: [oramaStore.getStore()] },
  );

  // We verify initial state.
  let docCount = await oramaStore.getDocumentCount();
  assertEquals(docCount, 6);

  // We delete some data using SPARQL.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     
     DELETE DATA {
       ex:person1 ex:age 30 .
     }`,
    { sources: [oramaStore.getStore()] },
  );

  // We verify that the Orama database reflects the deletion.
  docCount = await oramaStore.getDocumentCount();
  assertEquals(docCount, 5); // One less document

  // We verify that the age document is no longer in the Orama database.
  const ageQuadId = oramaStore.createQuad(
    "http://example.org/person1",
    "http://example.org/age",
    "30",
  ).subject.value;
  const ageDoc = await oramaStore.getDocumentById(ageQuadId);
  assertEquals(ageDoc, undefined);

  // We verify that other documents are still present.
  const johnResults = await oramaStore.search({
    term: "John Doe",
  });
  assertEquals(johnResults.count, 1);
});

Deno.test("OramaStore - Real-time synchronization", async () => {
  // We create an OramaStore that handles synchronization automatically.
  const oramaStore = new OramaStore();

  // We start with empty stores.
  assertEquals(oramaStore.getSize(), 0);
  let oramaCount = await oramaStore.getDocumentCount();
  assertEquals(oramaCount, 0);

  // We add data incrementally and verify synchronization.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       ex:person1 rdf:type ex:Person .
     }`,
    { sources: [oramaStore.getStore()] },
  );

  assertEquals(oramaStore.getSize(), 1);
  oramaCount = await oramaStore.getDocumentCount();
  assertEquals(oramaCount, 1);

  // We add more data.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     
     INSERT DATA {
       ex:person1 ex:name "Alice" .
     }`,
    { sources: [oramaStore.getStore()] },
  );

  assertEquals(oramaStore.getSize(), 2);
  oramaCount = await oramaStore.getDocumentCount();
  assertEquals(oramaCount, 2);

  // We remove data and verify synchronization.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     
     DELETE DATA {
       ex:person1 ex:name "Alice" .
     }`,
    { sources: [oramaStore.getStore()] },
  );

  assertEquals(oramaStore.getSize(), 1);
  oramaCount = await oramaStore.getDocumentCount();
  assertEquals(oramaCount, 1);

  // We verify that the removed data is no longer searchable.
  const aliceResults = await oramaStore.search({
    term: "Alice",
  });
  assertEquals(aliceResults.count, 0);
});

// This test suite demonstrates how to use the OramaStore abstraction for effective RDF data synchronization.
// The OramaStore class encapsulates the connection between SyncStore (source of truth) and Orama database (search index),
// providing a clean API while automatically maintaining synchronization through event-driven updates.
// This enables real-time search capabilities while ensuring data integrity between the two systems.
