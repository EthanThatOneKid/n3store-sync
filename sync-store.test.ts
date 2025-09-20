import { assertEquals } from "@std/assert";
import { QueryEngine } from "@comunica/query-sparql";
import { SyncStore, SyncStoreEvent } from "./sync-store.ts";

Deno.test("Example N3 Store sync events with SPARQL", async () => {
  let count = 0;
  const store = new SyncStore();
  store.addEventListener(SyncStoreEvent.ADD_QUAD, (event) => {
    console.log("Single quad added:", event);
    count++;
  });

  const queryEngine = new QueryEngine();
  await queryEngine.queryVoid(
    `
PREFIX ex: <http://example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

INSERT DATA {
  ex:person1 rdf:type ex:Person ;
             ex:name "John Doe" ;
             ex:age 30 .
  
  ex:person2 rdf:type ex:Person ;
             ex:name "Jane Smith" ;
             ex:age 25 .
}
`,
    { sources: [store] },
  );

  assertEquals(count, 6); // 3 quads x 2 persons
});
