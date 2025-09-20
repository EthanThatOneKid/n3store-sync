import { Parser } from "n3";
import { SyncStore, SyncStoreEvent } from "./sync-store.ts";

Deno.test("Example N3 Store sync events with Turtle parsing", () => {
  const store = new SyncStore();

  store.target.addEventListener(SyncStoreEvent.ADD_QUAD, (event) => {
    console.log("Single quad added:", event);
  });

  store.target.addEventListener(SyncStoreEvent.ADD_QUADS, (event) => {
    console.log("Batch quads added:", event);
  });

  store.target.addEventListener(SyncStoreEvent.REMOVE_QUAD, (event) => {
    console.log("Quad removed:", event);
  });

  store.target.addEventListener(SyncStoreEvent.REMOVE_QUADS, (event) => {
    console.log("Batch quads removed:", event);
  });

  const parser = new Parser();
  const quads = parser.parse(`
    @prefix ex: <http://example.org/> .
    @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
    
    ex:person1 rdf:type ex:Person ;
               ex:name "John Doe" ;
               ex:age 30 .
    
    ex:person2 rdf:type ex:Person ;
               ex:name "Jane Smith" ;
               ex:age 25 .
  `);
  store.addQuads(quads);

  console.log(`Store now contains ${store.size} quads`);

  const quadsToRemove = store.getQuads(
    "http://example.org/person1",
    "http://example.org/age",
    "30",
    "",
  );

  if (quadsToRemove.length > 0) {
    store.removeQuad(quadsToRemove[0]);
  }

  console.log(`Store now contains ${store.size} quads after removal`);
});
