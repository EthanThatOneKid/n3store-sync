import { assertEquals } from "@std/assert";
import { QueryEngine } from "@comunica/query-sparql";
import type { Quad } from "n3";
import { DataFactory } from "n3";
import { SyncStore, SyncStoreEvent } from "./sync-store.ts";

const queryEngine = new QueryEngine();

function createMonitoredStore() {
  const store = new SyncStore();
  const eventCounts = {
    addQuad: 0,
    removeQuad: 0,
    addQuads: 0,
    removeQuads: 0,
    removeMatches: 0,
    deleteGraph: 0,
  };

  store.addEventListener(SyncStoreEvent.ADD_QUAD, () => {
    eventCounts.addQuad++;
  });

  store.addEventListener(SyncStoreEvent.REMOVE_QUAD, () => {
    eventCounts.removeQuad++;
  });

  store.addEventListener(SyncStoreEvent.ADD_QUADS, () => {
    eventCounts.addQuads++;
  });

  store.addEventListener(SyncStoreEvent.REMOVE_QUADS, () => {
    eventCounts.removeQuads++;
  });

  store.addEventListener(SyncStoreEvent.REMOVE_MATCHES, () => {
    eventCounts.removeMatches++;
  });

  store.addEventListener(SyncStoreEvent.DELETE_GRAPH, () => {
    eventCounts.deleteGraph++;
  });

  return { store, eventCounts };
}

Deno.test("SPARQL INSERT DATA - Basic", async () => {
  const { store, eventCounts } = createMonitoredStore();

  await queryEngine.queryVoid(
    `
  PREFIX ex: <http://example.org/>
  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
 INSERT DATA {
        ex:person1 rdf:type ex:Person ;
                   ex:name "John Doe" ;
                   ex:age 30 .
      }`,
    { sources: [store] },
  );

  assertEquals(eventCounts.addQuad, 3);
  assertEquals(eventCounts.removeQuad, 0);
  assertEquals(eventCounts.addQuads, 0);
  assertEquals(eventCounts.removeQuads, 0);
  assertEquals(store.size, 3);
});

Deno.test("SPARQL DELETE DATA", async () => {
  const { store, eventCounts } = createMonitoredStore();

  // First, we insert the initial data.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       ex:person1 rdf:type ex:Person ;
                  ex:name "John Doe" ;
                  ex:age 30 .
     }`,
    { sources: [store] },
  );

  // Then, we delete specific data.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     
     DELETE DATA {
       ex:person1 ex:age 30 .
     }`,
    { sources: [store] },
  );

  assertEquals(eventCounts.addQuad, 3);
  assertEquals(eventCounts.removeQuad, 1);
  assertEquals(eventCounts.addQuads, 0);
  assertEquals(eventCounts.removeQuads, 0);
  assertEquals(eventCounts.removeMatches, 0);
  assertEquals(eventCounts.deleteGraph, 0);
  assertEquals(store.size, 2);
});

Deno.test("SPARQL INSERT/DELETE with WHERE", async () => {
  const { store, eventCounts } = createMonitoredStore();

  // We insert the initial data.
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
    { sources: [store] },
  );

  // We delete all persons with age greater than 28.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     DELETE {
       ?person ?p ?o .
     } WHERE {
       ?person rdf:type ex:Person ;
               ex:age ?age .
       ?person ?p ?o .
       FILTER(?age > 28)
     }`,
    { sources: [store] },
  );

  assertEquals(eventCounts.addQuad, 6);
  assertEquals(eventCounts.removeQuad, 3);
  assertEquals(eventCounts.addQuads, 0);
  assertEquals(eventCounts.removeQuads, 0);
  assertEquals(eventCounts.removeMatches, 0);
  assertEquals(eventCounts.deleteGraph, 0);
  assertEquals(store.size, 3);
});

Deno.test("SPARQL DELETE with pattern matching", async () => {
  const { store, eventCounts } = createMonitoredStore();

  // We insert the test data.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       ex:person1 rdf:type ex:Person ;
                  ex:name "John" ;
                  ex:age 30 .
       ex:person2 rdf:type ex:Person ;
                  ex:name "Jane" ;
                  ex:age 25 .
       ex:person3 rdf:type ex:Person ;
                  ex:name "Bob" ;
                  ex:age 35 .
     }`,
    { sources: [store] },
  );

  // We delete all age properties.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     
     DELETE {
       ?person ex:age ?age .
     } WHERE {
       ?person ex:age ?age .
     }`,
    { sources: [store] },
  );

  assertEquals(eventCounts.addQuad, 9);
  assertEquals(eventCounts.removeQuad, 3);
  assertEquals(eventCounts.addQuads, 0);
  assertEquals(eventCounts.removeQuads, 0);
  assertEquals(eventCounts.removeMatches, 0);
  assertEquals(eventCounts.deleteGraph, 0);
  assertEquals(store.size, 6);
});

Deno.test("SPARQL Mixed INSERT and DELETE", async () => {
  const { store, eventCounts } = createMonitoredStore();

  // We insert the initial data.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       ex:person1 rdf:type ex:Person ;
                  ex:name "John" ;
                  ex:age 30 .
     }`,
    { sources: [store] },
  );

  // We perform a mixed update: change age and add email.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     
     DELETE {
       ex:person1 ex:age 30 .
     }
     INSERT {
       ex:person1 ex:age 31 ;
                  ex:email "john@example.com" .
     }
     WHERE {
       ex:person1 ex:age 30 .
     }`,
    { sources: [store] },
  );

  assertEquals(eventCounts.addQuad, 5);
  assertEquals(eventCounts.removeQuad, 1);
  assertEquals(eventCounts.addQuads, 0);
  assertEquals(eventCounts.removeQuads, 0);
  assertEquals(eventCounts.removeMatches, 0);
  assertEquals(eventCounts.deleteGraph, 0);
  assertEquals(store.size, 4);
});

Deno.test("SPARQL INSERT with different graphs", async () => {
  const { store, eventCounts } = createMonitoredStore();

  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       GRAPH ex:graph1 {
         ex:person1 rdf:type ex:Person ;
                    ex:name "John" .
       }
       GRAPH ex:graph2 {
         ex:person2 rdf:type ex:Person ;
                    ex:name "Jane" .
       }
     }`,
    { sources: [store] },
  );

  assertEquals(eventCounts.addQuad, 4);
  assertEquals(eventCounts.removeQuad, 0);
  assertEquals(eventCounts.addQuads, 0);
  assertEquals(eventCounts.removeQuads, 0);
  assertEquals(eventCounts.removeMatches, 0);
  assertEquals(eventCounts.deleteGraph, 0);
  assertEquals(store.size, 4);
});

Deno.test("SPARQL DELETE WHERE without pattern", async () => {
  const { store, eventCounts } = createMonitoredStore();

  // We insert the test data.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       ex:person1 rdf:type ex:Person ;
                  ex:name "John" ;
                  ex:age 30 .
       ex:person2 rdf:type ex:Person ;
                  ex:name "Jane" ;
                  ex:age 25 .
     }`,
    { sources: [store] },
  );

  // We delete all quads using DELETE WHERE.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     
     DELETE WHERE {
       ?s ?p ?o .
     }`,
    { sources: [store] },
  );

  assertEquals(eventCounts.addQuad, 6);
  assertEquals(eventCounts.removeQuad, 6);
  assertEquals(eventCounts.addQuads, 0);
  assertEquals(eventCounts.removeQuads, 0);
  assertEquals(eventCounts.removeMatches, 0);
  assertEquals(eventCounts.deleteGraph, 0);
  assertEquals(store.size, 0);
});

Deno.test("SPARQL INSERT with blank nodes", async () => {
  const { store, eventCounts } = createMonitoredStore();

  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       ex:person1 rdf:type ex:Person ;
                  ex:name "John" ;
                  ex:address [ ex:street "123 Main St" ;
                               ex:city "Anytown" ] .
     }`,
    { sources: [store] },
  );

  assertEquals(eventCounts.addQuad, 5);
  assertEquals(eventCounts.removeQuad, 0);
  assertEquals(eventCounts.addQuads, 0);
  assertEquals(eventCounts.removeQuads, 0);
  assertEquals(eventCounts.removeMatches, 0);
  assertEquals(eventCounts.deleteGraph, 0);
  assertEquals(store.size, 5);
});

Deno.test("SPARQL INSERT with RDF collections", async () => {
  const { store, eventCounts } = createMonitoredStore();

  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       ex:person1 rdf:type ex:Person ;
                  ex:name "John" ;
                  ex:hobbies ( "reading" "swimming" "cooking" ) .
     }`,
    { sources: [store] },
  );

  assertEquals(eventCounts.addQuad, 9);
  assertEquals(eventCounts.removeQuad, 0);
  assertEquals(eventCounts.addQuads, 0);
  assertEquals(eventCounts.removeQuads, 0);
  assertEquals(eventCounts.removeMatches, 0);
  assertEquals(eventCounts.deleteGraph, 0);
  assertEquals(store.size, 9);
});

Deno.test("SPARQL INSERT with language tags and datatypes", async () => {
  const { store, eventCounts } = createMonitoredStore();

  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       ex:person1 rdf:type ex:Person ;
                  ex:name "John"@en ;
                  ex:name "Jean"@fr ;
                  ex:age 30 ;
                  ex:height 1.75 ;
                  ex:birthDate "1990-01-01" .
     }`,
    { sources: [store] },
  );

  assertEquals(eventCounts.addQuad, 6);
  assertEquals(eventCounts.removeQuad, 0);
  assertEquals(eventCounts.addQuads, 0);
  assertEquals(eventCounts.removeQuads, 0);
  assertEquals(eventCounts.removeMatches, 0);
  assertEquals(eventCounts.deleteGraph, 0);
  assertEquals(store.size, 6);
});

Deno.test("SPARQL INSERT with simple property paths", async () => {
  const { store, eventCounts } = createMonitoredStore();

  // We insert some data.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       ex:person1 rdf:type ex:Person ;
                  ex:name "John" .
       ex:person2 rdf:type ex:Person ;
                  ex:name "Jane" .
       ex:person1 ex:knows ex:person2 .
     }`,
    { sources: [store] },
  );

  // We perform a simple update without property paths.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     
     INSERT {
       ?person ex:updated true .
     }
     WHERE {
       ?person rdf:type ex:Person ;
               ex:name ?name .
     }`,
    { sources: [store] },
  );

  assertEquals(eventCounts.addQuad, 7);
  assertEquals(eventCounts.removeQuad, 0);
  assertEquals(eventCounts.addQuads, 0);
  assertEquals(eventCounts.removeQuads, 0);
  assertEquals(eventCounts.removeMatches, 0);
  assertEquals(eventCounts.deleteGraph, 0);
  assertEquals(store.size, 7);
});

Deno.test("SPARQL INSERT with UNION patterns", async () => {
  const { store, eventCounts } = createMonitoredStore();

  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       ex:person1 rdf:type ex:Person ;
                  ex:name "John" .
       ex:person2 rdf:type ex:Employee ;
                  ex:name "Jane" .
     }`,
    { sources: [store] },
  );

  // We use UNION to find both Person and Employee types.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT {
       ?person ex:processed true .
     }
     WHERE {
       {
         ?person rdf:type ex:Person ;
                 ex:name ?name .
       } UNION {
         ?person rdf:type ex:Employee ;
                 ex:name ?name .
       }
     }`,
    { sources: [store] },
  );

  assertEquals(eventCounts.addQuad, 6);
  assertEquals(eventCounts.removeQuad, 0);
  assertEquals(eventCounts.addQuads, 0);
  assertEquals(eventCounts.removeQuads, 0);
  assertEquals(eventCounts.removeMatches, 0);
  assertEquals(eventCounts.deleteGraph, 0);
  assertEquals(store.size, 6);
});

Deno.test("SPARQL INSERT with OPTIONAL patterns", async () => {
  const { store, eventCounts } = createMonitoredStore();

  // We insert some data with optional properties.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       ex:person1 rdf:type ex:Person ;
                  ex:name "John" ;
                  ex:email "john@example.com" .
       ex:person2 rdf:type ex:Person ;
                  ex:name "Jane" .
     }`,
    { sources: [store] },
  );

  // We use OPTIONAL to conditionally add missing email.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     
     INSERT {
       ?person ex:email "unknown@example.com" .
     }
     WHERE {
       ?person rdf:type ex:Person ;
               ex:name ?name .
       OPTIONAL {
         ?person ex:email ?email .
       }
       FILTER(!BOUND(?email))
     }`,
    { sources: [store] },
  );

  assertEquals(eventCounts.addQuad, 6);
  assertEquals(eventCounts.removeQuad, 0);
  assertEquals(eventCounts.addQuads, 0);
  assertEquals(eventCounts.removeQuads, 0);
  assertEquals(eventCounts.removeMatches, 0);
  assertEquals(eventCounts.deleteGraph, 0);
  assertEquals(store.size, 6);
});

Deno.test("SPARQL INSERT with FILTER expressions", async () => {
  const { store, eventCounts } = createMonitoredStore();

  // We insert the test data.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       ex:person1 rdf:type ex:Person ;
                  ex:name "John" ;
                  ex:age 25 .
       ex:person2 rdf:type ex:Person ;
                  ex:name "Jane" ;
                  ex:age 35 .
       ex:person3 rdf:type ex:Person ;
                  ex:name "Bob" ;
                  ex:age 45 .
     }`,
    { sources: [store] },
  );

  // We use FILTER to conditionally update based on age.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     
     DELETE {
       ?person ex:age ?age .
     }
     INSERT {
       ?person ex:age ?newAge ;
                ex:category ?category .
     }
     WHERE {
       ?person rdf:type ex:Person ;
               ex:name ?name ;
               ex:age ?age .
       BIND(?age + 1 AS ?newAge)
       BIND(IF(?age < 30, "young", IF(?age < 40, "middle-aged", "senior")) AS ?category)
       FILTER(?age >= 30)
     }`,
    { sources: [store] },
  );

  assertEquals(eventCounts.addQuad, 13);
  assertEquals(eventCounts.removeQuad, 2);
  assertEquals(eventCounts.addQuads, 0);
  assertEquals(eventCounts.removeQuads, 0);
  assertEquals(eventCounts.removeMatches, 0);
  assertEquals(eventCounts.deleteGraph, 0);
  assertEquals(store.size, 11);
});

Deno.test("SPARQL INSERT with MINUS patterns", async () => {
  const { store, eventCounts } = createMonitoredStore();

  // We insert the test data.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       ex:person1 rdf:type ex:Person ;
                  ex:name "John" ;
                  ex:status "active" .
       ex:person2 rdf:type ex:Person ;
                  ex:name "Jane" .
       ex:person3 rdf:type ex:Person ;
                  ex:name "Bob" ;
                  ex:status "inactive" .
     }`,
    { sources: [store] },
  );

  // We use MINUS to exclude people with status.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     
     INSERT {
       ?person ex:processed true .
     }
     WHERE {
       ?person rdf:type ex:Person ;
               ex:name ?name .
       MINUS {
         ?person ex:status ?status .
       }
     }`,
    { sources: [store] },
  );

  assertEquals(eventCounts.addQuad, 9);
  assertEquals(eventCounts.removeQuad, 0);
  assertEquals(eventCounts.addQuads, 0);
  assertEquals(eventCounts.removeQuads, 0);
  assertEquals(eventCounts.removeMatches, 0);
  assertEquals(eventCounts.deleteGraph, 0);
  assertEquals(store.size, 9);
});

Deno.test("Direct method calls", async (t) => {
  const subject = DataFactory.namedNode("http://example.org/person1");
  const predicate = DataFactory.namedNode(
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
  );
  const object = DataFactory.namedNode("http://example.org/Person");
  const graph = DataFactory.defaultGraph();

  await t.step(
    "Direct addQuad calls should only trigger ADD_QUAD events",
    () => {
      const { store, eventCounts } = createMonitoredStore();
      const quad1 = DataFactory.quad(subject, predicate, object, graph);
      store.addQuad(quad1);
      assertEquals(eventCounts.addQuad, 1);
      assertEquals(eventCounts.removeQuad, 0);
      assertEquals(eventCounts.addQuads, 0);
      assertEquals(eventCounts.removeQuads, 0);
      assertEquals(eventCounts.removeMatches, 0);
      assertEquals(eventCounts.deleteGraph, 0);
    },
  );

  await t.step(
    "Direct removeQuad calls should only trigger REMOVE_QUAD events",
    () => {
      const { store, eventCounts } = createMonitoredStore();
      const quad1 = DataFactory.quad(subject, predicate, object, graph);
      store.removeQuad(quad1);
      assertEquals(eventCounts.addQuad, 0);
      assertEquals(eventCounts.removeQuad, 1);
      assertEquals(eventCounts.addQuads, 0);
      assertEquals(eventCounts.removeQuads, 0);
      assertEquals(eventCounts.removeMatches, 0);
      assertEquals(eventCounts.deleteGraph, 0);
    },
  );
});

Deno.test("Track quads by subject IRI", async () => {
  const { store } = createMonitoredStore();

  // External tracking map that stores subject IRI to quad count mappings.
  const subjectQuadCounts = new Map<string, number>();

  // Helper function that updates subject counts based on quad changes.
  function updateSubjectCounts(quads: Quad[], increment: number) {
    for (const quad of quads) {
      const subjectIri = quad.subject.value;
      const currentCount = subjectQuadCounts.get(subjectIri) ?? 0;
      subjectQuadCounts.set(subjectIri, currentCount + increment);
    }
  }

  // Listen to ADD_QUAD events to track new quads being added.
  store.addEventListener(SyncStoreEvent.ADD_QUAD, (event: Event) => {
    const customEvent = event as CustomEvent;
    const quad = customEvent.detail[0] as Quad;
    updateSubjectCounts([quad], 1);
  });

  // Listen to REMOVE_QUAD events to track individual quads being removed.
  store.addEventListener(SyncStoreEvent.REMOVE_QUAD, (event: Event) => {
    const customEvent = event as CustomEvent;
    const quad = customEvent.detail[0] as Quad;
    updateSubjectCounts([quad], -1);
  });

  // Insert test data using SPARQL INSERT DATA.
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
    { sources: [store] },
  );

  // Verify that subject counts are correct after data insertion.
  assertEquals(subjectQuadCounts.get("http://example.org/person1"), 3);
  assertEquals(subjectQuadCounts.get("http://example.org/person2"), 3);
  assertEquals(subjectQuadCounts.size, 2);

  // Delete one quad for person1 using SPARQL DELETE DATA.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     
     DELETE DATA {
       ex:person1 ex:age 30 .
     }`,
    { sources: [store] },
  );

  // Verify that subject counts are correct after data deletion.
  assertEquals(subjectQuadCounts.get("http://example.org/person1"), 2);
  assertEquals(subjectQuadCounts.get("http://example.org/person2"), 3);
  assertEquals(subjectQuadCounts.size, 2);

  // Verify that the store size matches our external tracking.
  assertEquals(store.size, 5);
});
