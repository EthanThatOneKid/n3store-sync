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

  // First insert data.
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

  // Then delete specific data.
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

  // Insert initial data.
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

  // Delete all persons with age > 28.
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

Deno.test("SPARQL Multiple INSERT operations", async () => {
  const { store, eventCounts } = createMonitoredStore();

  // First batch.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       ex:person1 rdf:type ex:Person ;
                  ex:name "John" .
     }`,
    { sources: [store] },
  );

  // Second batch.
  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       ex:person2 rdf:type ex:Person ;
                  ex:name "Jane" .
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

Deno.test("SPARQL Complex INSERT patterns", async () => {
  const { store, eventCounts } = createMonitoredStore();

  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       ex:person1 rdf:type ex:Person ;
                  ex:name "John Doe" ;
                  ex:age 30 ;
                  ex:email "john@example.com" ;
                  ex:worksFor ex:company1 .
       
       ex:company1 rdf:type ex:Company ;
                   ex:name "Acme Corp" .
     }`,
    { sources: [store] },
  );

  // Verify only ADD_QUAD events occurred.
  assertEquals(eventCounts.addQuad, 7);
  assertEquals(eventCounts.removeQuad, 0);
  assertEquals(eventCounts.addQuads, 0);
  assertEquals(eventCounts.removeQuads, 0);
  assertEquals(eventCounts.removeMatches, 0);
  assertEquals(eventCounts.deleteGraph, 0);
  assertEquals(store.size, 7);
});

Deno.test("SPARQL DELETE with pattern matching", async () => {
  const { store, eventCounts } = createMonitoredStore();

  // Insert test data.
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

  // Delete all age properties.
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

Deno.test("SPARQL Mixed INSERT and DELETE", async () => {
  const { store, eventCounts } = createMonitoredStore();

  // Insert initial data.
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

  // Mixed update: change age and add email.
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

Deno.test("SPARQL Large batch operations", async () => {
  const { store, eventCounts } = createMonitoredStore();

  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       ex:person1 rdf:type ex:Person ; ex:name "Person 1" ; ex:age 20 .
       ex:person2 rdf:type ex:Person ; ex:name "Person 2" ; ex:age 21 .
       ex:person3 rdf:type ex:Person ; ex:name "Person 3" ; ex:age 22 .
       ex:person4 rdf:type ex:Person ; ex:name "Person 4" ; ex:age 23 .
       ex:person5 rdf:type ex:Person ; ex:name "Person 5" ; ex:age 24 .
     }`,
    { sources: [store] },
  );

  assertEquals(eventCounts.addQuad, 15);
  assertEquals(eventCounts.removeQuad, 0);
  assertEquals(eventCounts.addQuads, 0);
  assertEquals(eventCounts.removeQuads, 0);
  assertEquals(eventCounts.removeMatches, 0);
  assertEquals(eventCounts.deleteGraph, 0);
  assertEquals(store.size, 15);
});

Deno.test("SPARQL Error handling", async () => {
  const { store, eventCounts } = createMonitoredStore();

  await queryEngine.queryVoid(
    `PREFIX ex: <http://example.org/>
     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     
     INSERT DATA {
       ex:person1 rdf:type ex:Person ;
                  ex:name "John" .
     }`,
    { sources: [store] },
  );

  assertEquals(eventCounts.addQuad, 2);
  assertEquals(eventCounts.removeQuad, 0);
  assertEquals(eventCounts.addQuads, 0);
  assertEquals(eventCounts.removeQuads, 0);
  assertEquals(eventCounts.removeMatches, 0);
  assertEquals(eventCounts.deleteGraph, 0);
  assertEquals(store.size, 2);
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

  await t.step(
    "Direct addQuads calls trigger both ADD_QUADS and individual ADD_QUAD events",
    () => {
      const { store, eventCounts } = createMonitoredStore();
      const quad1 = DataFactory.quad(subject, predicate, object, graph);
      const quad2 = DataFactory.quad(
        subject,
        DataFactory.namedNode("http://example.org/name"),
        DataFactory.literal("John"),
        graph,
      );
      store.addQuads([quad1, quad2]);
      assertEquals(eventCounts.addQuad, 2);
      assertEquals(eventCounts.removeQuad, 0);
      assertEquals(eventCounts.addQuads, 1);
      assertEquals(eventCounts.removeQuads, 0);
      assertEquals(eventCounts.removeMatches, 0);
      assertEquals(eventCounts.deleteGraph, 0);
    },
  );

  await t.step(
    "Direct removeQuads calls trigger both REMOVE_QUADS and individual REMOVE_QUAD events",
    () => {
      const { store, eventCounts } = createMonitoredStore();
      const quad1 = DataFactory.quad(subject, predicate, object, graph);
      const quad2 = DataFactory.quad(
        subject,
        DataFactory.namedNode("http://example.org/name"),
        DataFactory.literal("John"),
        graph,
      );
      store.removeQuads([quad1, quad2]);
      assertEquals(eventCounts.addQuad, 0);
      assertEquals(eventCounts.removeQuad, 2);
      assertEquals(eventCounts.addQuads, 0);
      assertEquals(eventCounts.removeQuads, 1);
      assertEquals(eventCounts.removeMatches, 0);
      assertEquals(eventCounts.deleteGraph, 0);
    },
  );

  await t.step(
    "removeMatches method should trigger REMOVE_MATCHES events",
    () => {
      const { store, eventCounts } = createMonitoredStore();
      const quad1 = DataFactory.quad(subject, predicate, object, graph);
      store.addQuad(quad1);
      store.removeMatches(subject, predicate, object, graph);
      assertEquals(eventCounts.addQuad, 1);
      assertEquals(eventCounts.removeQuad, 0);
      assertEquals(eventCounts.addQuads, 0);
      assertEquals(eventCounts.removeQuads, 0);
      assertEquals(eventCounts.removeMatches, 1);
      assertEquals(eventCounts.deleteGraph, 0);
    },
  );

  await t.step("deleteGraph method should trigger DELETE_GRAPH events", () => {
    const { store, eventCounts } = createMonitoredStore();
    const quad1 = DataFactory.quad(subject, predicate, object, graph);
    store.addQuad(quad1);
    store.deleteGraph(graph);
    assertEquals(eventCounts.addQuad, 1);
    assertEquals(eventCounts.removeQuad, 0);
    assertEquals(eventCounts.addQuads, 0);
    assertEquals(eventCounts.removeQuads, 0);
    assertEquals(eventCounts.removeMatches, 1);
    assertEquals(eventCounts.deleteGraph, 1);
  });
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

  // Listen to REMOVE_MATCHES events to track bulk quad removals.
  store.addEventListener(SyncStoreEvent.REMOVE_MATCHES, (event: Event) => {
    const customEvent = event as CustomEvent;
    const quads = customEvent.detail as Quad[];
    updateSubjectCounts(quads, -1);
  });

  // Listen to DELETE_GRAPH events to track graph deletions.
  store.addEventListener(SyncStoreEvent.DELETE_GRAPH, (event: Event) => {
    const customEvent = event as CustomEvent;
    const quads = customEvent.detail as Quad[];
    updateSubjectCounts(quads, -1);
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
