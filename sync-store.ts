// @deno-types="@types/n3"
import { OTerm, Store } from "n3";
import type { EventEmitter } from "node:events";

/**
 * N3 Store Internal Method Call Chain Documentation
 *
 * Understanding which methods call each other internally is crucial for accurate event dispatching.
 * Based on N3 source code analysis and comprehensive testing:
 *
 * DIRECT METHODS (no internal calls to other mutation methods):
 * - addQuad(): Direct quad addition, no internal calls
 * - removeQuad(): Direct quad removal, no internal calls
 *
 * BULK METHODS (call individual methods internally):
 * - addQuads([quad1, quad2]): Internally calls addQuad() for each quad
 *   → Dispatches: 1 ADD_QUADS + 2 ADD_QUAD events
 *
 * - removeQuads([quad1, quad2]): Internally calls removeQuad() for each quad
 *   → Dispatches: 1 REMOVE_QUADS + 2 REMOVE_QUAD events
 *
 * PATTERN-BASED METHODS (call base N3 methods directly):
 * - removeMatches(): Creates a stream of matching quads, then calls base remove(stream)
 *   → N3 Implementation: removeMatches() → remove(stream) → removeQuad() for each quad
 *   → Our SyncStore: Dispatches 1 REMOVE_MATCHES event only (no REMOVE_QUADS from base remove)
 *
 * GRAPH METHODS (call pattern-based methods):
 * - deleteGraph(graph): Internally calls removeMatches(null, null, null, graph)
 *   → N3 Implementation: deleteGraph() → removeMatches() → remove(stream) → removeQuad() for each quad
 *   → Our SyncStore: Dispatches 1 REMOVE_MATCHES + 1 DELETE_GRAPH events (no REMOVE_QUADS from base remove)
 *
 * CONFIRMED N3 SOURCE CODE CALL CHAINS:
 * 1. deleteGraph(graph) → removeMatches(null, null, null, graph)
 * 2. removeMatches(s,p,o,g) → remove(stream) → removeQuad(quad) for each matching quad
 * 3. addQuads(quads) → addQuad(quad) for each quad
 * 4. removeQuads(quads) → removeQuad(quad) for each quad
 *
 * IMPORTANT NOTES:
 * - The remove() method is NOT wrapped in our SyncStore (calls base N3 method directly)
 * - Comunica uses remove() internally for SPARQL DELETE operations, but we don't capture those events
 * - Our SyncStore wraps other methods to capture the actual quads being modified
 * - removeMatches() and deleteGraph() call base N3 remove() internally, so no REMOVE_QUADS events are dispatched
 * - Only direct calls to our wrapped methods (addQuad, removeQuad, addQuads, removeQuads) dispatch events
 */

/**
 * SyncStoreEvent contains the event names dispatched when the store is modified.
 */
export enum SyncStoreEvent {
  ADD_QUAD = "addQuad",
  REMOVE_QUAD = "removeQuad",
  ADD_QUADS = "addQuads",
  REMOVE_QUADS = "removeQuads",
  REMOVE_MATCHES = "removeMatches",
  DELETE_GRAPH = "deleteGraph",
}

/**
 * SyncStore extends the N3 Store class with event-driven synchronization capabilities.
 */
export class SyncStore extends Store {
  private target = new EventTarget();

  public addEventListener(
    event: SyncStoreEvent,
    listener: Parameters<EventTarget["addEventListener"]>[1],
    options?: Parameters<EventTarget["addEventListener"]>[2],
  ): void {
    this.target.addEventListener(event, listener, options);
  }

  public override addQuad(...args: Parameters<Store["addQuad"]>): boolean {
    const result = super.addQuad(...args);
    this.target.dispatchEvent(
      new CustomEvent(SyncStoreEvent.ADD_QUAD, { detail: args }),
    );
    return result;
  }

  public override removeQuad(
    ...args: Parameters<Store["removeQuad"]>
  ): boolean {
    const result = super.removeQuad(...args);
    this.target.dispatchEvent(
      new CustomEvent(SyncStoreEvent.REMOVE_QUAD, { detail: args }),
    );
    return result;
  }

  public override addQuads(...args: Parameters<Store["addQuads"]>): void {
    super.addQuads(...args);
    this.target.dispatchEvent(
      new CustomEvent(SyncStoreEvent.ADD_QUADS, { detail: args }),
    );
  }

  public override removeQuads(...args: Parameters<Store["removeQuads"]>): void {
    super.removeQuads(...args);
    this.target.dispatchEvent(
      new CustomEvent(SyncStoreEvent.REMOVE_QUADS, { detail: args }),
    );
  }

  public override removeMatches(
    ...args: Parameters<Store["removeMatches"]>
  ): EventEmitter {
    const quadsToRemove = this.getQuads(
      ...(args as [OTerm, OTerm, OTerm, OTerm]),
    );
    const result = super.removeMatches(...args);
    if (quadsToRemove.length > 0) {
      this.target.dispatchEvent(
        new CustomEvent(
          SyncStoreEvent.REMOVE_MATCHES,
          { detail: quadsToRemove },
        ),
      );
    }

    return result;
  }

  public override deleteGraph(
    ...args: Parameters<Store["deleteGraph"]>
  ): EventEmitter {
    // First, get all quads in the graph that will be deleted.
    const quadsToRemove = this.getQuads(
      null, // any subject
      null, // any predicate
      null, // any object
      args[0] as OTerm, // specific graph
    );

    // Then perform the actual deletion.
    const result = super.deleteGraph(...args);

    // Dispatch event with the actual quads that were removed.
    if (quadsToRemove.length > 0) {
      this.target.dispatchEvent(
        new CustomEvent(SyncStoreEvent.DELETE_GRAPH, { detail: quadsToRemove }),
      );
    }

    return result;
  }
}
