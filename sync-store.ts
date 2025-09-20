// @deno-types="@types/n3"
import { OTerm, Store } from "n3";
import type { EventEmitter } from "node:events";

/**
 * SyncStoreEvent are the event names dispatched when the store is modified.
 */
export enum SyncStoreEvent {
  ADD_QUAD = "addQuad",
  REMOVE_QUAD = "removeQuad",
  ADD_QUADS = "addQuads",
  REMOVE_QUADS = "removeQuads",
}

/**
 * SyncStore extends the N3 Store class with events.
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

  public override remove(
    ...args: Parameters<Store["remove"]>
  ): EventEmitter {
    const result = super.remove(...args);
    this.target.dispatchEvent(
      new CustomEvent(SyncStoreEvent.REMOVE_QUADS, { detail: args }),
    );
    return result;
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
      args[0] as OTerm,
      args[1] as OTerm,
      args[2] as OTerm,
      args[3] as OTerm,
    );
    if (!(quadsToRemove.length > 0)) {
      throw new Error("Invalid quads");
    }
    const result = super.removeMatches(...args);
    if (quadsToRemove.length > 0) {
      this.target.dispatchEvent(
        new CustomEvent(SyncStoreEvent.REMOVE_QUADS, { detail: quadsToRemove }),
      );
    }

    return result;
  }

  public override deleteGraph(
    ...args: Parameters<Store["deleteGraph"]>
  ): EventEmitter {
    // First, get all quads in the graph that will be deleted
    const quadsToRemove = this.getQuads(
      null, // any subject
      null, // any predicate
      null, // any object
      args[0] as OTerm, // specific graph
    );

    // Then perform the actual deletion
    const result = super.deleteGraph(...args);

    // Dispatch event with the actual quads that were removed
    if (quadsToRemove.length > 0) {
      this.target.dispatchEvent(
        new CustomEvent(SyncStoreEvent.REMOVE_QUADS, { detail: quadsToRemove }),
      );
    }

    return result;
  }
}
