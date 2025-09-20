// @deno-types="@types/n3"
import { Store } from "n3";
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
  public target = new EventTarget();

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
    const result = super.removeMatches(...args);
    this.target.dispatchEvent(
      new CustomEvent(SyncStoreEvent.REMOVE_QUADS, { detail: args }),
    );
    return result;
  }

  public override deleteGraph(
    ...args: Parameters<Store["deleteGraph"]>
  ): EventEmitter {
    const result = super.deleteGraph(...args);
    this.target.dispatchEvent(
      new CustomEvent(SyncStoreEvent.REMOVE_QUADS, { detail: args }),
    );
    return result;
  }
}
