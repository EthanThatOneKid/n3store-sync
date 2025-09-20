import type { Quad } from "n3";
import { DataFactory } from "n3";
import {
  count,
  create,
  getByID,
  insert,
  type Orama,
  remove,
  search,
  type SearchParams,
} from "@orama/orama";
import { SyncStore, SyncStoreEvent } from "./sync-store.ts";

// We define the schema for our Orama database to index RDF quads.
const oramaSchema = {
  id: "string",
  subject: "string",
  predicate: "string",
  object: "string",
  graph: "string",
  objectType: "string", // "uri", "literal", "blank"
  objectValue: "string", // The actual value for literals
  objectLanguage: "string", // Language tag for literals
  objectDatatype: "string", // Datatype for literals
  // Vector search fields
  subjectEmbedding: "vector[384]", // Embedding for subject (using 384-dimensional vectors)
  predicateEmbedding: "vector[384]", // Embedding for predicate
  objectEmbedding: "vector[384]", // Embedding for object
  combinedEmbedding: "vector[384]", // Combined embedding for the entire quad
} as const;

// Simple mock embedding function that generates deterministic vectors based on text
function generateMockEmbedding(text: string): number[] {
  // Create a simple hash-based embedding for demonstration
  const hash = text.split("").reduce((a, b) => {
    a = ((a << 5) - a) + b.charCodeAt(0);
    return a & a;
  }, 0);

  // Generate 384-dimensional vector based on hash
  const embedding = new Array(384).fill(0);
  for (let i = 0; i < 384; i++) {
    embedding[i] = Math.sin(hash + i) * 0.5;
  }

  return embedding;
}

// We create a helper function to convert RDF quads to Orama documents.
function quadToOramaDoc(quad: Quad, id: string) {
  const objectType = quad.object.termType;
  const objectValue = quad.object.value;

  let objectLanguage = "";
  let objectDatatype = "";

  if (objectType === "Literal") {
    objectLanguage = quad.object.language ?? "";
    objectDatatype = quad.object.datatype?.value ?? "";
  }

  // Generate embeddings for each component
  const subjectEmbedding = generateMockEmbedding(quad.subject.value);
  const predicateEmbedding = generateMockEmbedding(quad.predicate.value);
  const objectEmbedding = generateMockEmbedding(objectValue);

  // Generate combined embedding for the entire quad
  const quadText =
    `${quad.subject.value} ${quad.predicate.value} ${objectValue}`;
  const combinedEmbedding = generateMockEmbedding(quadText);

  return {
    id,
    subject: quad.subject.value,
    predicate: quad.predicate.value,
    object: objectValue,
    graph: quad.graph.value,
    objectType,
    objectValue,
    objectLanguage,
    objectDatatype,
    subjectEmbedding,
    predicateEmbedding,
    objectEmbedding,
    combinedEmbedding,
  };
}

// We create a helper function to generate unique IDs for quads.
function generateQuadId(quad: Quad): string {
  const subject = quad.subject.value;
  const predicate = quad.predicate.value;
  const object = quad.object.value;
  const graph = quad.graph.value;
  const objectType = quad.object.termType;
  const objectLanguage = quad.object.termType === "Literal"
    ? (quad.object.language ?? "")
    : "";
  const objectDatatype = quad.object.termType === "Literal"
    ? (quad.object.datatype?.value ?? "")
    : "";

  // We create a deterministic ID based on all quad components including literal metadata.
  return `${subject}|${predicate}|${object}|${graph}|${objectType}|${objectLanguage}|${objectDatatype}`;
}

/**
 * OramaStore class that maintains synchronization between a SyncStore and an Orama database.
 * This class automatically keeps the Orama search index in sync with RDF data changes.
 */
export class OramaStore {
  private syncStore: SyncStore;
  private oramaDB: Orama<typeof oramaSchema>;
  private isInitialized = false;

  constructor() {
    this.syncStore = new SyncStore();
    this.oramaDB = create({ schema: oramaSchema });
    this.setupEventListeners();
  }

  /**
   * Sets up event listeners to keep the Orama database synchronized with the SyncStore.
   */
  private setupEventListeners(): void {
    this.syncStore.addEventListener(SyncStoreEvent.ADD_QUAD, (event: Event) => {
      const customEvent = event as CustomEvent;
      const quad = customEvent.detail[0] as Quad;
      const id = generateQuadId(quad);
      const doc = quadToOramaDoc(quad, id);
      insert(this.oramaDB, doc);
    });

    this.syncStore.addEventListener(
      SyncStoreEvent.REMOVE_QUAD,
      (event: Event) => {
        const customEvent = event as CustomEvent;
        const quad = customEvent.detail[0] as Quad;
        const id = generateQuadId(quad);
        remove(this.oramaDB, id);
      },
    );
  }

  /**
   * Gets the underlying SyncStore instance.
   * @returns The SyncStore instance
   */
  getStore(): SyncStore {
    return this.syncStore;
  }

  /**
   * Gets the underlying Orama database instance.
   * @returns The Orama database instance
   */
  getOramaDB(): Orama<typeof oramaSchema> {
    return this.oramaDB;
  }

  /**
   * Gets the number of quads in the SyncStore.
   * @returns The number of quads
   */
  getSize(): number {
    return this.syncStore.size;
  }

  /**
   * Gets the number of documents in the Orama database.
   * @returns A promise that resolves to the number of documents
   */
  async getDocumentCount(): Promise<number> {
    return await count(this.oramaDB);
  }

  /**
   * Verifies that the SyncStore and Orama database are in sync.
   * @returns A promise that resolves to true if they are in sync, false otherwise
   */
  async isInSync(): Promise<boolean> {
    const storeSize = this.getSize();
    const oramaCount = await this.getDocumentCount();
    return storeSize === oramaCount;
  }

  /**
   * Searches the Orama database.
   * @param searchParams - The search parameters
   * @returns A promise that resolves to the search results
   */
  async search(searchParams: SearchParams<Orama<typeof oramaSchema>>) {
    return await search(this.oramaDB, searchParams);
  }

  /**
   * Gets a document by ID from the Orama database.
   * @param id - The document ID
   * @returns A promise that resolves to the document or undefined
   */
  async getDocumentById(id: string) {
    return await getByID(this.oramaDB, id);
  }

  /**
   * Creates a quad from the given components.
   * @param subject - The subject URI
   * @param predicate - The predicate URI
   * @param object - The object value
   * @param graph - The graph URI (optional, defaults to default graph)
   * @returns The created quad
   */
  createQuad(
    subject: string,
    predicate: string,
    object: string,
    graph?: string,
  ): Quad {
    const subjectNode = DataFactory.namedNode(subject);
    const predicateNode = DataFactory.namedNode(predicate);
    const objectNode = DataFactory.literal(object);
    const graphNode = graph
      ? DataFactory.namedNode(graph)
      : DataFactory.defaultGraph();

    return DataFactory.quad(subjectNode, predicateNode, objectNode, graphNode);
  }

  /**
   * Adds a quad to the store (which will automatically sync to Orama).
   * @param quad - The quad to add
   */
  addQuad(quad: Quad): void {
    this.syncStore.addQuad(quad);
  }

  /**
   * Removes a quad from the store (which will automatically sync to Orama).
   * @param quad - The quad to remove
   */
  removeQuad(quad: Quad): void {
    this.syncStore.removeQuad(quad);
  }

  /**
   * Adds multiple quads to the store.
   * @param quads - Array of quads to add
   */
  addQuads(quads: Quad[]): void {
    for (const quad of quads) {
      this.syncStore.addQuad(quad);
    }
  }

  /**
   * Removes multiple quads from the store.
   * @param quads - Array of quads to remove
   */
  removeQuads(quads: Quad[]): void {
    for (const quad of quads) {
      this.syncStore.removeQuad(quad);
    }
  }

  /**
   * Performs vector search on the Orama database.
   * @param query - The search query text
   * @param vectorProperty - The vector property to search on (default: 'combinedEmbedding')
   * @param similarity - Minimum similarity threshold (default: 0.8)
   * @param limit - Maximum number of results (default: 10)
   * @returns A promise that resolves to the search results
   */
  async vectorSearch(
    query: string,
    vectorProperty: string = "combinedEmbedding",
    similarity: number = 0.8,
    limit: number = 10,
  ) {
    const queryEmbedding = generateMockEmbedding(query);
    return await search(this.oramaDB, {
      mode: "vector",
      vector: {
        value: queryEmbedding,
        property: vectorProperty,
      },
      similarity,
      limit,
    });
  }

  /**
   * Performs hybrid search combining full-text and vector search.
   * @param query - The search query text
   * @param vectorProperty - The vector property to search on (default: 'combinedEmbedding')
   * @param similarity - Minimum vector similarity threshold (default: 0.8)
   * @param limit - Maximum number of results (default: 10)
   * @param hybridWeights - Weights for text vs vector search (default: {text: 0.5, vector: 0.5})
   * @returns A promise that resolves to the search results
   */
  async hybridSearch(
    query: string,
    vectorProperty: string = "combinedEmbedding",
    similarity: number = 0.8,
    limit: number = 10,
    hybridWeights: { text: number; vector: number } = {
      text: 0.5,
      vector: 0.5,
    },
  ) {
    const queryEmbedding = generateMockEmbedding(query);
    return await search(this.oramaDB, {
      mode: "hybrid",
      term: query,
      vector: {
        value: queryEmbedding,
        property: vectorProperty,
      },
      similarity,
      limit,
      hybridWeights,
    });
  }

  /**
   * Generates an embedding for the given text using the same method as stored documents.
   * @param text - The text to embed
   * @returns The embedding vector
   */
  generateEmbedding(text: string): number[] {
    return generateMockEmbedding(text);
  }
}
