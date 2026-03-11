/**
 * Throws an error indicating an unreachable code path.
 * Used as a runtime assertion in exhaustive checks and action runners.
 */
export function unreachable(message: string): never {
  throw new Error(`Unreachable: ${message}`);
}
