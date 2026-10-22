import type { SessionObservation } from "./consolidation-types.js";

/** Metadata registered by one Pi runtime while it uses a session file. */
export interface SessionUsage extends SessionObservation {
  readonly ownerId: string;
  readonly processToken: string;
  readonly state: "active" | "closing" | "released";
}
