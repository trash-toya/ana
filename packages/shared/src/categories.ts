// Event categories (blueprint §"Event categories", docs/plan/04-services.md).
//
// The server-side list is the single source of truth; SQL never hard-codes
// event names. `cat` is a single char in the database: e engagement, l
// lifecycle, s system. Everything not reserved below — including custom
// names — is engagement.

/** Reserved name → category char. Never hard-code these names in SQL. */
export const CATEGORY: Readonly<Record<string, "l" | "s">> = {
  session_start: "l",
  session_end: "l",
  app_foreground: "l",
  app_background: "l",
  heartbeat: "s",
  sdk_dropped: "s",
} as const;

/** Map an event name to its category char ('e' | 'l' | 's'). */
export function categorize(name: string): "e" | "l" | "s" {
  return CATEGORY[name] ?? "e";
}
