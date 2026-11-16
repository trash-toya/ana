// Page context: URL, referrer, UTM (04-services.md step 5). Everything is
// resolved at emit time, so SPA route changes are captured per event.

export interface PageContext {
  url: string | null;
  referrer: string | null;
  utm: Record<string, string>;
}

/** UTM parameters off the current URL's query string. */
export function utmFromSearch(search: string): Record<string, string> {
  const utm: Record<string, string> = {};
  if (!search) return utm;
  let params: URLSearchParams;
  try {
    params = new URLSearchParams(search.startsWith("?") ? search.slice(1) : search);
  } catch {
    return utm;
  }
  for (const key of [
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
  ]) {
    const v = params.get(key);
    if (v) utm[key.slice(4)] = v;
  }
  return utm;
}

/** Context for the current page view (browser globals, injectable for tests). */
export function pageContext(win: {
  location: { href: string; search: string };
  document: { referrer: string };
}): PageContext {
  let url: string | null = null;
  try {
    url = new URL(win.location.href).pathname + win.location.search;
  } catch {
    url = null;
  }
  return {
    url,
    referrer: win.document.referrer || null,
    utm: utmFromSearch(win.location.search),
  };
}
