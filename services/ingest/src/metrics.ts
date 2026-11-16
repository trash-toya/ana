// Minimal Prometheus text exposition (04-services.md): counters and gauges,
// no client library — the ingest surface stays tiny and the dashboard service
// will reuse it. Series are keyed by name + sorted label pairs, so label order
// never duplicates a series.

export type Labels = Record<string, string>;

interface Series {
  name: string;
  labels: Labels;
  value: number;
}

export class Metrics {
  private readonly series = new Map<string, Series>();
  private readonly counters = new Set<string>();
  private readonly gauges = new Set<string>();

  private key(name: string, labels: Labels): string {
    const sorted = Object.keys(labels)
      .sort()
      .map((k) => `${k}\u0000${labels[k]}`)
      .join("\u0001");
    return `${name}\u0000${sorted}`;
  }

  /** Increment a counter (default step 1). */
  inc(name: string, labels: Labels = {}, value = 1): void {
    const k = this.key(name, labels);
    const existing = this.series.get(k);
    if (existing) existing.value += value;
    else this.series.set(k, { name, labels: { ...labels }, value });
    this.counters.add(name);
  }

  /** Set a gauge to an absolute value. */
  gauge(name: string, value: number, labels: Labels = {}): void {
    const k = this.key(name, labels);
    this.series.set(k, { name, labels: { ...labels }, value });
    this.gauges.add(name);
  }

  /** Prometheus text format 0.0.4. */
  render(): string {
    const lines: string[] = [];
    const names = [...this.counters, ...this.gauges];
    for (const name of names) {
      lines.push(`# TYPE ${name} ${this.counters.has(name) ? "counter" : "gauge"}`);
    }
    for (const s of this.series.values()) {
      const labelPart = Object.keys(s.labels).length
        ? `{${Object.keys(s.labels)
            .sort()
            .map((k) => `${k}="${escapeLabel(String(s.labels[k]))}"`)
            .join(",")}}`
        : "";
      lines.push(`${s.name}${labelPart} ${formatValue(s.value)}`);
    }
    lines.push("");
    return lines.join("\n");
  }
}

function escapeLabel(v: string): string {
  return v.replaceAll("\\", "\\\\").replaceAll('"', '\\"').replaceAll("\n", "\\n");
}

function formatValue(v: number): string {
  return String(v);
}
