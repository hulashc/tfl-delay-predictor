import styles from "./TflForecast.module.css";

/**
 * Live "next hour" TfL forecast, read from the public snapshot that the
 * Databricks job pushes to GitHub every hour.
 * Server component: fetched on the server and cached for 15 minutes (ISR).
 */

const SNAPSHOT_URL =
  "https://raw.githubusercontent.com/hulashc/tfl-delay-predictor/snapshot/latest.json";

type Band = "clear" | "watch" | "likely";

type Line = {
  id: string;
  name: string;
  mode: string;
  now: string;
  now_disrupted: boolean;
  probability: number;
  band: Band;
};

type Snapshot = {
  generated_at: string;
  scored_at: string;
  predicted_for: string;
  model_version: number;
  lines: Line[];
  track_record_24h: { caught: number; total: number; false_alarms: number; checked: number };
};

const LINE_COLOURS: Record<string, string> = {
  bakerloo: "#B36305", central: "#E32017", circle: "#FFD300", district: "#00782A",
  "hammersmith-city": "#F3A9BB", jubilee: "#A0A5A9", metropolitan: "#9B0056",
  northern: "#000000", piccadilly: "#003688", victoria: "#0098D4",
  "waterloo-city": "#95CDBA", dlr: "#00A4A7", elizabeth: "#6950A1",
  liberty: "#61686B", lioness: "#FAA61A", mildmay: "#0077AD",
  suffragette: "#5BB972", weaver: "#823A62", windrush: "#ED1B00",
};

const MODES: [string, string][] = [
  ["tube", "Underground"],
  ["elizabeth-line", "Elizabeth line"],
  ["dlr", "DLR"],
  ["overground", "Overground"],
];

const BAND_LABEL: Record<Band, string> = {
  clear: "Looks clear",
  watch: "Worth watching",
  likely: "Likely disrupted",
};

const STALE_AFTER_HOURS = 3;

const londonTime = new Intl.DateTimeFormat("en-GB", {
  timeZone: "Europe/London",
  hour: "2-digit",
  minute: "2-digit",
});
const londonDay = new Intl.DateTimeFormat("en-GB", {
  timeZone: "Europe/London",
  weekday: "long",
  day: "numeric",
  month: "short",
});

function textOn(hex: string): string {
  const [r, g, b] = [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16));
  return 0.299 * r + 0.587 * g + 0.114 * b > 150 ? "#1D1D1B" : "#FFFFFF";
}

function Badge({ line, small = false }: { line: Pick<Line, "id" | "name">; small?: boolean }) {
  const bg = LINE_COLOURS[line.id] ?? "#8A8F96";
  return (
    <span
      className={small ? `${styles.badge} ${styles.badgeSmall}` : styles.badge}
      style={{ background: bg, color: textOn(bg) }}
    >
      {line.name}
    </span>
  );
}

async function getSnapshot(): Promise<Snapshot | null> {
  try {
    const res = await fetch(SNAPSHOT_URL, { next: { revalidate: 900 } });
    if (!res.ok) return null;
    return (await res.json()) as Snapshot;
  } catch {
    return null;
  }
}

export default async function TflForecast() {
  const snap = await getSnapshot();

  if (!snap || snap.lines.length === 0) {
    return (
      <section className={styles.wrap} aria-label="TfL next-hour forecast">
        <p className={styles.when}>Live forecast</p>
        <p className={styles.meta}>The forecast isn&apos;t available right now. Check back shortly.</p>
      </section>
    );
  }

  const target = new Date(snap.predicted_for);
  const generated = new Date(snap.generated_at);
  const ageHours = (Date.now() - generated.getTime()) / 36e5;
  const stale = ageHours > STALE_AFTER_HOURS;

  const likely = snap.lines
    .filter((l) => l.band === "likely")
    .sort((a, b) => b.probability - a.probability);

  const knownModes = new Set(MODES.map(([m]) => m));
  const groups = [
    ...MODES,
    ...[...new Set(snap.lines.map((l) => l.mode))]
      .filter((m) => !knownModes.has(m))
      .map((m) => [m, m] as [string, string]),
  ]
    .map(([mode, title]) => ({
      title,
      lines: snap.lines.filter((l) => l.mode === mode).sort((a, b) => a.name.localeCompare(b.name)),
    }))
    .filter((g) => g.lines.length > 0);

  const t = snap.track_record_24h;

  return (
    <section className={styles.wrap} aria-label="TfL next-hour forecast">
      <p className={styles.when}>
        Forecast for {londonTime.format(target)}, {londonDay.format(target)}
      </p>

      <h2 className={styles.hero}>
        {likely.length === 0
          ? "Every line looks clear in an hour."
          : `${likely.length} ${likely.length === 1 ? "line looks" : "lines look"} likely to be disrupted in an hour.`}
      </h2>

      {likely.length > 0 && (
        <div className={styles.chips}>
          {likely.map((l) => (
            <Badge key={l.id} line={l} small />
          ))}
        </div>
      )}

      <p className={styles.meta}>
        Predicted from live TfL status, recent disruption history and London weather.
        Updated at {londonTime.format(generated)} using model version {snap.model_version}.
      </p>

      {stale && (
        <p className={styles.note}>
          This forecast is paused, so it&apos;s showing the last one from{" "}
          {londonTime.format(generated)} on {londonDay.format(generated)}.
        </p>
      )}

      {groups.map((g) => (
        <div key={g.title}>
          <h3 className={styles.group}>{g.title}</h3>
          <ul className={styles.list}>
            {g.lines.map((l) => (
              <li key={l.id} className={styles.row}>
                <Badge line={l} />
                <span className={l.now_disrupted ? `${styles.now} ${styles.nowBad}` : styles.now}>
                  Now: {l.now}
                </span>
                <span className={styles.forecast}>
                  <span className={`${styles.pill} ${styles[l.band]}`}>{BAND_LABEL[l.band]}</span>
                  <span className={styles.pct}>{Math.round(l.probability * 100)}%</span>
                </span>
              </li>
            ))}
          </ul>
        </div>
      ))}

      {t.checked > 0 && (
        <p className={styles.track}>
          In the last 24 hours it caught <strong>{t.caught} of {t.total}</strong> disruptions,
          with <strong>{t.false_alarms}</strong> false alarms across {t.checked} checked forecasts.
        </p>
      )}
    </section>
  );
}
