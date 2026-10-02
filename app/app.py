"""
Next-hour line forecast - Databricks App.
Reads tfl.gold.predictions (and actuals from tfl.gold.line_features) via a SQL warehouse.
"""
import html
import os

import altair as alt
import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

st.set_page_config(page_title="Next-hour line forecast", page_icon="🚇", layout="centered")

LINE_COLOURS = {
    "bakerloo": "#B36305", "central": "#E32017", "circle": "#FFD300", "district": "#00782A",
    "hammersmith-city": "#F3A9BB", "jubilee": "#A0A5A9", "metropolitan": "#9B0056",
    "northern": "#000000", "piccadilly": "#003688", "victoria": "#0098D4",
    "waterloo-city": "#95CDBA", "dlr": "#00A4A7", "elizabeth": "#6950A1",
    "liberty": "#61686B", "lioness": "#FAA61A", "mildmay": "#0077AD",
    "suffragette": "#5BB972", "weaver": "#823A62", "windrush": "#ED1B00",
}
MODE_ORDER = [("tube", "Underground"), ("elizabeth-line", "Elizabeth line"),
              ("dlr", "DLR"), ("overground", "Overground")]
NOW_LABEL = {0: "Good service", 1: "Minor disruption", 2: "Severe delays", 3: "Suspended"}
THRESHOLD = 0.5
BLUE = "#0019A8"

st.markdown(f"""
<link href="https://fonts.googleapis.com/css2?family=Hanken+Grotesk:wght@400;500;700;800&display=swap" rel="stylesheet">
<style>
  html, body, [class*="css"], .stMarkdown, p, h1, h2, h3, label {{
    font-family: 'Hanken Grotesk', system-ui, sans-serif !important; }}
  .block-container {{ padding-top: 2.5rem; max-width: 760px; }}

  .when {{ color: {BLUE}; font-weight: 700; font-size: 1rem; margin: 0 0 .5rem; }}
  .hero {{ font-weight: 800; font-size: 2.6rem; line-height: 1.05; letter-spacing: -0.025em;
           margin: 0 0 1rem; color: #1D1D1B; }}
  .chips {{ display: flex; flex-wrap: wrap; gap: .4rem; margin-bottom: 1rem; }}
  .meta {{ color: #5F6368; font-size: .92rem; margin: 0 0 2.2rem; max-width: 60ch; }}
  .note {{ background: #FFF6D6; border-left: 4px solid #FFD300; padding: .7rem .9rem;
           font-size: .9rem; color: #3D3A2E; margin: -1.2rem 0 2.2rem; }}

  .group {{ font-weight: 700; font-size: .95rem; color: #5F6368; margin: 1.6rem 0 .3rem; }}
  .row {{ display: grid; grid-template-columns: 11rem 1fr auto; gap: 1rem; align-items: center;
          padding: .35rem 0; border-bottom: 1px solid #E4E7EB; }}
  .badge {{ padding: .55rem .7rem; font-weight: 700; font-size: .95rem; line-height: 1.15; }}
  .now {{ font-size: .92rem; color: #1D1D1B; }}
  .now.bad {{ color: #B3261E; font-weight: 700; }}
  .fc {{ display: flex; align-items: baseline; gap: .5rem; justify-content: flex-end; min-width: 8.5rem; }}
  .pill {{ font-weight: 700; font-size: .85rem; padding: .2rem .6rem; border-radius: 99px; }}
  .pill.clear {{ background: #E3F1E6; color: #0B6B2B; }}
  .pill.watch {{ background: #FFF1C2; color: #7A5B00; }}
  .pill.likely {{ background: #B3261E; color: #FFFFFF; }}
  .pct {{ font-size: .85rem; color: #5F6368; font-variant-numeric: tabular-nums; width: 2.6rem; text-align: right; }}

  @media (max-width: 560px) {{
    .hero {{ font-size: 2rem; }}
    .row {{ grid-template-columns: 1fr auto; }}
    .now {{ grid-column: 1; grid-row: 2; padding-bottom: .3rem; }}
    .fc {{ grid-column: 2; grid-row: 1 / span 2; min-width: 0; }}
  }}
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=600, show_spinner="Loading the latest forecast")
def query(q: str) -> pd.DataFrame:
    cfg = Config()
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}",
        credentials_provider=lambda: cfg.authenticate,
    ) as conn, conn.cursor() as cur:
        cur.execute(q)
        return cur.fetchall_arrow().to_pandas()


def to_london(s: pd.Series) -> pd.Series:
    s = pd.to_datetime(s)
    if s.dt.tz is None:
        s = s.dt.tz_localize("UTC")
    return s.dt.tz_convert("Europe/London")


def text_on(hex_colour: str) -> str:
    """Black or white text, whichever reads better on the line colour."""
    r, g, b = (int(hex_colour[i:i + 2], 16) for i in (1, 3, 5))
    return "#1D1D1B" if (0.299 * r + 0.587 * g + 0.114 * b) > 150 else "#FFFFFF"


def band(p: float) -> tuple[str, str]:
    if p >= THRESHOLD:
        return "likely", "Likely disrupted"
    if p >= 0.25:
        return "watch", "Worth watching"
    return "clear", "Looks clear"


def badge(line_id: str, name: str, small: bool = False) -> str:
    c = LINE_COLOURS.get(line_id, "#8A8F96")
    size = "font-size:.85rem;padding:.25rem .55rem;" if small else ""
    return f'<span class="badge" style="background:{c};color:{text_on(c)};{size}">{html.escape(name)}</span>'


try:
    latest = query("""
        SELECT * FROM tfl.gold.predictions
        WHERE time_bucket = (SELECT MAX(time_bucket) FROM tfl.gold.predictions)
    """)
    history = query("""
        SELECT p.line_id, p.line_name, p.predicted_for, p.disruption_prob, p.predicted_disrupted,
               p.scored_at, f.is_disrupted AS actual_disrupted
        FROM tfl.gold.predictions p
        LEFT JOIN tfl.gold.line_features f
          ON f.line_id = p.line_id AND f.time_bucket = p.predicted_for
        WHERE p.predicted_for >= current_timestamp() - INTERVAL 24 HOURS
    """)
    first_scored = query("SELECT MIN(scored_at) AS t FROM tfl.gold.predictions")["t"]
except Exception as e:
    st.error(
        "The forecast couldn't be loaded. Check that the SQL warehouse is running and the app "
        f"has SELECT access on tfl.gold. Details: {e}"
    )
    st.stop()

if latest.empty:
    st.info("No predictions yet. Run the scoring job once, then refresh this page.")
    st.stop()

# ---------- Hero ----------
scored_at = to_london(latest["scored_at"]).max()
target = to_london(latest["predicted_for"]).max()
version = int(latest["model_version"].max())
likely = latest[latest["disruption_prob"] >= THRESHOLD].sort_values("disruption_prob", ascending=False)

st.markdown(f'<p class="when">Forecast for {target:%H:%M}, {target:%A}</p>', unsafe_allow_html=True)
if likely.empty:
    st.markdown('<h1 class="hero">Every line looks clear in an hour.</h1>', unsafe_allow_html=True)
else:
    n = len(likely)
    st.markdown(
        f'<h1 class="hero">{n} {"line looks" if n == 1 else "lines look"} likely to be disrupted in an hour.</h1>',
        unsafe_allow_html=True,
    )
    chips = "".join(badge(r.line_id, r.line_name, small=True) for r in likely.itertuples())
    st.markdown(f'<div class="chips">{chips}</div>', unsafe_allow_html=True)

st.markdown(
    f'<p class="meta">Predicted from live TfL status, recent disruption history and London weather. '
    f'Updated at {scored_at:%H:%M} using model version {version}.</p>',
    unsafe_allow_html=True,
)

days_live = (pd.Timestamp.now(tz="UTC") - to_london(first_scored).min().tz_convert("UTC")).days
if days_live < 3:
    st.markdown(
        '<div class="note">Early days: the model has learned from less than a few days of data, '
        'so its forecasts swing between extremes. They become more measured as history builds up.</div>',
        unsafe_allow_html=True,
    )

# ---------- Status board, grouped by mode ----------
for mode, title in MODE_ORDER + [(m, m.title()) for m in latest["mode"].unique()
                                 if m not in dict(MODE_ORDER)]:
    group = latest[latest["mode"] == mode]
    if group.empty:
        continue
    rows = []
    for r in group.sort_values("line_name").itertuples():
        level = None if pd.isna(r.current_delay_level) else int(r.current_delay_level)
        now = NOW_LABEL.get(level, "Planned works or notice")
        cls, label = band(r.disruption_prob)
        rows.append(f"""
          <div class="row">
            {badge(r.line_id, r.line_name)}
            <div class="now {'bad' if level and level >= 1 else ''}">Now: {now}</div>
            <div class="fc"><span class="pill {cls}">{label}</span>
              <span class="pct">{round(r.disruption_prob * 100)}%</span></div>
          </div>""")
    st.markdown(f'<div class="group">{title}</div>{"".join(rows)}', unsafe_allow_html=True)

# ---------- Track record ----------
st.markdown("<br>", unsafe_allow_html=True)
st.subheader("How the forecast has done")
checked = history.dropna(subset=["actual_disrupted"])
if checked.empty:
    st.caption("Each forecast is checked against what actually happened an hour later. "
               "Results appear here once the first forecasts have played out.")
else:
    actual = checked["actual_disrupted"].astype(int)
    pred = checked["predicted_disrupted"].astype(int)
    c1, c2, c3 = st.columns(3)
    c1.metric("Disruptions caught", f"{int(((pred == 1) & (actual == 1)).sum())} of {int(actual.sum())}")
    c2.metric("False alarms", int(((pred == 1) & (actual == 0)).sum()))
    c3.metric("Forecasts checked", len(checked))
    st.caption("Last 24 hours.")

# ---------- Per-line history ----------
st.subheader("One line over the last day")
names = latest.sort_values("line_name")["line_name"].tolist()
choice = st.selectbox("Line", names, index=names.index("Victoria") if "Victoria" in names else 0,
                      label_visibility="collapsed")
h = history[history["line_name"] == choice].copy()
if h.empty:
    st.caption("No history for this line yet.")
else:
    h["time"] = to_london(h["predicted_for"]).dt.tz_localize(None)
    colour = LINE_COLOURS.get(h["line_id"].iloc[0], "#8A8F96")
    base = alt.Chart(h).encode(x=alt.X("time:T", title=None, axis=alt.Axis(format="%H:%M", grid=False)))
    prob = base.mark_line(color=colour, strokeWidth=3, interpolate="step-after").encode(
        y=alt.Y("disruption_prob:Q", title="Chance of disruption",
                axis=alt.Axis(format="%", tickCount=3), scale=alt.Scale(domain=[0, 1])),
        tooltip=[alt.Tooltip("time:T", format="%H:%M"), alt.Tooltip("disruption_prob:Q", format=".0%")],
    )
    hits = alt.Chart(h[h["actual_disrupted"] == 1]).mark_tick(color="#B3261E", thickness=4, size=16).encode(
        x="time:T", y=alt.value(0))
    st.altair_chart((prob + hits).properties(height=220).configure_view(strokeWidth=0),
                    use_container_width=True)
    st.caption("The line shows the forecast. Red marks along the bottom are times the line was actually disrupted.")

st.caption("Data from the TfL Unified API and Open-Meteo. Built on Databricks with Lakeflow, Unity Catalog and MLflow.")
