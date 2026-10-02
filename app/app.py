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

# Official-ish line colours; anything unknown falls back to grey.
LINE_COLOURS = {
    "bakerloo": "#B36305", "central": "#E32017", "circle": "#FFD300", "district": "#00782A",
    "hammersmith-city": "#F3A9BB", "jubilee": "#A0A5A9", "metropolitan": "#9B0056",
    "northern": "#000000", "piccadilly": "#003688", "victoria": "#0098D4",
    "waterloo-city": "#95CDBA", "dlr": "#00A4A7", "elizabeth": "#6950A1",
    "liberty": "#61686B", "lioness": "#FAA61A", "mildmay": "#0077AD",
    "suffragette": "#5BB972", "weaver": "#823A62", "windrush": "#ED1B00",
}
NOW_LABEL = {0: "Good service", 1: "Minor disruption", 2: "Severe delays", 3: "Suspended"}
THRESHOLD = 0.5

st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Hanken+Grotesk:wght@400;600;800&display=swap" rel="stylesheet">
<style>
  html, body, [class*="css"], .stMarkdown, .stCaption { font-family: 'Hanken Grotesk', system-ui, sans-serif; }
  h1.headline { font-weight: 800; font-size: 2.4rem; line-height: 1.1; letter-spacing: -0.02em; margin: 0 0 .4rem; }
  p.sub { color: #5a5f66; margin: 0 0 1.6rem; }
  .board { border-top: 3px solid #0019A8; }
  .row { display: grid; grid-template-columns: 10px 1fr 38% 3.2rem; gap: .9rem; align-items: center;
         padding: .7rem 0; border-bottom: 1px solid #e3e5e8; }
  .stripe { height: 2.2rem; border-radius: 2px; }
  .name { font-weight: 600; font-size: 1.02rem; }
  .now { display: block; font-weight: 400; font-size: .85rem; color: #5a5f66; }
  .now.bad { color: #b3261e; }
  .bar { height: .55rem; background: #eef0f2; border-radius: 99px; overflow: hidden; }
  .fill { height: 100%; border-radius: 99px; }
  .pct { text-align: right; font-weight: 800; font-variant-numeric: tabular-nums; }
  .pct.hot { color: #b3261e; }
  @media (max-width: 520px) { .row { grid-template-columns: 8px 1fr 30% 2.8rem; gap: .6rem; } }
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


try:
    latest = query("""
        SELECT * FROM tfl.gold.predictions
        WHERE time_bucket = (SELECT MAX(time_bucket) FROM tfl.gold.predictions)
    """)
    history = query("""
        SELECT p.line_id, p.line_name, p.predicted_for, p.disruption_prob, p.predicted_disrupted,
               f.is_disrupted AS actual_disrupted
        FROM tfl.gold.predictions p
        LEFT JOIN tfl.gold.line_features f
          ON f.line_id = p.line_id AND f.time_bucket = p.predicted_for
        WHERE p.predicted_for >= current_timestamp() - INTERVAL 24 HOURS
    """)
except Exception as e:
    st.error(
        "The forecast couldn't be loaded. Check that the SQL warehouse is running and the app "
        f"has SELECT access on tfl.gold. Details: {e}"
    )
    st.stop()

if latest.empty:
    st.info("No predictions yet. Run the scoring job once and refresh this page.")
    st.stop()

# ---------- Header ----------
scored_at = to_london(latest["scored_at"]).max()
predicted_for = to_london(latest["predicted_for"]).max()
version = int(latest["model_version"].max())

st.markdown(f'<h1 class="headline">How will your line look at {predicted_for:%H:%M}?</h1>',
            unsafe_allow_html=True)
st.markdown(
    f'<p class="sub">Chance of disruption one hour ahead, for every Tube, DLR, Overground and '
    f'Elizabeth line. Updated {scored_at:%H:%M} by model version {version}.</p>',
    unsafe_allow_html=True,
)

# ---------- Board ----------
rows = []
for r in latest.sort_values("disruption_prob", ascending=False).itertuples():
    colour = LINE_COLOURS.get(r.line_id, "#8a8f96")
    pct = round(r.disruption_prob * 100)
    level = None if pd.isna(r.current_delay_level) else int(r.current_delay_level)
    now = NOW_LABEL.get(level, "Planned works or other notice")
    rows.append(f"""
      <div class="row">
        <div class="stripe" style="background:{colour}"></div>
        <div class="name">{html.escape(r.line_name)}
          <span class="now {'bad' if level and level >= 1 else ''}">Now: {now}</span></div>
        <div class="bar"><div class="fill" style="width:{pct}%;background:{colour}"></div></div>
        <div class="pct {'hot' if r.disruption_prob >= THRESHOLD else ''}">{pct}%</div>
      </div>""")
st.markdown(f'<div class="board">{"".join(rows)}</div>', unsafe_allow_html=True)

# ---------- Track record ----------
st.subheader("How it's been doing")
checked = history.dropna(subset=["actual_disrupted"])
if checked.empty:
    st.caption("Predictions are checked against what actually happened an hour later. "
               "Come back once a few hours of predictions have played out.")
else:
    actual = checked["actual_disrupted"].astype(int)
    pred = checked["predicted_disrupted"].astype(int)
    caught = int(((pred == 1) & (actual == 1)).sum())
    total = int((actual == 1).sum())
    false_alarms = int(((pred == 1) & (actual == 0)).sum())
    c1, c2, c3 = st.columns(3)
    c1.metric("Disruptions caught", f"{caught} of {total}")
    c2.metric("False alarms", false_alarms)
    c3.metric("Predictions checked", len(checked))
    st.caption("Last 24 hours.")

# ---------- Per-line history ----------
st.subheader("One line over the last day")
names = latest.sort_values("line_name")["line_name"].tolist()
choice = st.selectbox("Line", names, index=names.index("Victoria") if "Victoria" in names else 0)
h = history[history["line_name"] == choice].copy()
if h.empty:
    st.caption("No history for this line yet.")
else:
    h["time"] = to_london(h["predicted_for"]).dt.tz_localize(None)
    colour = LINE_COLOURS.get(h["line_id"].iloc[0], "#8a8f96")
    prob = alt.Chart(h).mark_line(color=colour, strokeWidth=2.5).encode(
        x=alt.X("time:T", title=None),
        y=alt.Y("disruption_prob:Q", title="Chance of disruption", axis=alt.Axis(format="%"),
                scale=alt.Scale(domain=[0, 1])),
    )
    hits = alt.Chart(h[h["actual_disrupted"] == 1]).mark_tick(color="#b3261e", thickness=3, size=14).encode(
        x="time:T", y=alt.value(0),
    )
    st.altair_chart(prob + hits, use_container_width=True)
    st.caption("Red marks along the bottom show when the line was actually disrupted.")

st.caption("Data: TfL Unified API and Open-Meteo. Built on Databricks: Lakeflow, Unity Catalog, MLflow.")
