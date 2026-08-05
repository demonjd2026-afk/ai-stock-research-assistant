import os, json, uuid, gradio as gr
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as dbsql
from datetime import datetime

MODEL      = "databricks-meta-llama-3-3-70b-instruct"
USER_EMAIL = "jayanthdolai07@gmail.com"
_w = _wh   = None

def get_w():
    global _w
    if not _w: _w = WorkspaceClient()
    return _w

def get_wh():
    global _wh
    if _wh: return _wh
    whs = list(get_w().warehouses.list())
    t   = next((w for w in whs if "RUNNING" in str(w.state or "")), whs[0] if whs else None)
    if not t: raise RuntimeError("No warehouse")
    _wh = t.id; return _wh

def run_sql(q):
    try:
        r = get_w().statement_execution.execute_statement(
            warehouse_id=get_wh(), statement=q, wait_timeout="30s")
        if r.status.state == dbsql.StatementState.SUCCEEDED:
            cols = [c.name for c in r.manifest.schema.columns]
            return [dict(zip(cols, row)) for row in (r.result.data_array or [])]
        return []
    except Exception as e: return [{"error": str(e)}]

def call_llm(messages, tools=None):
    body = {"messages": messages, "max_tokens": 2048}
    if tools: body["tools"] = tools; body["tool_choice"] = "auto"
    return get_w().api_client.do(
        "POST", "/serving-endpoints/" + MODEL + "/invocations", body=body)

# ── Tool functions ────────────────────────────────────────────────────────────
def get_price_data(ticker):
    r = run_sql("SELECT ticker,name,close,open,high,low,volume,daily_return_pct,is_up_day,market_cap_billions FROM main.gold.ticker_daily_summary WHERE ticker='" + ticker.upper() + "' LIMIT 1")
    return r[0] if r else {"error": "No data for " + ticker}

def get_sentiment(ticker):
    r = run_sql("SELECT ticker,news_count,avg_sentiment_score,sentiment_signal,sentiment_confidence FROM main.gold.sentiment_summary WHERE ticker='" + ticker.upper() + "' LIMIT 1")
    return r[0] if r else {"error": "No sentiment for " + ticker}

def compare_tickers(tickers):
    t = ", ".join(["'" + x.upper() + "'" for x in tickers])
    return run_sql("SELECT ticker,name,close,daily_return_pct,market_cap_billions,avg_sentiment_score FROM main.gold.ticker_daily_summary WHERE ticker IN (" + t + ") ORDER BY daily_return_pct DESC")

def get_top_movers(limit=5):
    r = run_sql("SELECT return_rank,ticker,name,close,daily_return_pct,mover_type FROM main.gold.top_movers ORDER BY return_rank LIMIT " + str(limit * 2))
    return {"gainers": [x for x in r if x.get("mover_type") == "GAINER"][:limit],
            "losers":  [x for x in r if x.get("mover_type") == "LOSER"][:limit]}

def search_news(query, num_results=3):
    try:
        from databricks.ai_search.client import VectorSearchClient
        idx  = VectorSearchClient(disable_notice=True).get_index(
            "stock-assistant-vs", "main.silver.news_for_search_index")
        hits = idx.similarity_search(
            query_text=query,
            columns=["ticker","title","sentiment"],
            num_results=num_results).get("result",{}).get("data_array",[])
        return [dict(zip(["ticker","title","sentiment"], h)) for h in hits]
    except:
        return run_sql("SELECT ticker,title,sentiment FROM main.silver.news_articles WHERE LOWER(title) LIKE '%" + query.lower() + "%' LIMIT " + str(num_results))

def add_to_watchlist(ticker, watchlist_name="My Watchlist"):
    run_sql("INSERT INTO main.agent.watchlists(id,user_email,watchlist,ticker,added_at) VALUES('" + str(uuid.uuid4()) + "','" + USER_EMAIL + "','" + watchlist_name + "','" + ticker.upper() + "','" + datetime.now().isoformat() + "')")
    return {"status": "success", "message": ticker.upper() + " added to '" + watchlist_name + "'"}

def save_research_note(ticker, note):
    run_sql("INSERT INTO main.agent.research_notes(id,user_email,ticker,note,created_at) VALUES('" + str(uuid.uuid4()) + "','" + USER_EMAIL + "','" + ticker.upper() + "','" + note[:500].replace("'","''") + "','" + datetime.now().isoformat() + "')")
    return {"status": "success", "message": "Note saved for " + ticker.upper()}

def save_analysis_report(ticker, report_text):
    run_sql("INSERT INTO main.agent.analysis_reports(id,user_email,ticker,report_text,agent_model,generated_at) VALUES('" + str(uuid.uuid4()) + "','" + USER_EMAIL + "','" + ticker.upper() + "','" + report_text[:1000].replace("'","''") + "','" + MODEL + "','" + datetime.now().isoformat() + "')")
    return {"status": "success", "message": "Report saved for " + ticker.upper()}

TOOLS = [
    {"type":"function","function":{"name":"get_price_data","description":"Get stock price data","parameters":{"type":"object","properties":{"ticker":{"type":"string"}},"required":["ticker"]}}},
    {"type":"function","function":{"name":"get_sentiment","description":"Get sentiment signal","parameters":{"type":"object","properties":{"ticker":{"type":"string"}},"required":["ticker"]}}},
    {"type":"function","function":{"name":"compare_tickers","description":"Compare multiple tickers","parameters":{"type":"object","properties":{"tickers":{"type":"array","items":{"type":"string"}}},"required":["tickers"]}}},
    {"type":"function","function":{"name":"get_top_movers","description":"Get top gainers and losers","parameters":{"type":"object","properties":{"limit":{"type":"integer"}}}}},
    {"type":"function","function":{"name":"search_news","description":"Search news articles","parameters":{"type":"object","properties":{"query":{"type":"string"},"num_results":{"type":"integer"}},"required":["query"]}}},
    {"type":"function","function":{"name":"add_to_watchlist","description":"Add ticker to watchlist","parameters":{"type":"object","properties":{"ticker":{"type":"string"},"watchlist_name":{"type":"string"}},"required":["ticker"]}}},
    {"type":"function","function":{"name":"save_research_note","description":"Save research note","parameters":{"type":"object","properties":{"ticker":{"type":"string"},"note":{"type":"string"}},"required":["ticker","note"]}}},
    {"type":"function","function":{"name":"save_analysis_report","description":"Save analysis report","parameters":{"type":"object","properties":{"ticker":{"type":"string"},"report_text":{"type":"string"}},"required":["ticker","report_text"]}}}
]
TMAP = {
    "get_price_data":get_price_data, "get_sentiment":get_sentiment,
    "compare_tickers":compare_tickers, "get_top_movers":get_top_movers,
    "search_news":search_news, "add_to_watchlist":add_to_watchlist,
    "save_research_note":save_research_note, "save_analysis_report":save_analysis_report
}

def agent(query, conv_state):
    conv_state.append({"role":"user","content":query})
    for _ in range(5):
        r         = call_llm(conv_state, TOOLS)
        choices   = r.get("choices",[{}])
        msg       = choices[0].get("message",{}) if choices else {}
        tool_calls= msg.get("tool_calls",[])
        reply     = msg.get("content","")
        if not tool_calls:
            conv_state.append({"role":"assistant","content":reply})
            return reply, conv_state
        conv_state.append({"role":"assistant","content":reply,
            "tool_calls":[{"id":tc.get("id"),"type":"function",
                "function":{"name":tc.get("function",{}).get("name"),
                            "arguments":tc.get("function",{}).get("arguments","{}")}}
                for tc in tool_calls]})
        for tc in tool_calls:
            fn = tc.get("function",{}).get("name","")
            try: args = json.loads(tc.get("function",{}).get("arguments","{}"))
            except: args = {}
            res = TMAP[fn](**args) if fn in TMAP else {"error":"Unknown: "+fn}
            conv_state.append({"role":"tool","tool_call_id":tc.get("id"),
                               "content":json.dumps(res, default=str)})
    return "Max rounds reached", conv_state

# ── HTML helpers ──────────────────────────────────────────────────────────────
def esc(t): return t.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace("\n","<br>")

def user_bubble(text):
    return ("<div style='display:flex;justify-content:flex-end;margin:8px 0 4px 80px'>"
            "<div style='background:#2563eb;color:#fff;padding:10px 16px;"
            "border-radius:18px 18px 4px 18px;font-size:14px;line-height:1.6;word-wrap:break-word'>"
            + esc(text) + "</div></div>")

def assistant_bubble(text, thinking=False):
    inner = "<span style='color:#f59e0b;font-size:18px'>&#9881;</span> <i style='color:#94a3b8'>Thinking...</i>" if thinking else esc(text)
    return ("<div style='display:flex;justify-content:flex-start;margin:4px 80px 8px 0'>"
            "<div style='background:#1e293b;color:#e2e8f0;padding:10px 16px;"
            "border-radius:18px 18px 18px 4px;font-size:14px;line-height:1.6;"
            "border:1px solid #334155;word-wrap:break-word'>"
            + inner + "</div></div>")

def render_chat(pairs):
    html = "".join(user_bubble(q) + (assistant_bubble("", True) if a is None else assistant_bubble(a))
                   for q, a in pairs)
    return ("<div style='height:500px;overflow-y:auto;padding:16px 12px;"
            "background:#0f172a;border-radius:12px;border:1px solid #1e293b;'>"
            + html + "</div>")

WELCOME = ("<div style='height:500px;display:flex;align-items:center;justify-content:center;"
           "background:#0f172a;border-radius:12px;border:1px solid #1e293b'>"
           "<div style='text-align:center;color:#475569'>"
           "<div style='font-size:40px;margin-bottom:12px'>📈</div>"
           "<p style='font-size:15px;margin:0'>Ask me anything about stocks</p>"
           "<p style='font-size:12px;margin:6px 0 0;color:#334155'>Press Enter or ↑ to send</p>"
           "</div></div>")

SUGGESTIONS = [
    ("🍎", "Apple stock",     "What is Apple's current price and sentiment?"),
    ("🏆", "Top movers",      "What are today's top gainers and losers?"),
    ("⚡", "MSFT vs NVDA",    "Compare MSFT and NVDA — which has better momentum?"),
    ("📰", "AI stocks news",  "Search for recent AI technology stocks news"),
    ("📋", "Add to watchlist","Add Apple to my Tech Watchlist"),
    ("📊", "Sector view",     "Show me sector rankings and market cap breakdown"),
    ("💡", "Best pick today", "Which stock has the best combination of price gain and positive sentiment?"),
]

# ── Chat handler (generator for instant question display) ─────────────────────
def chat(question, pairs, conv_state):
    if not question.strip(): return
    # Immediately show question + thinking spinner
    new_pairs = pairs + [(question, None)]
    yield render_chat(new_pairs), "", pairs, conv_state
    # Run agent
    try:
        if not conv_state:
            conv_state = [{"role":"system","content":"You are an AI stock market assistant. Use tools to get real data. Be concise."}]
        reply, conv_state = agent(question, conv_state)
    except Exception as e:
        reply = "Error: " + str(e)
    final_pairs = pairs + [(question, reply)]
    yield render_chat(final_pairs), "", final_pairs, conv_state

def clear_chat(): return WELCOME, "", [], []

# ── Sidebar helpers ───────────────────────────────────────────────────────────
def market_fn():
    try:
        rows = run_sql("SELECT t.ticker,t.close,t.daily_return_pct,t.is_up_day FROM main.gold.ticker_daily_summary t ORDER BY t.daily_return_pct DESC")
        if not rows: return "No data yet. Run pipeline first."
        seen  = set(); lines = []
        for r in rows:
            if r["ticker"] in seen: continue
            seen.add(r["ticker"])
            d    = "🟢" if str(r.get("is_up_day","")).lower()=="true" else "🔴"
            ret  = float(r.get("daily_return_pct") or 0)
            sign = "+" if ret >= 0 else ""
            lines.append(d + " " + r["ticker"] + "  $" + str(r["close"]) + "  (" + sign + str(round(ret,2)) + "%)")
        return "\n".join(lines)
    except Exception as e: return "Error: " + str(e)

def watchlist_fn():
    try:
        rows = run_sql("SELECT ticker,watchlist FROM main.agent.watchlists WHERE user_email='" + USER_EMAIL + "' ORDER BY added_at DESC LIMIT 10")
        if not rows: return "No tickers yet."
        seen  = set(); lines = []
        for r in rows:
            key = r["ticker"] + r["watchlist"]
            if key in seen: continue
            seen.add(key); lines.append("• " + r["ticker"] + "  —  " + r["watchlist"])
        return "\n".join(lines)
    except Exception as e: return "Error: " + str(e)

# ── CSS ───────────────────────────────────────────────────────────────────────
CSS = """
.gradio-container { max-width:1500px !important; margin:0 auto !important; }
footer { display:none !important; }
.sugg-btn {
    text-align:left !important; padding:10px 14px !important;
    border-radius:10px !important; background:#0f172a !important;
    border:1px solid #1e293b !important; color:#94a3b8 !important;
    font-size:13px !important; margin-bottom:6px !important;
    width:100% !important; cursor:pointer !important; white-space:normal !important;
    line-height:1.4 !important;
}
.sugg-btn:hover { background:#1e293b !important; color:#e2e8f0 !important; border-color:#334155 !important; }
.send-btn {
    background:#2563eb !important; border-radius:8px !important;
    width:40px !important; height:40px !important; min-width:40px !important;
    padding:0 !important; font-size:18px !important; border:none !important;
    flex-shrink:0 !important;
}
.send-btn:hover { background:#1d4ed8 !important; }
.clear-btn {
    background:#1e293b !important; border-radius:8px !important;
    width:40px !important; height:40px !important; min-width:40px !important;
    padding:0 !important; font-size:16px !important; border:1px solid #334155 !important;
    flex-shrink:0 !important;
}
"""

# ── Layout ────────────────────────────────────────────────────────────────────
with gr.Blocks(css=CSS, title="AI Stock Research Assistant") as demo:

    pairs_state = gr.State([])
    conv_state  = gr.State([])

    # Header
    gr.HTML("""
    <div style='text-align:center;padding:20px 0 14px'>
        <h1 style='font-size:26px;font-weight:700;color:#f1f5f9;margin:0 0 4px'>
            📈 AI Stock Market Research Assistant
        </h1>
        <p style='font-size:12px;color:#475569;margin:0;letter-spacing:.4px'>
            Powered by Databricks Lakehouse &amp; Meta Llama 3.3 70B
        </p>
    </div>
    """)

    with gr.Row(equal_height=False):

        # ── LEFT: Suggestions ─────────────────────────────────
        with gr.Column(scale=1, min_width=200):
            gr.HTML("<p style='color:#64748b;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:1px;margin:0 0 10px'>Suggestions</p>")
            btns = []
            for icon, label, prompt in SUGGESTIONS:
                b = gr.Button(icon + "  " + label, elem_classes="sugg-btn")
                btns.append((b, prompt))

        # ── CENTER: Chat ──────────────────────────────────────
        with gr.Column(scale=3):
            chat_html = gr.HTML(value=WELCOME)

            # Input bar
            with gr.Row():
                msg_input = gr.Textbox(
                    placeholder="Message Stock Assistant...",
                    show_label=False, lines=1, scale=10,
                    container=False
                )
                send_btn  = gr.Button("↑", elem_classes="send-btn", scale=1)
                clear_btn = gr.Button("🗑️", elem_classes="clear-btn", scale=1)

        # ── RIGHT: Market data ────────────────────────────────
        with gr.Column(scale=1, min_width=220):
            gr.HTML("<p style='color:#64748b;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:1px;margin:0 0 6px'>📊 Market Summary</p>")
            market_out = gr.Textbox(value="", lines=9, interactive=False, show_label=False)
            gr.Button("🔄 Refresh", size="sm").click(fn=market_fn, outputs=market_out)

            gr.HTML("<p style='color:#64748b;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:1px;margin:14px 0 6px'>📋 Watchlist</p>")
            wl_out = gr.Textbox(value="", lines=6, interactive=False, show_label=False)
            gr.Button("🔄 Refresh", size="sm").click(fn=watchlist_fn, outputs=wl_out)

    # ── Wire events ───────────────────────────────────────────
    send_args = dict(fn=chat,
                     inputs=[msg_input, pairs_state, conv_state],
                     outputs=[chat_html, msg_input, pairs_state, conv_state])
    send_btn.click(**send_args)
    msg_input.submit(**send_args)
    clear_btn.click(fn=clear_chat, outputs=[chat_html, msg_input, pairs_state, conv_state])

    for btn, prompt in btns:
        btn.click(fn=lambda p=prompt: p, outputs=msg_input)

if __name__ == "__main__":
    demo.launch()
