import os, json, uuid, gradio as gr
from openai import OpenAI
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as dbsql
from datetime import datetime

MODEL = "databricks-meta-llama-3-3-70b-instruct"
USER_EMAIL = "jayanthdolai07@gmail.com"
_w = _client = _wh = None

def get_w():
    global _w
    if not _w: _w = WorkspaceClient()
    return _w

def get_client():
    global _client
    if not _client:
        host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
        tok = os.environ.get("DATABRICKS_TOKEN", "") or (get_w().config.token or "")
        _client = OpenAI(api_key=tok, base_url=host + "/serving-endpoints")
    return _client

def get_wh():
    global _wh
    if _wh: return _wh
    whs = list(get_w().warehouses.list())
    t = next((w for w in whs if "RUNNING" in str(w.state or "")), whs[0] if whs else None)
    if not t: raise RuntimeError("No warehouse")
    _wh = t.id
    return _wh

def run_sql(q):
    try:
        r = get_w().statement_execution.execute_statement(
            warehouse_id=get_wh(), statement=q, wait_timeout="30s")
        if r.status.state == dbsql.StatementState.SUCCEEDED:
            cols = [c.name for c in r.manifest.schema.columns]
            return [dict(zip(cols, row)) for row in (r.result.data_array or [])]
        return []
    except Exception as e:
        return [{"error": str(e)}]

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
            "losers": [x for x in r if x.get("mover_type") == "LOSER"][:limit]}

def search_news(query, num_results=3):
    try:
        from databricks.ai_search.client import VectorSearchClient
        idx = VectorSearchClient(disable_notice=True).get_index("stock-assistant-vs", "main.silver.news_for_search_index")
        hits = idx.similarity_search(query_text=query, columns=["ticker", "title", "sentiment"], num_results=num_results).get("result", {}).get("data_array", [])
        return [dict(zip(["ticker", "title", "sentiment"], h)) for h in hits]
    except:
        return run_sql("SELECT ticker,title,sentiment FROM main.silver.news_articles WHERE LOWER(title) LIKE '%" + query.lower() + "%' LIMIT " + str(num_results))

def add_to_watchlist(ticker, watchlist_name="My Watchlist"):
    run_sql("INSERT INTO main.agent.watchlists(id,user_email,watchlist,ticker,added_at) VALUES('" + str(uuid.uuid4()) + "','" + USER_EMAIL + "','" + watchlist_name + "','" + ticker.upper() + "','" + datetime.now().isoformat() + "')")
    return {"status": "success", "message": ticker.upper() + " added to '" + watchlist_name + "'"}

def save_research_note(ticker, note):
    clean = note[:500].replace("'", "''")
    run_sql("INSERT INTO main.agent.research_notes(id,user_email,ticker,note,created_at) VALUES('" + str(uuid.uuid4()) + "','" + USER_EMAIL + "','" + ticker.upper() + "','" + clean + "','" + datetime.now().isoformat() + "')")
    return {"status": "success", "message": "Note saved for " + ticker.upper()}

def save_analysis_report(ticker, report_text):
    clean = report_text[:1000].replace("'", "''")
    run_sql("INSERT INTO main.agent.analysis_reports(id,user_email,ticker,report_text,agent_model,generated_at) VALUES('" + str(uuid.uuid4()) + "','" + USER_EMAIL + "','" + ticker.upper() + "','" + clean + "','" + MODEL + "','" + datetime.now().isoformat() + "')")
    return {"status": "success", "message": "Report saved for " + ticker.upper()}

TOOLS = [
    {"type": "function", "function": {"name": "get_price_data", "description": "Get stock price data", "parameters": {"type": "object", "properties": {"ticker": {"type": "string"}}, "required": ["ticker"]}}},
    {"type": "function", "function": {"name": "get_sentiment", "description": "Get sentiment signal", "parameters": {"type": "object", "properties": {"ticker": {"type": "string"}}, "required": ["ticker"]}}},
    {"type": "function", "function": {"name": "compare_tickers", "description": "Compare multiple tickers", "parameters": {"type": "object", "properties": {"tickers": {"type": "array", "items": {"type": "string"}}}, "required": ["tickers"]}}},
    {"type": "function", "function": {"name": "get_top_movers", "description": "Get top gainers and losers", "parameters": {"type": "object", "properties": {"limit": {"type": "integer"}}}}},
    {"type": "function", "function": {"name": "search_news", "description": "Search news articles", "parameters": {"type": "object", "properties": {"query": {"type": "string"}, "num_results": {"type": "integer"}}, "required": ["query"]}}},
    {"type": "function", "function": {"name": "add_to_watchlist", "description": "Add ticker to watchlist", "parameters": {"type": "object", "properties": {"ticker": {"type": "string"}, "watchlist_name": {"type": "string"}}, "required": ["ticker"]}}},
    {"type": "function", "function": {"name": "save_research_note", "description": "Save research note", "parameters": {"type": "object", "properties": {"ticker": {"type": "string"}, "note": {"type": "string"}}, "required": ["ticker", "note"]}}},
    {"type": "function", "function": {"name": "save_analysis_report", "description": "Save analysis report", "parameters": {"type": "object", "properties": {"ticker": {"type": "string"}, "report_text": {"type": "string"}}, "required": ["ticker", "report_text"]}}}
]

TMAP = {
    "get_price_data": get_price_data, "get_sentiment": get_sentiment,
    "compare_tickers": compare_tickers, "get_top_movers": get_top_movers,
    "search_news": search_news, "add_to_watchlist": add_to_watchlist,
    "save_research_note": save_research_note, "save_analysis_report": save_analysis_report
}

def agent(query):
    msgs = [
        {"role": "system", "content": "You are an AI stock market assistant. Use tools to get current data. Be concise."},
        {"role": "user", "content": query}
    ]
    for _ in range(5):
        r = get_client().chat.completions.create(model=MODEL, messages=msgs, tools=TOOLS, tool_choice="auto")
        m = r.choices[0].message
        if not m.tool_calls:
            return m.content or ""
        msgs.append({"role": "assistant", "content": m.content,
                     "tool_calls": [{"id": tc.id, "type": "function", "function": {"name": tc.function.name, "arguments": tc.function.arguments}} for tc in m.tool_calls]})
        for tc in m.tool_calls:
            fn = tc.function.name
            args = json.loads(tc.function.arguments)
            res = TMAP[fn](**args) if fn in TMAP else {"error": "Unknown"}
            msgs.append({"role": "tool", "tool_call_id": tc.id, "content": json.dumps(res, default=str)})
    return "Max rounds reached"

def send_fn(question, history):
    if not question.strip():
        return history, ""
    resp = agent(question)
    separator = "-" * 50
    new_entry = "You: " + question + "\n\nAssistant: " + resp + "\n\n" + separator + "\n\n"
    return history + new_entry, ""

def market_fn():
    try:
        rows = run_sql("SELECT t.ticker,t.close,t.daily_return_pct,t.is_up_day,s.sentiment_signal FROM main.gold.ticker_daily_summary t LEFT JOIN main.gold.sentiment_summary s ON t.ticker=s.ticker ORDER BY t.daily_return_pct DESC")
        if not rows:
            return "No data yet - run pipeline first"
        out = "Market Summary\n\n"
        for r in rows:
            try:
                arrow = "UP" if str(r.get("is_up_day", "")).lower() == "true" else "DOWN"
                ret = float(r.get("daily_return_pct") or 0)
                out = out + r["ticker"] + ": $" + str(r["close"]) + " (" + ("+{:.2f}".format(ret) if ret >= 0 else "{:.2f}".format(ret)) + "%)" + " [" + arrow + "]\n"
            except:
                out = out + str(r.get("ticker", "?")) + "\n"
        return out
    except Exception as e:
        return "Error: " + str(e)

def watchlist_fn():
    try:
        rows = run_sql("SELECT ticker,watchlist FROM main.agent.watchlists WHERE user_email='" + USER_EMAIL + "' ORDER BY added_at DESC LIMIT 10")
        if not rows:
            return "No tickers in watchlist yet"
        out = "Watchlist\n\n"
        for r in rows:
            out = out + r["ticker"] + " - " + r["watchlist"] + "\n"
        return out
    except Exception as e:
        return "Error: " + str(e)

with gr.Blocks(title="AI Stock Research Assistant") as demo:
    gr.HTML("<h1>AI Stock Market Research Assistant</h1><p>Powered by Databricks Lakehouse + Llama 3.3 70B</p>")

    with gr.Row():
        with gr.Column(scale=3):
            history_box = gr.Textbox(label="Conversation", lines=15, interactive=False, value="Ask a question below to get started.")
            msg_box = gr.Textbox(label="Your question", placeholder="Ask about stocks, prices, news...")
            send_btn = gr.Button("Send", variant="primary")

            gr.HTML("<p>Try these:</p>")
            with gr.Row():
                b1 = gr.Button("Apple price + sentiment")
                b2 = gr.Button("Compare MSFT vs NVDA")
                b3 = gr.Button("Top movers today")

        with gr.Column(scale=1):
            mkt_box = gr.Textbox(label="Market Summary", lines=10, interactive=False, value="Click Refresh to load")
            refresh_mkt = gr.Button("Refresh Market")
            wl_box = gr.Textbox(label="Watchlist", lines=6, interactive=False, value="Click Refresh to load")
            refresh_wl = gr.Button("Refresh Watchlist")

    send_btn.click(fn=send_fn, inputs=[msg_box, history_box], outputs=[history_box, msg_box])
    msg_box.submit(fn=send_fn, inputs=[msg_box, history_box], outputs=[history_box, msg_box])
    refresh_mkt.click(fn=market_fn, outputs=mkt_box)
    refresh_wl.click(fn=watchlist_fn, outputs=wl_box)
    b1.click(fn=lambda: "What is Apple's current stock price and sentiment?", outputs=msg_box)
    b2.click(fn=lambda: "Compare MSFT and NVDA - which has better momentum today?", outputs=msg_box)
    b3.click(fn=lambda: "What are today's top gainers and losers?", outputs=msg_box)

if __name__ == "__main__":
    demo.launch()
