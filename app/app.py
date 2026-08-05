import os, json, uuid, gradio as gr
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as dbsql
from datetime import datetime

MODEL = "databricks-meta-llama-3-3-70b-instruct"
USER_EMAIL = "jayanthdolai07@gmail.com"
_w = _wh = None

def get_w():
    global _w
    if not _w:
        _w = WorkspaceClient()
    return _w

def call_llm(messages, tools=None):
    """Call Foundation Models API using SDK authenticated client."""
    body = {"messages": messages, "max_tokens": 2048}
    if tools:
        body["tools"] = tools
        body["tool_choice"] = "auto"
    result = get_w().api_client.do(
        "POST",
        "/serving-endpoints/" + MODEL + "/invocations",
        body=body
    )
    return result

def get_wh():
    global _wh
    if _wh:
        return _wh
    whs = list(get_w().warehouses.list())
    t = next((w for w in whs if "RUNNING" in str(w.state or "")), whs[0] if whs else None)
    if not t:
        raise RuntimeError("No warehouse available")
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
    return {"gainers": [x for x in r if x.get("mover_type") == "GAINER"][:limit], "losers": [x for x in r if x.get("mover_type") == "LOSER"][:limit]}

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
    run_sql("INSERT INTO main.agent.research_notes(id,user_email,ticker,note,created_at) VALUES('" + str(uuid.uuid4()) + "','" + USER_EMAIL + "','" + ticker.upper() + "','" + note[:500].replace("'", "''") + "','" + datetime.now().isoformat() + "')")
    return {"status": "success", "message": "Note saved for " + ticker.upper()}

def save_analysis_report(ticker, report_text):
    run_sql("INSERT INTO main.agent.analysis_reports(id,user_email,ticker,report_text,agent_model,generated_at) VALUES('" + str(uuid.uuid4()) + "','" + USER_EMAIL + "','" + ticker.upper() + "','" + report_text[:1000].replace("'", "''") + "','" + MODEL + "','" + datetime.now().isoformat() + "')")
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

def agent(query, conversation_msgs):
    """Run agent with full conversation history for continuous chat."""
    conversation_msgs.append({"role": "user", "content": query})
    for _ in range(5):
        r = call_llm(conversation_msgs, TOOLS)
        choices = r.get("choices", [{}])
        msg = choices[0].get("message", {}) if choices else {}
        tool_calls = msg.get("tool_calls", [])
        reply_text = msg.get("content", "")

        if not tool_calls:
            conversation_msgs.append({"role": "assistant", "content": reply_text})
            return reply_text, conversation_msgs

        conversation_msgs.append({
            "role": "assistant", "content": reply_text,
            "tool_calls": [{"id": tc.get("id"), "type": "function",
                           "function": {"name": tc.get("function", {}).get("name"),
                                       "arguments": tc.get("function", {}).get("arguments", "{}")}}
                          for tc in tool_calls]})
        for tc in tool_calls:
            fn = tc.get("function", {}).get("name", "")
            try:
                args = json.loads(tc.get("function", {}).get("arguments", "{}"))
            except:
                args = {}
            res = TMAP[fn](**args) if fn in TMAP else {"error": "Unknown tool: " + fn}
            conversation_msgs.append({"role": "tool", "tool_call_id": tc.get("id"), "content": json.dumps(res, default=str)})
    return "Max rounds reached", conversation_msgs

def make_bubble(role, text):
    if role == "user":
        return (
            "<div style='display:flex;justify-content:flex-end;margin:8px 0'>"
            "<div style='background:#1a56db;color:#fff;padding:10px 16px;border-radius:18px 18px 4px 18px;"
            "max-width:80%;font-size:14px;line-height:1.5'>"
            "<b>You</b><br>" + text.replace("\n","<br>") +
            "</div></div>"
        )
    else:
        return (
            "<div style='display:flex;justify-content:flex-start;margin:8px 0'>"
            "<div style='background:#1e2530;color:#e2e8f0;padding:10px 16px;border-radius:18px 18px 18px 4px;"
            "max-width:80%;font-size:14px;line-height:1.5;border:1px solid #2d3748'>"
            "<b>Assistant</b><br>" + text.replace("\n","<br>") +
            "</div></div>"
        )

CHAT_WRAPPER = (
    "<div style='height:480px;overflow-y:auto;padding:12px;background:#111827;"
    "border-radius:8px;border:1px solid #374151'>{}</div>"
)

def chat(question, html_history, conv_state):
    if not question.strip():
        return html_history, "", conv_state
    try:
        if not conv_state:
            conv_state = [{"role": "system", "content": "You are an AI stock market assistant. Use tools to get current data. Be concise and data-driven."}]
        reply, conv_state = agent(question, conv_state)
        bubbles = html_history + make_bubble("user", question) + make_bubble("assistant", reply)
        return bubbles, "", conv_state
    except Exception as e:
        bubbles = html_history + make_bubble("user", question) + make_bubble("assistant", "Error: " + str(e))
        return bubbles, "", conv_state

def clear_chat():
    return "", "", []

def market_summary():
    try:
        rows = run_sql("SELECT t.ticker,t.close,t.daily_return_pct,t.is_up_day,s.sentiment_signal FROM main.gold.ticker_daily_summary t LEFT JOIN main.gold.sentiment_summary s ON t.ticker=s.ticker ORDER BY t.daily_return_pct DESC")
        if not rows:
            return "No data yet. Run pipeline first."
        lines = ["=== Market Summary ==="]
        for r in rows:
            try:
                direction = "UP" if str(r.get("is_up_day", "")).lower() == "true" else "DOWN"
                ret = float(r.get("daily_return_pct") or 0)
                sign = "+" if ret >= 0 else ""
                lines.append(r["ticker"] + ": $" + str(r["close"]) + " (" + sign + str(round(ret, 2)) + "%) [" + direction + "]")
            except:
                lines.append(str(r.get("ticker", "?")))
        return "\n".join(lines)
    except Exception as e:
        return "Error: " + str(e)

def watchlist():
    try:
        rows = run_sql("SELECT ticker,watchlist FROM main.agent.watchlists WHERE user_email='" + USER_EMAIL + "' ORDER BY added_at DESC LIMIT 10")
        if not rows:
            return "No tickers in watchlist yet."
        lines = ["=== Watchlist ==="]
        for r in rows:
            lines.append(r.get("ticker", "?") + " - " + r.get("watchlist", ""))
        return "\n".join(lines)
    except Exception as e:
        return "Error: " + str(e)

with gr.Blocks(title="AI Stock Research Assistant") as demo:
    gr.HTML("<h2>AI Stock Market Research Assistant</h2><p>Powered by Databricks Lakehouse and Llama 3.3 70B</p>")

    conv_state = gr.State([])

    with gr.Row():
        with gr.Column(scale=2):
            history_box = gr.HTML(value="<div style='height:480px;overflow-y:auto;padding:12px;background:#111827;border-radius:8px;border:1px solid #374151;color:#9ca3af;font-size:14px'>Welcome! Ask me anything about stocks...</div>")
            question_input = gr.Textbox(
                label="Your question",
                placeholder="What is Apple's stock price? Compare MSFT vs NVDA. Top movers today?",
                lines=2
            )
            with gr.Row():
                ask_btn   = gr.Button("Ask Agent", variant="primary")
                clear_btn = gr.Button("Clear Chat")

            gr.HTML("<p><b>Quick questions:</b></p>")
            with gr.Row():
                q1 = gr.Button("Apple price + sentiment")
                q2 = gr.Button("Top movers today")
                q3 = gr.Button("Compare MSFT vs NVDA")

        with gr.Column(scale=1):
            market_out = gr.Textbox(label="Market Summary", lines=12, interactive=False)
            gr.Button("Refresh Market").click(fn=market_summary, outputs=market_out)
            watchlist_out = gr.Textbox(label="My Watchlist", lines=8, interactive=False)
            gr.Button("Refresh Watchlist").click(fn=watchlist, outputs=watchlist_out)

    ask_btn.click(fn=chat, inputs=[question_input, history_box, conv_state], outputs=[history_box, question_input, conv_state])
    question_input.submit(fn=chat, inputs=[question_input, history_box, conv_state], outputs=[history_box, question_input, conv_state])
    clear_btn.click(fn=clear_chat, outputs=[history_box, question_input, conv_state])
    q1.click(fn=lambda: "What is Apple's current stock price and sentiment?", outputs=question_input)
    q2.click(fn=lambda: "What are today's top gainers and losers?", outputs=question_input)
    q3.click(fn=lambda: "Compare MSFT and NVDA which has better momentum today?", outputs=question_input)

if __name__ == "__main__":
    demo.launch()
