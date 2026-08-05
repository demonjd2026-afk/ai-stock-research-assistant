# =============================================================
# AI Stock Market Research Assistant — Databricks App
# Minimal Gradio setup for maximum compatibility
# =============================================================

import os, json, uuid, gradio as gr
from openai import OpenAI
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as dbsql
from datetime import datetime

MODEL      = "databricks-meta-llama-3-3-70b-instruct"
USER_EMAIL = "jayanthdolai07@gmail.com"
_w = _client = _wh = None

def get_w():
    global _w
    if not _w: _w = WorkspaceClient()
    return _w

def get_client():
    global _client
    if not _client:
        host = os.environ.get("DATABRICKS_HOST","").rstrip("/")
        tok  = os.environ.get("DATABRICKS_TOKEN","") or (get_w().config.token or "")
        _client = OpenAI(api_key=tok, base_url=f"{host}/serving-endpoints")
    return _client

def get_wh():
    global _wh
    if _wh: return _wh
    whs = list(get_w().warehouses.list())
    t = next((w for w in whs if "RUNNING" in str(w.state or "")), whs[0] if whs else None)
    if not t: raise RuntimeError("No warehouse")
    _wh = t.id; return _wh

def sql(q):
    try:
        r = get_w().statement_execution.execute_statement(warehouse_id=get_wh(),statement=q,wait_timeout="30s")
        if r.status.state == dbsql.StatementState.SUCCEEDED:
            cols=[c.name for c in r.manifest.schema.columns]
            return [dict(zip(cols,row)) for row in (r.result.data_array or [])]
        return []
    except Exception as e: return [{"error":str(e)}]

def get_price_data(ticker):
    r=sql(f"SELECT ticker,name,close,open,high,low,volume,daily_return_pct,is_up_day,market_cap_billions FROM main.gold.ticker_daily_summary WHERE ticker='{ticker.upper()}' LIMIT 1")
    return r[0] if r else {"error":f"No data for {ticker}"}

def get_sentiment(ticker):
    r=sql(f"SELECT ticker,news_count,avg_sentiment_score,sentiment_signal,sentiment_confidence FROM main.gold.sentiment_summary WHERE ticker='{ticker.upper()}' LIMIT 1")
    return r[0] if r else {"error":f"No sentiment for {ticker}"}

def compare_tickers(tickers):
    t=", ".join([f"'{x.upper()}'" for x in tickers])
    return sql(f"SELECT ticker,name,close,daily_return_pct,market_cap_billions,avg_sentiment_score FROM main.gold.ticker_daily_summary WHERE ticker IN ({t}) ORDER BY daily_return_pct DESC")

def get_top_movers(limit=5):
    r=sql(f"SELECT return_rank,ticker,name,close,daily_return_pct,mover_type FROM main.gold.top_movers ORDER BY return_rank LIMIT {limit*2}")
    return {"gainers":[x for x in r if x.get("mover_type")=="GAINER"][:limit],"losers":[x for x in r if x.get("mover_type")=="LOSER"][:limit]}

def search_news(query,num_results=3):
    try:
        from databricks.ai_search.client import VectorSearchClient
        idx=VectorSearchClient(disable_notice=True).get_index("stock-assistant-vs","main.silver.news_for_search_index")
        hits=idx.similarity_search(query_text=query,columns=["ticker","title","sentiment"],num_results=num_results).get("result",{}).get("data_array",[])
        return [dict(zip(["ticker","title","sentiment"],h)) for h in hits]
    except:
        return sql(f"SELECT ticker,title,sentiment FROM main.silver.news_articles WHERE LOWER(title) LIKE '%{query.lower()}%' LIMIT {num_results}")

def add_to_watchlist(ticker,watchlist_name="My Watchlist"):
    sql(f"INSERT INTO main.agent.watchlists(id,user_email,watchlist,ticker,added_at)VALUES('{uuid.uuid4()}','{USER_EMAIL}','{watchlist_name}','{ticker.upper()}','{datetime.now().isoformat()}')")
    return {"status":"success","message":f"{ticker.upper()} added to '{watchlist_name}'"}

def save_research_note(ticker,note):
    sql(f"INSERT INTO main.agent.research_notes(id,user_email,ticker,note,created_at)VALUES('{uuid.uuid4()}','{USER_EMAIL}','{ticker.upper()}','{note[:500].replace(chr(39),chr(39)*2)}','{datetime.now().isoformat()}')")
    return {"status":"success","message":f"Note saved for {ticker.upper()}"}

def save_analysis_report(ticker,report_text):
    sql(f"INSERT INTO main.agent.analysis_reports(id,user_email,ticker,report_text,agent_model,generated_at)VALUES('{uuid.uuid4()}','{USER_EMAIL}','{ticker.upper()}','{report_text[:1000].replace(chr(39),chr(39)*2)}','{MODEL}','{datetime.now().isoformat()}')")
    return {"status":"success","message":f"Report saved for {ticker.upper()}"}

TOOLS=[
    {"type":"function","function":{"name":"get_price_data","description":"Get stock price data","parameters":{"type":"object","properties":{"ticker":{"type":"string"}},"required":["ticker"]}}},
    {"type":"function","function":{"name":"get_sentiment","description":"Get sentiment signal","parameters":{"type":"object","properties":{"ticker":{"type":"string"}},"required":["ticker"]}}},
    {"type":"function","function":{"name":"compare_tickers","description":"Compare multiple tickers","parameters":{"type":"object","properties":{"tickers":{"type":"array","items":{"type":"string"}}},"required":["tickers"]}}},
    {"type":"function","function":{"name":"get_top_movers","description":"Get top gainers/losers","parameters":{"type":"object","properties":{"limit":{"type":"integer"}}}}},
    {"type":"function","function":{"name":"search_news","description":"Search news articles","parameters":{"type":"object","properties":{"query":{"type":"string"},"num_results":{"type":"integer"}},"required":["query"]}}},
    {"type":"function","function":{"name":"add_to_watchlist","description":"Add to watchlist","parameters":{"type":"object","properties":{"ticker":{"type":"string"},"watchlist_name":{"type":"string"}},"required":["ticker"]}}},
    {"type":"function","function":{"name":"save_research_note","description":"Save research note","parameters":{"type":"object","properties":{"ticker":{"type":"string"},"note":{"type":"string"}},"required":["ticker","note"]}}},
    {"type":"function","function":{"name":"save_analysis_report","description":"Save analysis report","parameters":{"type":"object","properties":{"ticker":{"type":"string"},"report_text":{"type":"string"}},"required":["ticker","report_text"]}}}
]
TMAP={"get_price_data":get_price_data,"get_sentiment":get_sentiment,"compare_tickers":compare_tickers,
      "get_top_movers":get_top_movers,"search_news":search_news,"add_to_watchlist":add_to_watchlist,
      "save_research_note":save_research_note,"save_analysis_report":save_analysis_report}

def agent(query):
    msgs=[{"role":"system","content":"You are an AI stock market assistant. Use tools to get current data. Be concise."},
          {"role":"user","content":query}]
    trace=[]
    for _ in range(5):
        r=get_client().chat.completions.create(model=MODEL,messages=msgs,tools=TOOLS,tool_choice="auto")
        m=r.choices[0].message
        if not m.tool_calls:
            return m.content or ""
        msgs.append({"role":"assistant","content":m.content,"tool_calls":[{"id":tc.id,"type":"function","function":{"name":tc.function.name,"arguments":tc.function.arguments}} for tc in m.tool_calls]})
        for tc in m.tool_calls:
            fn=tc.function.name; args=json.loads(tc.function.arguments)
            res=TMAP[fn](**args) if fn in TMAP else {"error":"Unknown"}
            trace.append(f"→ {fn}({args})")
            msgs.append({"role":"tool","tool_call_id":tc.id,"content":json.dumps(res,default=str)})
    return "Max rounds reached"

def market_fn():
    try:
        rows=sql("SELECT t.ticker,t.close,t.daily_return_pct,t.is_up_day,s.sentiment_signal FROM main.gold.ticker_daily_summary t LEFT JOIN main.gold.sentiment_summary s ON t.ticker=s.ticker ORDER BY t.daily_return_pct DESC")
        if not rows: return "No data — click Refresh after running the pipeline"
        out="📊 Market Summary\n\n"
        for r in rows:
            try:
                arrow="🟢" if str(r.get("is_up_day","")).lower()=="true" else "🔴"
                ret=float(r.get("daily_return_pct") or 0)
                out+=f"{arrow} {r['ticker']}: ${r['close']} ({ret:+.2f}%)\n"
            except: out+=f"• {r.get('ticker','?')}\n"
        return out
    except Exception as e: return f"Error: {e}"

def watchlist_fn():
    try:
        rows=sql(f"SELECT ticker,watchlist FROM main.agent.watchlists WHERE user_email='{USER_EMAIL}' ORDER BY added_at DESC LIMIT 10")
        if not rows: return "No tickers in watchlist yet"
        return "📋 Watchlist\n\n" + "\n".join([f"• {r['ticker']} — {r['watchlist']}" for r in rows])
    except Exception as e: return f"Error: {e}"

# Simple gr.Interface approach — most compatible
with gr.Blocks() as demo:
    gr.HTML("<h1>📈 AI Stock Market Research Assistant</h1><p><i>Powered by Databricks Lakehouse + Llama 3.3 70B</i></p>")
    with gr.Row():
        with gr.Column(scale=3):
            history_box = gr.Textbox(label="Conversation", lines=15, interactive=False,
                                     value="Ask a question below to get started...")
            msg  = gr.Textbox(label="Your question", placeholder="Ask about stocks...")
            send = gr.Button("Send 🚀", variant="primary")
            gr.HTML("<p><b>Try:</b></p>")
            with gr.Row():
                b1 = gr.Button("Apple price + sentiment")
                b2 = gr.Button("Compare MSFT vs NVDA")
                b3 = gr.Button("Top movers")
        with gr.Column(scale=1):
            mkt=gr.Textbox(value="Click Refresh ↓", label="Market Summary", lines=8, interactive=False)
            gr.Button("🔄 Refresh Market").click(market_fn, outputs=mkt)
            wl=gr.Textbox(value="Click Refresh ↓", label="Watchlist", lines=6, interactive=False)
            gr.Button("🔄 Refresh Watchlist").click(watchlist_fn, outputs=wl)

    # Chat history as plain text
    conv_state = []
    def send_fn(question, history_text):
        if not question.strip():
            return history_text, ""
        resp = agent(question)
        new_line = f"You: {question}
Assistant: {resp}
{'-'*50}
"
        return history_text + "
" + new_line, ""

    send.click(send_fn, [msg, history_box], [history_box, msg])
    msg.submit(send_fn, [msg, history_box], [history_box, msg])
    b1.click(lambda: "What is Apple's price and sentiment?", outputs=msg)
    b2.click(lambda: "Compare MSFT and NVDA momentum today", outputs=msg)
    b3.click(lambda: "What are today's top gainers and losers?", outputs=msg)

if __name__ == "__main__":
    demo.launch()
