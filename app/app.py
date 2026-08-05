# =============================================================
# AI Stock Market Research Assistant — Databricks App
# File    : app/app.py
# Model   : databricks-meta-llama-3-3-70b-instruct
#           (Databricks Foundation Models — no extra API key needed)
# Deploy  : Databricks Apps (Gradio frontend)
# =============================================================

import os
import json
import uuid
import gradio as gr
from openai import OpenAI
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as dbsql
from datetime import datetime

# ------------------------------------------------------------------
# 0. Config
# ------------------------------------------------------------------
MODEL      = "databricks-meta-llama-3-3-70b-instruct"
USER_EMAIL = "jayanthdolai07@gmail.com"

# Lazy init — DATABRICKS_TOKEN and DATABRICKS_HOST are auto-injected
# by Databricks Apps runtime. Only access them on first use, not at
# module import time (which is why earlier versions crashed).
_w      = None
_client = None

def get_w():
    global _w
    if _w is None:
        _w = WorkspaceClient()
    return _w

def get_client():
    global _client
    if _client is None:
        host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
        tok  = os.environ.get("DATABRICKS_TOKEN", "")
        if not tok:
            # Fallback: get token from SDK
            tok = get_w().config.token or ""
        _client = OpenAI(
            api_key  = tok,
            base_url = f"{host}/serving-endpoints"
        )
    return _client

# ------------------------------------------------------------------
# 1. SQL helper — queries Delta tables via SQL Warehouse
# ------------------------------------------------------------------
_warehouse_id = None

def get_warehouse_id() -> str:
    global _warehouse_id
    if _warehouse_id:
        return _warehouse_id
    warehouses = list(get_w().warehouses.list())
    running    = [wh for wh in warehouses if "RUNNING" in str(wh.state or "")]
    target     = running[0] if running else (warehouses[0] if warehouses else None)
    if not target:
        raise RuntimeError("No SQL warehouse available")
    _warehouse_id = target.id
    return _warehouse_id

def run_sql(query: str) -> list:
    try:
        result = get_w().statement_execution.execute_statement(
            warehouse_id = get_warehouse_id(),
            statement    = query,
            wait_timeout = "30s"
        )
        if result.status.state == dbsql.StatementState.SUCCEEDED:
            cols = [col.name for col in result.manifest.schema.columns]
            rows = result.result.data_array or []
            return [dict(zip(cols, row)) for row in rows]
        return []
    except Exception as e:
        return [{"error": str(e)}]

# ------------------------------------------------------------------
# 2. Tool implementations
# ------------------------------------------------------------------
def get_price_data(ticker: str) -> dict:
    rows = run_sql(f"""
        SELECT ticker, name, close, open, high, low, volume,
               daily_return_pct, price_range, is_up_day,
               market_cap_billions, sector
        FROM main.gold.ticker_daily_summary
        WHERE ticker = '{ticker.upper()}' LIMIT 1
    """)
    return rows[0] if rows else {"error": f"No data for {ticker}"}

def get_sentiment(ticker: str) -> dict:
    rows = run_sql(f"""
        SELECT ticker, name, close, news_count, avg_sentiment_score,
               positive_count, negative_count, neutral_count,
               sentiment_signal, sentiment_confidence
        FROM main.gold.sentiment_summary
        WHERE ticker = '{ticker.upper()}' LIMIT 1
    """)
    return rows[0] if rows else {"error": f"No sentiment for {ticker}"}

def compare_tickers(tickers: list) -> list:
    tickers_str = ", ".join([f"'{t.upper()}'" for t in tickers])
    return run_sql(f"""
        SELECT ticker, name, close, daily_return_pct,
               market_cap_billions, avg_sentiment_score,
               news_count, is_up_day
        FROM main.gold.ticker_daily_summary
        WHERE ticker IN ({tickers_str})
        ORDER BY daily_return_pct DESC
    """)

def get_top_movers(limit: int = 5) -> dict:
    rows = run_sql(f"""
        SELECT return_rank, ticker, name, close,
               daily_return_pct, mover_type, avg_sentiment_score
        FROM main.gold.top_movers ORDER BY return_rank LIMIT {limit * 2}
    """)
    return {
        "gainers": [r for r in rows if r.get("mover_type") == "GAINER"][:limit],
        "losers" : [r for r in rows if r.get("mover_type") == "LOSER"][:limit]
    }

def search_news(query: str, num_results: int = 3) -> list:
    try:
        from databricks.ai_search.client import VectorSearchClient
        vsc     = VectorSearchClient(disable_notice=True)
        idx     = vsc.get_index("stock-assistant-vs", "main.silver.news_for_search_index")
        results = idx.similarity_search(
            query_text  = query,
            columns     = ["ticker", "title", "description", "sentiment", "published_utc"],
            num_results = num_results
        )
        hits = results.get("result", {}).get("data_array", [])
        cols = ["ticker", "title", "description", "sentiment", "published_utc"]
        return [dict(zip(cols, h)) for h in hits]
    except Exception as e:
        rows = run_sql(f"""
            SELECT ticker, title, description, sentiment, published_utc
            FROM main.silver.news_articles
            WHERE LOWER(title) LIKE '%{query.lower()}%'
            LIMIT {num_results}
        """)
        return rows if rows else [{"note": f"Fallback used: {str(e)[:80]}"}]

def add_to_watchlist(ticker: str, watchlist_name: str = "My Watchlist") -> dict:
    run_sql(f"""
        INSERT INTO main.agent.watchlists (id, user_email, watchlist, ticker, added_at)
        VALUES ('{uuid.uuid4()}', '{USER_EMAIL}', '{watchlist_name}',
                '{ticker.upper()}', '{datetime.now().isoformat()}')
    """)
    return {"status": "success", "message": f"{ticker.upper()} added to '{watchlist_name}'"}

def save_research_note(ticker: str, note: str) -> dict:
    run_sql(f"""
        INSERT INTO main.agent.research_notes (id, user_email, ticker, note, created_at)
        VALUES ('{uuid.uuid4()}', '{USER_EMAIL}', '{ticker.upper()}',
                '{note.replace("'", "''")}', '{datetime.now().isoformat()}')
    """)
    return {"status": "success", "message": f"Note saved for {ticker.upper()}"}

def save_analysis_report(ticker: str, report_text: str) -> dict:
    run_sql(f"""
        INSERT INTO main.agent.analysis_reports
            (id, user_email, ticker, report_text, agent_model, generated_at)
        VALUES ('{uuid.uuid4()}', '{USER_EMAIL}', '{ticker.upper()}',
                '{report_text.replace("'", "''")}',
                '{MODEL}', '{datetime.now().isoformat()}')
    """)
    return {"status": "success", "message": f"Report saved for {ticker.upper()}"}

# ------------------------------------------------------------------
# 3. Tool schemas (OpenAI format)
# ------------------------------------------------------------------
TOOLS = [
    {"type":"function","function":{"name":"get_price_data","description":"Get current price data and metrics for a stock ticker","parameters":{"type":"object","properties":{"ticker":{"type":"string"}},"required":["ticker"]}}},
    {"type":"function","function":{"name":"get_sentiment","description":"Get sentiment signal (BULLISH/NEUTRAL/BEARISH) for a ticker","parameters":{"type":"object","properties":{"ticker":{"type":"string"}},"required":["ticker"]}}},
    {"type":"function","function":{"name":"compare_tickers","description":"Compare multiple tickers on price and sentiment","parameters":{"type":"object","properties":{"tickers":{"type":"array","items":{"type":"string"}}},"required":["tickers"]}}},
    {"type":"function","function":{"name":"get_top_movers","description":"Get today's top gaining and losing stocks","parameters":{"type":"object","properties":{"limit":{"type":"integer"}}}}},
    {"type":"function","function":{"name":"search_news","description":"Semantic search over recent stock news","parameters":{"type":"object","properties":{"query":{"type":"string"},"num_results":{"type":"integer"}},"required":["query"]}}},
    {"type":"function","function":{"name":"add_to_watchlist","description":"Add ticker to user watchlist","parameters":{"type":"object","properties":{"ticker":{"type":"string"},"watchlist_name":{"type":"string"}},"required":["ticker"]}}},
    {"type":"function","function":{"name":"save_research_note","description":"Save research note for a ticker","parameters":{"type":"object","properties":{"ticker":{"type":"string"},"note":{"type":"string"}},"required":["ticker","note"]}}},
    {"type":"function","function":{"name":"save_analysis_report","description":"Save analysis report for a ticker","parameters":{"type":"object","properties":{"ticker":{"type":"string"},"report_text":{"type":"string"}},"required":["ticker","report_text"]}}}
]

TOOL_MAP = {
    "get_price_data"      : get_price_data,
    "get_sentiment"       : get_sentiment,
    "compare_tickers"     : compare_tickers,
    "get_top_movers"      : get_top_movers,
    "search_news"         : search_news,
    "add_to_watchlist"    : add_to_watchlist,
    "save_research_note"  : save_research_note,
    "save_analysis_report": save_analysis_report,
}

SYSTEM_PROMPT = """You are an AI stock market research assistant.
You have access to real-time market data, news sentiment, and portfolio tools.
Always use tools to get current data before answering.
Be concise, data-driven, and offer to save notes or add to watchlist."""

# ------------------------------------------------------------------
# 4. Agent runner
# ------------------------------------------------------------------
def run_agent(user_query: str) -> tuple[str, list]:
    messages   = [{"role": "system", "content": SYSTEM_PROMPT},
                  {"role": "user",   "content": user_query}]
    tool_trace = []

    for _ in range(5):
        response = get_client().chat.completions.create(
            model       = MODEL,
            messages    = messages,
            tools       = TOOLS,
            tool_choice = "auto"
        )
        msg = response.choices[0].message

        if not msg.tool_calls:
            return msg.content or "", tool_trace

        messages.append({
            "role": "assistant", "content": msg.content,
            "tool_calls": [
                {"id": tc.id, "type": "function",
                 "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
                for tc in msg.tool_calls
            ]
        })

        for tc in msg.tool_calls:
            fn_name = tc.function.name
            fn_args = json.loads(tc.function.arguments)
            result  = TOOL_MAP[fn_name](**fn_args) if fn_name in TOOL_MAP else {"error": "Unknown tool"}
            tool_trace.append({"tool": fn_name, "args": fn_args, "result": result})
            messages.append({
                "role": "tool", "tool_call_id": tc.id,
                "content": json.dumps(result, default=str)
            })

    return "Max rounds reached", tool_trace

# ------------------------------------------------------------------
# 5. Market summary helpers
# ------------------------------------------------------------------
def load_market_summary() -> str:
    try:
        rows = run_sql("""
            SELECT t.ticker, t.name, t.close, t.daily_return_pct,
                   t.is_up_day, s.sentiment_signal
            FROM main.gold.ticker_daily_summary t
            LEFT JOIN main.gold.sentiment_summary s ON t.ticker = s.ticker
            ORDER BY t.daily_return_pct DESC
        """)
        if not rows:
            return "**📊 Market Summary**\n\nNo data yet"
        lines = ["**📊 Market Summary**\n"]
        for r in rows:
            try:
                arrow   = "🟢" if str(r.get("is_up_day","")).lower() == "true" else "🔴"
                ret     = float(r.get("daily_return_pct") or 0)
                signal  = r.get("sentiment_signal") or ""
                s_emoji = "📈" if signal == "BULLISH" else ("📉" if signal == "BEARISH" else "➡️")
                lines.append(f"{arrow} **{r['ticker']}** ${r['close']}  ({ret:+.2f}%)  {s_emoji}")
            except Exception:
                lines.append(f"• {r.get('ticker','?')}")
        return "\n".join(lines)
    except Exception as e:
        return f"**📊 Market Summary**\n\nError: {str(e)[:100]}"

def load_watchlist() -> str:
    try:
        rows = run_sql(f"""
            SELECT ticker, watchlist, added_at FROM main.agent.watchlists
            WHERE user_email = '{USER_EMAIL}'
            ORDER BY added_at DESC LIMIT 10
        """)
        if not rows:
            return "**📋 Watchlist**\n\nNo tickers yet"
        lines = ["**📋 Watchlist**\n"]
        for r in rows:
            lines.append(f"• **{r.get('ticker','?')}** — {r.get('watchlist','')}")
        return "\n".join(lines)
    except Exception as e:
        return f"**📋 Watchlist**\n\nError: {str(e)[:100]}"

# ------------------------------------------------------------------
# 6. Gradio UI
# ------------------------------------------------------------------
def chat(message: str, history: list) -> tuple[str, list, str]:
    if not message.strip():
        return "", history, ""
    response, tool_trace = run_agent(message)
    trace_md = ""
    if tool_trace:
        lines = ["**🔧 Tools called:**\n"]
        for t in tool_trace:
            args_str   = json.dumps(t["args"])
            result_str = json.dumps(t["result"], default=str)[:150]
            lines.append(f"→ `{t['tool']}({args_str})`")
            lines.append(f"← `{result_str}...`\n")
        trace_md = "\n".join(lines)
    history.append((message, response))
    return "", history, trace_md

EXAMPLES = [
    "What is Apple's current stock price and sentiment?",
    "Compare MSFT and NVDA — which has better momentum?",
    "What are today's top gainers and losers?",
    "Search for news about AI stocks and save a note about NVDA",
    "Add Apple to my Tech Watchlist",
]

with gr.Blocks(
    title = "AI Stock Research Assistant",
    theme = gr.themes.Soft(primary_hue="blue", neutral_hue="slate")
) as demo:

    gr.Markdown("# 📈 AI Stock Market Research Assistant\n*Powered by Databricks Lakehouse + Llama 3.3 70B*")

    with gr.Row():
        with gr.Column(scale=3):
            chatbot = gr.Chatbot(label="Chat", height=480, show_copy_button=True)
            with gr.Row():
                msg     = gr.Textbox(placeholder="Ask about stocks, prices, news...", show_label=False, scale=4)
                send_btn= gr.Button("Send 🚀", variant="primary", scale=1)

            gr.Markdown("**💡 Try these:**")
            with gr.Row():
                for q in EXAMPLES[:3]:
                    gr.Button(q, size="sm").click(fn=lambda x=q: x, outputs=msg)
            with gr.Row():
                for q in EXAMPLES[3:]:
                    gr.Button(q, size="sm").click(fn=lambda x=q: x, outputs=msg)

            tool_trace_box = gr.Markdown(value="")

        with gr.Column(scale=1):
            market_md = gr.Markdown("**📊 Market Summary**\n\nClick Refresh ↓")
            gr.Button("🔄 Refresh Market", size="sm").click(fn=load_market_summary, outputs=market_md)
            gr.Markdown("---")
            watchlist_md = gr.Markdown("**📋 Watchlist**\n\nClick Refresh ↓")
            gr.Button("🔄 Refresh Watchlist", size="sm").click(fn=load_watchlist, outputs=watchlist_md)

    send_btn.click(fn=chat, inputs=[msg, chatbot], outputs=[msg, chatbot, tool_trace_box])
    msg.submit(fn=chat, inputs=[msg, chatbot], outputs=[msg, chatbot, tool_trace_box])

if __name__ == "__main__":
    demo.launch(show_api=False)
