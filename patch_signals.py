import re

file_path = "api/telegram/main.py"
with open(file_path, "r") as f:
    text = f.read()

# Replace the specific block of BTN_SIGNALS
old_block = """    elif text == BTN_SIGNALS:
        from database.session import async_session
        from database.models.all_models import Signal, SignalDecisionLog
        from sqlalchemy import select, desc
        
        try:
            from datetime import datetime, timedelta
            async with async_session() as session:
                # Показываем 5 свежих сигналов за последние 6 часов
                six_hours_ago = datetime.utcnow() - timedelta(hours=6)
                query = (
                    select(Signal)
                    .where(Signal.timestamp >= six_hours_ago)
                    .order_by(Signal.timestamp.desc())
                    .limit(5)
                )
                result = await session.execute(query)
                signals = result.scalars().all()

                fallback_sdl = []
                if not signals:
                    sdl_query = (
                        select(SignalDecisionLog)
                        .where(SignalDecisionLog.created_at >= six_hours_ago)
                        .order_by(desc(SignalDecisionLog.created_at))
                        .limit(5)
                    )
                    sdl_res = await session.execute(sdl_query)
                    fallback_sdl = sdl_res.scalars().all()
                
                if not signals and not fallback_sdl:
                    await update.message.reply_text("📉 Нет свежих сигналов за последние 6 часов.")
                    return
                
                msg = "📉 **ПОСЛЕДНИЕ СИГНАЛЫ (6ч)**\\n\\n"
                
                for s in signals:
                    kind = str(s.signal_type).split('.')[-1]
                    emo = "🟩" if kind == "LONG" else "🟥"
                    msg += f"{emo} **{s.symbol}** | {s.strategy}\\n"
                    msg += f"↳ Сигнал: `{kind}` | Цена: `{s.entry_price}`\\n"
                    msg += f"↳ Win Prob: {s.win_prob*100:.1f}% | Risk: {s.risk*100:.1f}%\\n\\n"

                for sdl in fallback_sdl:
                    emo = "🟩" if sdl.direction == "LONG" else "🟥"
                    msg += f"[Shadow] {emo} **{sdl.symbol}** | {sdl.strategy}\\n"
                    msg += f"↳ ML Score: {sdl.score*100:.1f}% | Outcome: {sdl.outcome or 'Pending'}\\n\\n"

                await update.message.reply_text(msg, parse_mode='Markdown')
        except Exception as e:
            logging.error(f"Signals error: {e}")
            await update.message.reply_text("❌ Ошибка загрузки сигналов.")"""

new_block = """    elif text == BTN_SIGNALS:
        try:
            async with get_http_client() as client:
                res = await client.get(f"{ENGINE_URL}/api/v1/signals?limit=5")
                res.raise_for_status()
                data = res.json()
            
            if not data:
                await update.message.reply_text("📉 Нет свежих сигналов за последние 6 часов.")
                return
            
            msg = "📉 **ПОСЛЕДНИЕ СИГНАЛЫ (6ч)**\\n\\n"
            for s in data:
                emo = "🟩" if s["signal_type"].upper() == "LONG" else "🟥"
                src = "[Shadow] " if s["source"] == "decision_logs" else ""
                msg += f"{src}{emo} **{s['symbol']}** | {s['strategy']}\\n"
                msg += f"↳ Сигнал: `{s['signal_type']}` | Цена: `{s['entry_price']}`\\n"
                wp = s.get("win_prob") or s.get("confidence") or 0.0
                msg += f"↳ Score/WinProb: {wp*100:.1f}% | Status: {s.get('status', 'Unknown')}\\n\\n"
                
            await update.message.reply_text(msg, parse_mode='Markdown')
        except Exception as e:
            logging.error(f"Signals error: {e}")
            await update.message.reply_text("❌ Ошибка загрузки сигналов.")"""

if old_block in text:
    text = text.replace(old_block, new_block)
    with open(file_path, "w") as f:
         f.write(text)
    print("Replaced successfully")
else:
    print("Block not found!")
