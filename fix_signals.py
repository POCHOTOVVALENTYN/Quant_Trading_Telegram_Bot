import re

file_path = "api/telegram/main.py"
with open(file_path, "r") as f:
    text = f.read()

# We need to find the entire block of `elif text == BTN_SIGNALS:` until `elif text == BTN_FAQ:` or whatever the next elif is.
# Let's find the indices.

start_idx = text.find("elif text == BTN_SIGNALS:")
end_idx = text.find("elif text == BTN_FAQ:", start_idx)
if end_idx == -1:
    end_idx = text.find("elif text == BTN_HOME:", start_idx)
if end_idx == -1:
    end_idx = text.find("else:", start_idx)

if start_idx != -1 and end_idx != -1:
    new_block = """elif text == BTN_SIGNALS:
        try:
            async with get_http_client() as client:
                resp = await client.get(f"{ENGINE_URL}/api/v1/signals?limit=5", timeout=5.0)
                resp.raise_for_status()
                signals = resp.json()

            if not signals:
                await update.message.reply_text("📭 Актуальных сигналов за последние 6 часов нет.")
                return

            await update.message.reply_text(f"📉 **ПОСЛЕДНИЕ СИГНАЛЫ ({len(signals)}):**", parse_mode='Markdown')

            status_map = {
                "PENDING": "⌛️ В ОЖИДАНИИ",
                "EXECUTED": "✅ В ПОЗИЦИИ",
                "FAILED": "❌ ОШИБКА",
                "REJECTED": "🛑 ОТКЛОНЕН",
                "EXPIRED": "⏱ ИСТЕК"
            }

            def fmt_p(val):
                if val is None: return "N/A"
                v = float(val)
                return f"{v:.4f}" if v < 1.0 else f"{v:.2f}"

            for s in signals:
                sig_side = str(s.get("signal_type", "")).upper()
                signal_type_ru = "🟢 LONG" if sig_side == "LONG" else "🔴 SHORT"
                status_raw = s.get("status", "UNKNOWN")
                status_ru = status_map.get(status_raw, f"❓ {status_raw}")
                
                win_p = s.get("win_prob", 0.0) or 0.0
                conf = s.get("confidence", 0.0) or 0.0
                ts = str(s.get("timestamp", "N/A"))[:19].replace("T", " ")

                msg = (
                    f"🚀 **СИГНАЛ: {s.get('strategy', 'Unknown')}**\\n\\n"
                    f"🔸 **Символ:** {s.get('symbol', 'N/A')}\\n"
                    f"🔸 **Направление:** {signal_type_ru}\\n\\n"
                    f"💰 **Цена входа:** {fmt_p(s.get('entry_price'))}\\n"
                    f"🛡 **Stop Loss:** {fmt_p(s.get('stop_loss', 0))}\\n"
                    f"🎯 **Take Profit:** {fmt_p(s.get('take_profit', 0))}\\n\\n"
                    f"🕒 **Время (UTC):** {ts}\\n\\n"
                    f"🤖 **AI ВЕРДИКТ:**\\n"
                    f"📈 **Вероятность успеха:** {int(win_p * 100)}%\\n"
                    f"💰 **Ож. доходность:** {s.get('expected_return', '0.0')}%\\n"
                    f"⚠️ **Уровень риска:** {s.get('risk', '1.0')}\\n"
                    f"📊 **AI Score:** {conf:.2f}\\n\\n"
                    f"ℹ️ **Статус:** {status_ru}\\n"
                    f"📁 **Источник:** {s.get('source', 'unknown')}\\n"
                    f"───────────────────"
                )
                await update.message.reply_text(msg, parse_mode='Markdown')
        except Exception as e:
            logging.error(f"Ошибка получения сигналов из REST API: {e}")
            await update.message.reply_text("❌ Ошибка при обращении к торговому движку.")
    """
    
    text = text[:start_idx] + new_block + text[end_idx:]
    with open(file_path, "w") as f:
        f.write(text)
    print("Fixed via python script!")
else:
    print(f"Could not find block boundaries: start={start_idx}, end={end_idx}")

