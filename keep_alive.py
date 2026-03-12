from flask import Flask, render_template, request, send_from_directory
import sqlite3
import os
import json
import datetime
import time
from threading import Thread
from config import KEEP_ALIVE_SECRET, DB_FILE

START_TIME = time.time()

# Disable Flask startup logs to keep console clean
import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask(__name__)

@app.route('/logo.jpg')
def serve_logo():
    """Serve the official Aura Apex branding logo."""
    return send_from_directory(os.path.dirname(os.path.abspath(__file__)), "6039744640304483712.jpg")

def get_stats():
    """Fetch real-time stats from the database."""
    stats = {
        "total_prospects": 0,
        "contacted": 0,
        "not_contacted": 0,
        "conversion_rate": "0%",
        "joined_groups": 0
    }
    try:
        if not os.path.exists(DB_FILE):
            return stats
            
        con = sqlite3.connect(DB_FILE)
        con.execute("PRAGMA journal_mode=WAL;")
        cur = con.cursor()
        
        # Check if tables exist before querying
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='prospects'")
        if cur.fetchone():
            cur.execute("SELECT COUNT(*) FROM prospects")
            stats["total_prospects"] = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM prospects WHERE status='contacted'")
            stats["contacted"] = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM prospects WHERE status='not_contacted'")
            stats["not_contacted"] = cur.fetchone()[0]
            
            if stats["total_prospects"] > 0:
                rate = (stats["contacted"] / stats["total_prospects"]) * 100
                stats["conversion_rate"] = f"{rate:.1f}%"
        
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='joined_groups'")
        if cur.fetchone():
            cur.execute("SELECT COUNT(*) FROM joined_groups")
            stats["joined_groups"] = cur.fetchone()[0]
            
        con.close()
    except Exception as e:
        print(f"Stats error: {e}")
    return stats

def get_activity_trends():
    """Fetch last 7 days of activity for charts."""
    trends = {"days": [], "prospects": [], "contacted": [], "responded": []}
    try:
        if not os.path.exists(DB_FILE):
            return trends
        con = sqlite3.connect(DB_FILE)
        con.execute("PRAGMA journal_mode=WAL;")
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='activity_rollup'")
        if cur.fetchone():
            cur.execute("SELECT day, prospects_total, contacted, responded FROM activity_rollup ORDER BY day DESC LIMIT 7")
            rows = cur.fetchall()[::-1] # Reverse to chronological
            for r in rows:
                trends["days"].append(r[0])
                trends["prospects"].append(r[1])
                trends["contacted"].append(r[2])
                trends["responded"].append(r[3])
        con.close()
    except Exception as e:
        print(f"Trends error: {e}")
    return trends

def get_recent_prospects():
    """Fetch 10 most recent prospects with niche analysis."""
    prospects = []
    try:
        if not os.path.exists(DB_FILE):
            return prospects
        con = sqlite3.connect(DB_FILE)
        con.execute("PRAGMA journal_mode=WAL;")
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='prospects'")
        if cur.fetchone():
            cur.execute("SELECT username, group_title, status, message_ts, message FROM prospects ORDER BY message_ts DESC LIMIT 10")
            for r in cur.fetchall():
                msg = (r[4] or "").lower()
                sentiment = "Neutral"
                niche = "General"
                
                # Intent Detection
                if any(k in niche for k in ["help", "setup", "fix", "buffering", "issue", "problem"]):
                    sentiment = "High Intent"
                elif any(k in msg for k in ["how", "where", "link", "buy", "price", "cost"]):
                    sentiment = "Buyer Signal"
                
                # Niche Categorization
                if any(k in msg for k in ["sports", "football", "soccer", "cricket", "nfl", "nba"]):
                    niche = "Sports"
                elif any(k in msg for k in ["movie", "cinema", "netflix", "series", "hbo"]):
                    niche = "Cinema"
                elif any(k in msg for k in ["uk", "usa", "canada", "india", "germany", "france"]):
                    niche = "Regional"
                elif any(k in msg for k in ["trial", "test", "demo", "free"]):
                    niche = "Trial"
                
                prospects.append({
                    "username": r[0] or "Anonymous",
                    "group": r[1] or "Unknown",
                    "status": r[2],
                    "time": r[3],
                    "sentiment": sentiment,
                    "niche": niche
                })
        con.close()
    except Exception as e:
        print(f"Prospects error: {e}")
    return prospects

def get_recent_logs():
    """Fetch recent activity from the join_attempts table."""
    logs = []
    try:
        if not os.path.exists(DB_FILE):
            return logs
            
        con = sqlite3.connect(DB_FILE)
        con.execute("PRAGMA journal_mode=WAL;")
        cur = con.cursor()
        
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='join_attempts'")
        if cur.fetchone():
            # Get last 15 attempts
            cur.execute("SELECT id, title, status, ts FROM join_attempts ORDER BY ts DESC LIMIT 15")
            for row in cur.fetchall():
                dt = datetime.datetime.fromtimestamp(row[3]).strftime('%H:%M:%S')
                logs.append({
                    "id": row[0],
                    "title": row[1],
                    "status": row[2],
                    "time": dt
                })
        con.close()
    except Exception as e:
        print(f"Logs error: {e}")
    return logs

def get_source_kpis():
    """Fetch KPI performance per search term."""
    kpis = {}
    try:
        if not os.path.exists(DB_FILE):
            return kpis
            
        con = sqlite3.connect(DB_FILE)
        con.execute("PRAGMA journal_mode=WAL;")
        cur = con.cursor()
        
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='source_kpis'")
        if cur.fetchone():
            cur.execute("SELECT term, attempts, successes FROM source_kpis ORDER BY successes DESC")
            for row in cur.fetchall():
                term, attempts, successes = row
                rate = (successes / attempts * 100) if attempts > 0 else 0
                kpis[term] = {
                    "attempts": attempts,
                    "successes": successes,
                    "success_rate": round(rate, 1)
                }
        con.close()
    except Exception as e:
        print(f"KPI error: {e}")
    return kpis

def get_potential_targets():
    """Fetch potential targets discovered by scouters."""
    targets = []
    try:
        if not os.path.exists(DB_FILE):
            return targets
            
        con = sqlite3.connect(DB_FILE)
        con.execute("PRAGMA journal_mode=WAL;")
        cur = con.cursor()
        
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='potential_targets'")
        if cur.fetchone():
            # Get last 10 targets
            cur.execute("SELECT link, title, members, discovered_at FROM potential_targets ORDER BY discovered_at DESC LIMIT 10")
            for row in cur.fetchall():
                targets.append({
                    "link": row[0],
                    "title": row[1],
                    "members": row[2],
                    "time": row[3]
                })
        con.close()
    except Exception as e:
        print(f"Targets error: {e}")
    return targets

def get_uptime():
    """Calculate uptime string."""
    diff = int(time.time() - START_TIME)
    hours, remainder = divmod(diff, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m {seconds}s"

def get_top_groups():
    """Fetch top 5 source groups for prospects."""
    groups = []
    try:
        if os.path.exists(DB_FILE):
            con = sqlite3.connect(DB_FILE)
            con.execute("PRAGMA journal_mode=WAL;")
            cur = con.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='prospects'")
            if cur.fetchone():
                cur.execute("SELECT group_title, COUNT(*) as count FROM prospects GROUP BY group_title ORDER BY count DESC LIMIT 5")
                for r in cur.fetchall():
                    groups.append({"title": r[0] or "Unknown", "count": r[1]})
            con.close()
    except Exception:
        pass
    return groups

def get_persona_stats():
    """Analyze which personas are converting best."""
    stats = []
    try:
        if os.path.exists(DB_FILE):
            con = sqlite3.connect(DB_FILE)
            con.execute("PRAGMA journal_mode=WAL;")
            cur = con.cursor()
            cur.execute("""
                SELECT persona_id, COUNT(*) as total, 
                SUM(CASE WHEN status='contacted' THEN 1 ELSE 0 END) as contacted,
                SUM(CASE WHEN responses_count > 0 THEN 1 ELSE 0 END) as responses
                FROM prospects GROUP BY persona_id ORDER BY total DESC
            """)
            for r in cur.fetchall():
                p_id = r[0] or "Standard"
                total = r[1]
                contacted = r[2]
                responses = r[3]
                rate = (responses / contacted * 100) if contacted > 0 else 0
                stats.append({
                    "id": p_id,
                    "total": total,
                    "contacted": contacted,
                    "responses": responses,
                    "rate": f"{rate:.1f}%"
                })
            con.close()
    except Exception:
        pass
    return stats

def get_outreach_queue():
    """Fetch next 5 prospects scheduled for outreach."""
    queue = []
    try:
        if os.path.exists(DB_FILE):
            con = sqlite3.connect(DB_FILE)
            con.execute("PRAGMA journal_mode=WAL;")
            cur = con.cursor()
            cur.execute("""
                SELECT username, group_title, message_ts 
                FROM prospects 
                WHERE status='not_contacted' 
                ORDER BY message_ts ASC LIMIT 5
            """)
            for r in cur.fetchall():
                queue.append({
                    "username": r[0] or "Anonymous",
                    "group": r[1] or "Unknown",
                    "time": r[2]
                })
            con.close()
    except Exception:
        pass
    return queue

def get_niche_distribution():
    """Calculate lead distribution across niches."""
    dist = {}
    try:
        if os.path.exists(DB_FILE):
            con = sqlite3.connect(DB_FILE)
            con.execute("PRAGMA journal_mode=WAL;")
            cur = con.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='prospects'")
            if cur.fetchone():
                cur.execute("SELECT message FROM prospects")
                for r in cur.fetchall():
                    msg = (r[0] or "").lower()
                    niche = "General"
                    if any(k in msg for k in ["sports", "football", "soccer", "cricket", "nfl", "nba"]): niche = "Sports"
                    elif any(k in msg for k in ["movie", "cinema", "netflix", "series", "hbo"]): niche = "Cinema"
                    elif any(k in msg for k in ["uk", "usa", "canada", "india", "germany", "france"]): niche = "Regional"
                    elif any(k in msg for k in ["trial", "test", "demo", "free"]): niche = "Trial"
                    dist[niche] = dist.get(niche, 0) + 1
            con.close()
    except Exception:
        pass
    return [{"name": k, "value": v} for k, v in dist.items()]

def get_activity_heatmap():
    """Calculate hourly activity density."""
    heatmap = [0] * 24
    try:
        if os.path.exists(DB_FILE):
            con = sqlite3.connect(DB_FILE)
            con.execute("PRAGMA journal_mode=WAL;")
            cur = con.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='prospects'")
            if cur.fetchone():
                # Extract hour from message_ts (format: "2026-03-11 18:08:23")
                cur.execute("SELECT message_ts FROM prospects")
                for r in cur.fetchall():
                    try:
                        ts = r[0]
                        hour = int(ts.split(" ")[1].split(":")[0])
                        heatmap[hour] += 1
                    except: continue
            con.close()
    except Exception:
        pass
    return heatmap

def get_lead_velocity():
    """Calculate lead capture velocity (leads per hour last 24h)."""
    try:
        if os.path.exists(DB_FILE):
            con = sqlite3.connect(DB_FILE)
            con.execute("PRAGMA journal_mode=WAL;")
            cur = con.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='prospects'")
            if cur.fetchone():
                cur.execute("""
                    SELECT COUNT(*) FROM prospects 
                    WHERE message_ts >= datetime('now', '-24 hours')
                """)
                count = cur.fetchone()[0]
                return round(count / 24, 1)
            con.close()
    except Exception:
        pass
    return 0.0

@app.route('/')
def home():
    stats = get_stats()
    recent_logs = get_recent_logs()
    source_kpis = get_source_kpis()
    potential_targets = get_potential_targets()
    recent_prospects = get_recent_prospects()
    trends = get_activity_trends()
    top_groups = get_top_groups()
    persona_stats = get_persona_stats()
    outreach_queue = get_outreach_queue()
    niche_dist = get_niche_distribution()
    activity_map = get_activity_heatmap()
    lead_velocity = get_lead_velocity()
    uptime = get_uptime()
    return render_template('index.html', 
                         stats=stats, 
                         recent_logs=recent_logs, 
                         source_kpis=source_kpis, 
                         potential_targets=potential_targets, 
                         recent_prospects=recent_prospects,
                         trends=trends,
                         top_groups=top_groups,
                         persona_stats=persona_stats,
                         outreach_queue=outreach_queue,
                         niche_dist=niche_dist,
                         activity_map=activity_map,
                         lead_velocity=lead_velocity,
                         uptime=uptime)

@app.route('/api/settings')
def get_settings():
    """Fetch global bot settings."""
    settings_file = "aura_settings.json"
    if os.path.exists(settings_file):
        try:
            with open(settings_file, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return json.dumps({})

@app.route('/api/settings/update', methods=['POST'])
def update_settings():
    """Update global bot settings."""
    settings_file = "aura_settings.json"
    try:
        data = request.get_json() or {}
        current_settings = {}
        if os.path.exists(settings_file):
            with open(settings_file, "r") as f:
                current_settings = json.load(f)
        
        current_settings.update(data)
        
        with open(settings_file, "w") as f:
            json.dump(current_settings, f, indent=4)
            
        return json.dumps({"status": "ok", "message": "Settings synced to Aura Core."})
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)}), 500

@app.route('/api/prospects/bulk', methods=['POST'])
def bulk_prospect_action():
    """Perform bulk actions on prospects (delete or update status)."""
    try:
        data = request.get_json() or {}
        user_ids = data.get('user_ids', [])
        action = data.get('action') # 'delete', 'contacted', 'not_contacted', 'blacklisted'
        
        if not user_ids or not action:
            return json.dumps({"status": "error", "message": "Missing IDs or action."}), 400
            
        if os.path.exists(DB_FILE):
            con = sqlite3.connect(DB_FILE)
            con.execute("PRAGMA journal_mode=WAL;")
            cur = con.cursor()
            
            # Convert user_ids to string for the IN clause
            placeholders = ','.join(['?'] * len(user_ids))
            
            if action == 'delete':
                cur.execute(f"DELETE FROM prospects WHERE user_id IN ({placeholders}) OR username IN ({placeholders})", user_ids + user_ids)
            else:
                cur.execute(f"UPDATE prospects SET status = ? WHERE user_id IN ({placeholders}) OR username IN ({placeholders})", [action] + user_ids + user_ids)
                
            con.commit()
            con.close()
            return json.dumps({"status": "ok", "message": f"Bulk {action} completed for {len(user_ids)} leads."})
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)}), 500
    return json.dumps({"status": "error", "message": "DB error."}), 500

@app.route('/api/prospect/update', methods=['POST'])
def update_prospect_status():
    """Update a prospect's status or opt-out state."""
    try:
        data = request.get_json() or {}
        user_id = data.get('user_id')
        new_status = data.get('status')
        opt_out = data.get('opt_out')
        
        if not user_id:
            return json.dumps({"status": "error", "message": "Missing user ID."}), 400
            
        if os.path.exists(DB_FILE):
            con = sqlite3.connect(DB_FILE)
            con.execute("PRAGMA journal_mode=WAL;")
            cur = con.cursor()
            
            if new_status:
                cur.execute("UPDATE prospects SET status = ? WHERE id = ? OR user_id = ?", (new_status, user_id, user_id))
            if opt_out is not None:
                cur.execute("UPDATE prospects SET opt_out = ? WHERE id = ? OR user_id = ?", (1 if opt_out else 0, user_id, user_id))
                
            con.commit()
            con.close()
            return json.dumps({"status": "ok", "message": "Prospect Updated Successfully."})
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)}), 500
    return json.dumps({"status": "error", "message": "DB not found."}), 404

@app.route('/api/intel')
def get_intel():
    """Fetch current niche intelligence rules."""
    from config import RULES_PATH
    if os.path.exists(RULES_PATH):
        try:
            with open(RULES_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return json.dumps({})

@app.route('/api/intel/update', methods=['POST'])
def update_intel():
    """Update specific rule categories in rules.json."""
    from config import RULES_PATH
    try:
        data = request.get_json() or {}
        category = data.get('category')
        action = data.get('action') # 'add' or 'remove'
        value = data.get('value')
        
        if not category or not action or not value:
            return json.dumps({"status": "error", "message": "Missing parameters."}), 400
            
        current_rules = {}
        if os.path.exists(RULES_PATH):
            with open(RULES_PATH, "r", encoding="utf-8") as f:
                current_rules = json.load(f)
        
        if category not in current_rules:
            current_rules[category] = []
            
        if action == 'add' and value not in current_rules[category]:
            current_rules[category].append(value)
        elif action == 'remove' and value in current_rules[category]:
            current_rules[category].remove(value)
            
        with open(RULES_PATH, "w", encoding="utf-8") as f:
            json.dump(current_rules, f, indent=4)
            
        return json.dumps({"status": "ok", "message": f"Rule {action}ed successfully."})
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)}), 500

@app.route('/api/blacklist')
def get_blacklist():
    """Fetch current blacklist IDs."""
    from config import BLACKLIST_FILE
    blacklist = []
    if os.path.exists(BLACKLIST_FILE):
        try:
            with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
                blacklist = [line.strip() for line in f.readlines() if line.strip()]
        except Exception:
            pass
    return json.dumps(blacklist)

@app.route('/api/blacklist/remove', methods=['POST'])
def remove_from_blacklist():
    """Remove an ID from the blacklist file."""
    from config import BLACKLIST_FILE
    try:
        data = request.get_json() or {}
        user_id = data.get('user_id')
        if not user_id:
            return json.dumps({"status": "error", "message": "Missing ID."}), 400
            
        if os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
                lines = f.readlines()
            with open(BLACKLIST_FILE, "w", encoding="utf-8") as f:
                for line in lines:
                    if line.strip() != str(user_id):
                        f.write(line)
            return json.dumps({"status": "ok", "message": "ID removed from blacklist."})
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)}), 500
    return json.dumps({"status": "error", "message": "Blacklist file not found."}), 404

@app.route('/api/system/logs')
def get_bot_logs():
    """Tail the last 50 lines of bot.log."""
    log_file = "bot.log"
    lines = []
    try:
        if os.path.exists(log_file):
            with open(log_file, "r", encoding="utf-8", errors="replace") as f:
                # Get last 50 lines
                lines = f.readlines()[-50:]
        return json.dumps({"logs": [line.strip() for line in lines]})
    except Exception as e:
        return json.dumps({"error": str(e)}), 500

@app.route('/api/system/logs/clear', methods=['POST'])
def clear_bot_logs():
    """Clear the bot.log file."""
    log_file = "bot.log"
    try:
        if os.path.exists(log_file):
            with open(log_file, "w", encoding="utf-8") as f:
                f.write(f"--- Log Cleared at {datetime.datetime.now()} ---\n")
        return json.dumps({"status": "ok", "message": "Logs cleared."})
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)}), 500

@app.route('/api/prospect/<int:user_id>')
def get_prospect_details(user_id):
    """Fetch full history for a specific prospect."""
    details = {}
    try:
        if os.path.exists(DB_FILE):
            con = sqlite3.connect(DB_FILE)
            con.execute("PRAGMA journal_mode=WAL;")
            cur = con.cursor()
            cur.execute("""
                SELECT username, group_title, status, message_ts, message, persona_id, responses_count, source
                FROM prospects WHERE id = ? OR user_id = ?
            """, (user_id, user_id))
            r = cur.fetchone()
            if r:
                details = {
                    "username": r[0] or "Anonymous",
                    "group": r[1] or "Unknown",
                    "status": r[2],
                    "time": r[3],
                    "message": r[4],
                    "persona": r[5] or "Default",
                    "responses": r[6],
                    "source": r[7] or "Scouter"
                }
            con.close()
    except Exception as e:
        print(f"Detail error: {e}")
    return json.dumps(details)

@app.route('/api/export/prospects')
def export_prospects():
    """Export prospects to CSV format."""
    import io
    import csv
    from flask import make_response
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['ID', 'Username', 'Group', 'Status', 'Message', 'Timestamp', 'Source'])
    
    try:
        if os.path.exists(DB_FILE):
            con = sqlite3.connect(DB_FILE)
            con.execute("PRAGMA journal_mode=WAL;")
            cur = con.cursor()
            cur.execute("SELECT user_id, username, group_title, status, message, message_ts, source FROM prospects")
            writer.writerows(cur.fetchall())
            con.close()
    except Exception:
        pass
        
    response = make_response(output.getvalue())
    response.headers["Content-Disposition"] = f"attachment; filename=aura_prospects_{int(time.time())}.csv"
    response.headers["Content-type"] = "text/csv"
    return response

@app.route('/api/keywords')
def get_keywords():
    """Fetch current tracking keywords."""
    keywords = []
    try:
        if os.path.exists(DB_FILE):
            con = sqlite3.connect(DB_FILE)
            con.execute("PRAGMA journal_mode=WAL;")
            cur = con.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='keywords'")
            if cur.fetchone():
                cur.execute("SELECT term, weight, hits FROM keywords ORDER BY hits DESC")
                for r in cur.fetchall():
                    keywords.append({"term": r[0], "weight": r[1], "hits": r[2]})
            con.close()
    except Exception:
        pass
    return json.dumps(keywords)

@app.route('/search')
def search():
    query = request.args.get('q', '').strip().lower()
    results = []
    try:
        if os.path.exists(DB_FILE):
            con = sqlite3.connect(DB_FILE)
            con.execute("PRAGMA journal_mode=WAL;")
            cur = con.cursor()
            if query:
                cur.execute("""
                    SELECT username, group_title, status, message_ts, message 
                    FROM prospects 
                    WHERE username LIKE ? OR group_title LIKE ? OR message LIKE ?
                    ORDER BY message_ts DESC LIMIT 50
                """, (f'%{query}%', f'%{query}%', f'%{query}%'))
            else:
                cur.execute("""
                    SELECT username, group_title, status, message_ts, message 
                    FROM prospects 
                    ORDER BY message_ts DESC LIMIT 50
                """)
            for r in cur.fetchall():
                results.append({
                    "username": r[0] or "Anonymous",
                    "group": r[1] or "Unknown",
                    "status": r[2],
                    "time": r[3],
                    "message": r[4]
                })
            con.close()
    except Exception as e:
        print(f"Search error: {e}")
    return json.dumps(results)

@app.route('/system/action', methods=['POST'])
def system_action():
    try:
        data = request.get_json() or {}
        action = data.get('action')
        
        if action == 'restart':
            # Create a restart signal for main.py (if we implement that)
            # or simply exit this process, and main.py will restart us.
            # For a full system restart, we'd need to kill the manager.
            # But let's just trigger a local script exit for now.
            def trigger_exit():
                time.sleep(1)
                os._exit(0)
            Thread(target=trigger_exit).start()
            return json.dumps({"status": "ok", "message": "Aura Core Restarting..."})
            
        elif action == 'clear_cache':
            if os.path.exists(DB_FILE):
                con = sqlite3.connect(DB_FILE)
                con.execute("PRAGMA journal_mode=WAL;")
                cur = con.cursor()
                # Just clear the entity cache table as an example
                cur.execute("DELETE FROM entity_cache")
                con.commit()
                con.close()
                return json.dumps({"status": "ok", "message": "Cache Flushed Successfully."})
                
        elif action == 'clear_db_logs':
            if os.path.exists(DB_FILE):
                con = sqlite3.connect(DB_FILE)
                con.execute("PRAGMA journal_mode=WAL;")
                cur = con.cursor()
                # Clear join_attempts table
                cur.execute("DELETE FROM join_attempts")
                con.commit()
                con.close()
                return json.dumps({"status": "ok", "message": "Log stream cleared."})
        
        return json.dumps({"status": "error", "message": "Unknown action."}), 400
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)}), 500

@app.route('/api/logs')
def api_logs():
    return json.dumps(get_recent_logs())

@app.route('/health')
def health():
    return "OK", 200

@app.route('/code', methods=['POST'])
def set_code():
    try:
        api_key = request.headers.get("X-API-Key") or request.args.get("api_key")
        if api_key != KEEP_ALIVE_SECRET:
            return ("Unauthorized", 401)
        
        code = None
        if request.is_json:
            data = request.get_json(silent=True) or {}
            code = (data.get("code") or "").strip()
        else:
            code = (request.form.get("code") or "").strip()
        if not code:
            return ("Missing code", 400)
        if len(code) > 100:
            return ("Code too long", 400)
        path = os.path.join(os.getcwd(), "WAITING_FOR_CODE")
        with open(path, "w", encoding="utf-8") as f:
            f.write(code)
        return ("OK", 200)
    except Exception as e:
        return (f"Error: {e}", 500)

def run():
    try:
        # Use PORT env var if available, default to 5002
        port = int(os.environ.get("PORT", 5002))
        print(f"Starting web server on port {port}...")
        app.run(host='0.0.0.0', port=port, threaded=True)
    except Exception as e:
        print(f"Web server failed to start: {e}")

def keep_alive():
    t = Thread(target=run)
    t.daemon = True
    t.start()

if __name__ == "__main__":
    run()
