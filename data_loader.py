import json


def process_json_line(post):
    try:
        doc = post.get("doc", {})
        created_at = doc.get("createdAt", None)
        sentiment = doc.get("sentiment", 0)
        account = doc.get("account", {})
        username = account.get("username", "unknown")
        account_id = account.get("id", "unknown")

        if not created_at or len(created_at) < 13:
            return None, 0, username, account_id

        hour = created_at[0:13]  # "2025-01-30T11"

        if not isinstance(sentiment, (int, float)):
            sentiment = 0

        return hour, sentiment, username, account_id

    except Exception:
        return None, 0, "unknown", "unknown"


def process_ndjson_file(filepath, rank, size):
    hour_scores = {}
    user_scores = {}
    with open(filepath, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i % size != rank:
                continue

            try:
                post = json.loads(line)
                hour, sentiment, username, account_id = process_json_line(post)

                if hour:
                    if hour in hour_scores:
                        hour_scores[hour] += sentiment
                    else:
                        hour_scores[hour] = sentiment

                if username and username != "unknown" and account_id != "unknown":
                    user_key = (username, account_id)
                    if user_key in user_scores:
                        user_scores[user_key] += sentiment
                    else:
                        user_scores[user_key] = sentiment

            except Exception:
                continue

        return hour_scores, user_scores

