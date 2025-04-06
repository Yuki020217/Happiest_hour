import json

def process_json_line(post):
  try:
    doc = post.get("doc", {})
    created_at = doc.get("createdAt", None)
    sentiment = doc.get("sentiment", 0)
    account = doc.get("account", {})
    username = account.get("username", {"unknown"})

    if not created_at or len(created_at) < 13:
      return None, None, 0, username

    hour = created_at[0:13] # "2025-01-30T11"
    day = created_at[0:10] # "2025-01-30"

    if not isinstance(sentiment, (int, float)):
      return hour, day, sentiment,username
    else:
      return hour, day, 0, username

  except Exception:
    return None, None, 0, "unknown"

def process_ndjson_file(filepath, rank, size):
  hour_scores = {}
  user_scores = {}
  with open(filepath, 'r', encoding=utf-8) as f:
    for i, line in enumerate(f):
      if i % size != rank:
        continue

      try:
        post = json.loads(line)
        hour, day, sentiment, username = process_json_line(post)

        if hour:
          if hour in hour_scores:
            hour_scores[hour] += sentiment
          else:
            hour_scores[hour] = sentiment

        if username and username != "unknown":
          if username in user_scores:
            user_scores[username] += sentiment
          else:
            user_scores[username] = sentiment

      except:
        continue

    return hour_scores

