import argparse, json, time
from datetime import date
import requests

p = argparse.ArgumentParser()
p.add_argument("--date", default=str(date.today()))
args, _ = p.parse_known_args()

all_games, page = {}, 0
while True:
    url = f"https://steamspy.com/api.php?request=all&page={page}"
    data = None
    for attempt in range(4):
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            data = r.json() if r.text.strip() else {}
            break
        except Exception as e:
            if attempt == 3:
                raise SystemExit(f"page {page} failed after retries: {e}")
            time.sleep(10 * (attempt + 1))
    if not data:
        print(f"page {page} empty, done")
        break
    all_games.update(data)
    print(f"page {page} done - total: {len(all_games)}")
    if len(data) < 1000:
        print("last page reached")
        break
    page += 1
    time.sleep(2)

out = f"steamspy_{args.date}.jsonl"
with open(out, "w") as f:
    for rec in all_games.values():
        f.write(json.dumps(rec) + "\n")
print(f"saved {out} with {len(all_games)} games")
