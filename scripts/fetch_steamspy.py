import argparse, json, time
from datetime import date
import requests

p = argparse.ArgumentParser()
p.add_argument("--date", default=str(date.today()))
args, _ = p.parse_known_args()

MAX_ATTEMPTS = 3

all_games, page, skipped = {}, 0, []
while True:
    url = f"https://steamspy.com/api.php?request=all&page={page}"
    data = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        r = None
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            data = r.json() if r.text.strip() else {}
            break
        except Exception as e:
            status = r.status_code if r is not None else "no response"
            body = r.text[:200] if r is not None else ""
            print(f"page {page} attempt {attempt} failed: {e} | status: {status} | body: {body!r}")
            if attempt < MAX_ATTEMPTS:
                time.sleep(10 * attempt)

    if data is None:                      # page kept failing: skip it and move on
        print(f"page {page} skipped after {MAX_ATTEMPTS} attempts")
        skipped.append(page)
        page += 1
        time.sleep(2)
        continue

    if not data:                          # a truly empty page means the end
        print(f"page {page} empty, done")
        break

    all_games.update(data)
    print(f"page {page}: {len(data)} games | total: {len(all_games)}")
    page += 1
    time.sleep(2)

out = f"steamspy_{args.date}.jsonl"
with open(out, "w") as f:
    for rec in all_games.values():
        f.write(json.dumps(rec) + "\n")
print(f"saved {out} with {len(all_games)} games | skipped pages: {skipped or 'none'}")
