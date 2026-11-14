import argparse, json, time
from datetime import date
import requests

p = argparse.ArgumentParser()
p.add_argument("--date", default=str(date.today()))
args, _ = p.parse_known_args()

MAX_ATTEMPTS = 3        # retries for "Too many connections"
MAX_SKIPS_IN_A_ROW = 3  # this many unreadable pages in a row = past the end

all_games, page, skipped, skips_in_a_row = {}, 0, [], 0
while True:
    url = f"https://steamspy.com/api.php?request=all&page={page}"
    data, end_reached = None, False

    for attempt in range(1, MAX_ATTEMPTS + 1):
        r = None
        try:
            r = requests.get(url, timeout=60)
        except Exception as e:
            print(f"page {page} attempt {attempt}: request failed: {e}")
            time.sleep(10 * attempt)
            continue

        body = r.text.strip()
        if r.status_code != 200:                       # 500 etc. = past the end
            print(f"page {page}: HTTP {r.status_code}, treating as the end")
            end_reached = True
            break
        if "Too many connections" in body:             # SteamSpy busy: wait and retry
            print(f"page {page} attempt {attempt}: too many connections")
            time.sleep(10 * attempt)
            continue
        try:
            data = json.loads(body) if body else {}
            break
        except ValueError:
            print(f"page {page}: unexpected response: {body[:200]!r}, treating as the end")
            end_reached = True
            break

    if end_reached:
        break

    if data is None:                                   # kept saying "too many connections"
        skipped.append(page)
        skips_in_a_row += 1
        print(f"page {page} skipped ({skips_in_a_row} in a row)")
        if skips_in_a_row >= MAX_SKIPS_IN_A_ROW:
            print("several pages in a row unreadable, treating as the end")
            break
        page += 1
        time.sleep(2)
        continue

    skips_in_a_row = 0
    if not data:
        print(f"page {page} empty, done")
        break

    before = len(all_games)
    all_games.update(data)
    added = len(all_games) - before
    print(f"page {page}: {len(data)} games, {added} new | total: {len(all_games)}")
    if added == 0:
        print("page added no new games, done")
        break
    page += 1
    time.sleep(2)

# pages skipped at the very end were past the last real page, not missing data
while skipped and skipped[-1] >= page - MAX_SKIPS_IN_A_ROW:
    skipped.pop()

out = f"steamspy_{args.date}.jsonl"
with open(out, "w") as f:
    for rec in all_games.values():
        f.write(json.dumps(rec) + "\n")
print(f"saved {out} with {len(all_games)} games | pages skipped mid-run: {skipped or 'none'}")
