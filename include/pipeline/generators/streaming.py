"""Streaming: users (JSON), content (CSV), playback_events (JSONL)."""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta

from pipeline.generators.base import DomainGenerator, Scenario, time_window, utc_ts

COUNTRIES = ("US", "MX", "CO", "ES", "BR", "GB", "IN")
DEVICES = ("tv", "mobile", "web", "tablet")
DURATIONS = {"movie": (4800, 9000), "episode": (1200, 3600), "track": (120, 420), "podcast": (1200, 5400)}
TITLES = ("Midnight", "Ocean", "Signal", "Harvest", "Echo", "Summit", "Atlas", "Ember")


class StreamingGenerator(DomainGenerator):
    domain = "streaming"
    generic_targets = {
        "missing_column": ("playback_events", "device"),
        "missing_file": "users",
        "malformed_record": "users",
        "empty_file": "content",
    }

    def generate_clean(self, day: date, rows: int, rng: random.Random):
        users = []
        for i in range(1, max(10, rows // 8) + 1):
            users.append({
                "user_id": f"USR-{i:05d}",
                "country": rng.choice(COUNTRIES),
                "plan": rng.choices(["free", "basic", "premium"], [40, 35, 25])[0],
                "signup_date": day - timedelta(days=rng.randint(1, 1500)),
            })

        content = []
        for i in range(1, 31):
            content_type = list(DURATIONS)[i % len(DURATIONS)]
            content.append({
                "content_id": f"CNT-{i:04d}",
                "title": f"{rng.choice(TITLES)} {i}",
                "content_type": content_type,
                "duration_seconds": rng.randint(*DURATIONS[content_type]),
                "release_date": day - timedelta(days=rng.randint(1, 3000)),
            })

        events = []
        _, window_end = time_window(day)
        while len(events) < rows:
            events.extend(self._session(rng, day, rng.choice(users), rng.choice(content), len(events)))
        events = [e for e in events if e["event_ts"] <= window_end]  # never emit future events
        for e in events:
            e["event_ts"] = utc_ts(e["event_ts"])
        return {"users": users, "content": content, "playback_events": events}

    @staticmethod
    def _session(rng, day, user, item, offset, device=None, watch_all=None) -> list[dict]:
        """start -> (pause/resume)* -> stop|complete, with positions and watch time that add up."""
        duration = item["duration_seconds"]
        if device is None:
            device = rng.choice(DEVICES[1:] if user["plan"] == "free" else DEVICES)
        completes = rng.random() < 0.5 if watch_all is None else watch_all
        target = duration if completes else rng.randint(1, max(1, int(duration * 0.85)))
        position = 0
        events = [("start", 0)]
        while position < target:
            step = min(target - position, rng.randint(60, max(60, duration // 2)))
            position += step
            if position < target:
                events += [("pause", step), ("resume", 0)]
        events.append(("complete" if completes else "stop", target - sum(w for _, w in events)))

        # Offsets first, then place the whole session inside the day so it never crosses midnight.
        offsets, elapsed = [], 0
        for event_type, watched in events:
            elapsed += watched + (rng.randint(5, 600) if event_type == "resume" else 0)
            offsets.append(elapsed)
        window_start, window_end = time_window(day)
        room = int((window_end - window_start).total_seconds()) - elapsed
        start: datetime = window_start + timedelta(seconds=rng.randint(0, max(0, room)))

        out, position = [], 0
        for n, ((event_type, watched), offset_s) in enumerate(zip(events, offsets)):
            position += watched
            ts = start + timedelta(seconds=offset_s)
            out.append({
                "event_id": f"EVT-{day:%Y%m%d}-{offset + n + 1:07d}",
                "user_id": user["user_id"],
                "content_id": item["content_id"],
                "event_ts": ts,
                "event_type": event_type,
                "position_seconds": position,
                "watch_seconds": watched,
                "device": device,
            })
        return out

    def domain_scenarios(self) -> list[Scenario]:
        def pick(b, rng, k=3, where=lambda e: True):
            return rng.sample([e for e in b.rows["playback_events"] if where(e)], k)

        def durations(b):
            return {c["content_id"]: c["duration_seconds"] for c in b.rows["content"]}

        def duplicate_events(b, rng):
            for e in pick(b, rng):
                b.rows["playback_events"].append({**e, "device": "web" if e["device"] != "web" else "mobile"})

        def null_users(b, rng):
            for e in pick(b, rng, 2):
                e["user_id"] = None

        def malformed_watch(b, rng):
            for e, bad in zip(pick(b, rng, 2), ["12s", "1e400x"]):
                e["watch_seconds"] = bad

        def invalid_device(b, rng):
            for e in pick(b, rng, 2):
                e["device"] = "smart_fridge"

        def unknown_content(b, rng):
            for e in pick(b, rng, 2):
                e["content_id"] = "CNT-9999"

        def position_overflow(b, rng):
            d = durations(b)
            for e in pick(b, rng, 2, lambda e: e["event_type"] == "stop"):
                e["position_seconds"] = d[e["content_id"]] + 600

        def negative_watch(b, rng):
            for e in pick(b, rng, 2, lambda e: e["watch_seconds"] > 0):
                e["watch_seconds"] = -e["watch_seconds"]

        def premature_completion(b, rng):
            d = durations(b)
            for e in pick(b, rng, 1, lambda e: e["event_type"] == "stop" and e["position_seconds"] < 0.5 * d[e["content_id"]]):
                e["event_type"] = "complete"

        def free_on_tv(b, rng):
            free = {u["user_id"] for u in b.rows["users"] if u["plan"] == "free"}
            if not free:
                b.rows["users"][0]["plan"] = "free"
                free = {b.rows["users"][0]["user_id"]}
            for e in pick(b, rng, 2, lambda e: e["user_id"] in free):
                e["device"] = "tv"

        def before_signup(b, rng):
            for u in b.rows["users"]:
                if any(e["user_id"] == u["user_id"] for e in b.rows["playback_events"]):
                    u["signup_date"] = b.day + timedelta(days=1)
                    break

        def binge(b, rng):
            user = {**b.rows["users"][0], "plan": "premium"}
            b.rows["users"][0] = user
            movies = [c for c in b.rows["content"] if c["content_type"] == "movie"]
            for k in range(40):  # ~40 movies in one day > 24 hours of watch time
                b.rows["playback_events"].extend(
                    {**e, "event_id": f"{e['event_id']}-B{k:02d}", "event_ts": utc_ts(e["event_ts"])}
                    for e in self._session(rng, b.day, user, rng.choice(movies), 0, device="web", watch_all=True)
                )

        return [
            Scenario("duplicate_event_ids", "same event_id delivered twice with different devices",
                     "silver", "streaming_event_id_unique", duplicate_events),
            Scenario("null_user_ids", "playback events without a user_id",
                     "bronze", "streaming_bronze_event_user_present", null_users),
            Scenario("malformed_watch_seconds", "watch_seconds like '12s' that are not integers",
                     "bronze", "streaming_bronze_event_watch_seconds_is_integer", malformed_watch),
            Scenario("invalid_device", "device type outside the supported list",
                     "silver", "streaming_event_device_valid", invalid_device),
            Scenario("unknown_content", "events for content not in the catalog",
                     "silver", "streaming_event_content_exists", unknown_content),
            Scenario("position_exceeds_duration", "playback position beyond the end of the content",
                     "silver", "streaming_event_position_within_duration", position_overflow),
            Scenario("negative_watch_time", "negative watch_seconds",
                     "silver", "streaming_event_watch_seconds_valid", negative_watch),
            Scenario("premature_completion", "'complete' event before 90% of the content was played",
                     "silver", "streaming_completion_requires_90_percent", premature_completion),
            Scenario("free_user_on_tv", "free-plan users streaming on TV (premium-only entitlement)",
                     "silver", "streaming_free_plan_not_on_tv", free_on_tv),
            Scenario("event_before_signup", "user plays content before signing up",
                     "silver", "streaming_event_after_signup", before_signup),
            Scenario("impossible_watch_time", "one user watches 40 full movies in a day: rows valid, day impossible",
                     "gold", "streaming_user_daily_watch_time_possible", binge),
        ]
