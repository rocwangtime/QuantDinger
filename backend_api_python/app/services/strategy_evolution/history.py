"""Durable conservative selection counts and holdout exposure, user-scoped."""

from app.utils.db import get_db_transaction


class ResearchHistory:
    def __init__(self, *, user_id, source_id, study_id):
        self.user_id, self.source_id, self.study_id = int(user_id), int(source_id), str(study_id)

    def reserve(self, *, code_hash, trials):
        with get_db_transaction() as db:
            cur = db.cursor()
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                        (f"research:{self.user_id}:{self.source_id}",))
            cur.execute("SELECT study_id FROM qd_research_studies WHERE study_id=%s AND user_id=%s",
                        (self.study_id, self.user_id))
            exists = cur.fetchone()
            cur.execute("SELECT COALESCE(SUM(reserved_trials),0) AS trials FROM qd_research_studies "
                        "WHERE user_id=%s AND source_id=%s AND study_id<>%s",
                        (self.user_id, self.source_id, self.study_id))
            previous = int((cur.fetchone() or {}).get("trials") or 0)
            if not exists:
                cur.execute("INSERT INTO qd_research_studies(study_id,user_id,source_id,code_hash,reserved_trials) "
                            "VALUES (%s,%s,%s,%s,%s) RETURNING study_id",
                            (self.study_id, self.user_id, self.source_id, code_hash, int(trials)))
            cur.close()
        return previous

    def expose_holdout(self, start, end):
        from .bundles import content_hash
        key = content_hash([start.isoformat(), end.isoformat()])
        with get_db_transaction() as db:
            cur = db.cursor()
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                        (f"research:{self.user_id}:{self.source_id}",))
            cur.execute("SELECT COUNT(*) AS exposures FROM qd_research_studies WHERE user_id=%s "
                        "AND source_id=%s AND holdout_start<=%s AND holdout_end>=%s AND holdout_exposed",
                        (self.user_id, self.source_id, end, start))
            previous = int((cur.fetchone() or {}).get("exposures") or 0)
            cur.execute("UPDATE qd_research_studies SET holdout_key=%s,holdout_start=%s,holdout_end=%s,holdout_exposed=TRUE "
                        "WHERE study_id=%s AND user_id=%s", (key, start, end, self.study_id, self.user_id))
            cur.close()
        return previous

    def attach_bundle(self, identity):
        with get_db_transaction() as db:
            cur = db.cursor()
            cur.execute("UPDATE qd_research_studies SET bundle_id=%s WHERE study_id=%s AND user_id=%s",
                        (identity, self.study_id, self.user_id))
            cur.close()
