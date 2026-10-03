import pytest

from app.services import ai_skill_registry


def test_user_skill_path_rejects_traversal_and_symlink_escape(tmp_path, monkeypatch):
    root = tmp_path / "skills"
    root.mkdir()
    monkeypatch.setattr(ai_skill_registry, "USER_SKILLS_DIR", root)

    existing = root / "safe_skill.json"
    existing.write_text('{"id":"safe_skill"}', encoding="utf-8")
    assert ai_skill_registry._skill_path("safe_skill") == existing
    assert ai_skill_registry._skill_path("new_skill") is None
    for invalid in ("../escape", "safe/escape", "safe\\escape", "safe.json", ""):
        with pytest.raises(ValueError):
            ai_skill_registry._skill_path(invalid)

    outside = tmp_path / "outside.json"
    outside.write_text("{}", encoding="utf-8")
    (root / "linked_skill.json").symlink_to(outside)
    assert ai_skill_registry._skill_path("linked_skill") is None
