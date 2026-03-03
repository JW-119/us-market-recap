"""일일 시황 아카이브 — GitHub 저장소에 JSON으로 저장/로드."""
from __future__ import annotations

import json
import base64
from github import Github, GithubException
from config import GITHUB_TOKEN, GITHUB_REPO

ARCHIVE_DIR = "archive"


def _get_repo():
    """PyGithub 저장소 객체 반환."""
    if not GITHUB_TOKEN:
        return None
    g = Github(GITHUB_TOKEN)
    return g.get_repo(GITHUB_REPO)


def save_daily_snapshot(data: dict) -> bool:
    """오늘 시황을 archive/YYYY-MM-DD.json으로 GitHub에 커밋.

    이미 존재하면 스킵. 성공 시 True, 실패/스킵 시 False 반환.
    """
    repo = _get_repo()
    if repo is None:
        return False

    market_date = data.get("market_date")
    if not market_date:
        return False

    path = f"{ARCHIVE_DIR}/{market_date}.json"
    content = json.dumps(data, ensure_ascii=False, indent=2)

    try:
        repo.get_contents(path)
        return False  # 이미 존재
    except GithubException as e:
        if e.status != 404:
            return False

    try:
        repo.create_file(
            path=path,
            message=f"archive: {market_date} daily snapshot",
            content=content,
        )
        return True
    except GithubException:
        return False


def list_archive_dates() -> list[str]:
    """GitHub repo archive/ 디렉토리에서 날짜 목록 반환 (최신순)."""
    repo = _get_repo()
    if repo is None:
        return []

    try:
        contents = repo.get_contents(ARCHIVE_DIR)
    except GithubException:
        return []

    dates = []
    for item in contents:
        if item.name.endswith(".json"):
            dates.append(item.name.replace(".json", ""))

    dates.sort(reverse=True)
    return dates


def load_snapshot(date: str) -> dict | None:
    """특정 날짜의 JSON 스냅샷을 GitHub에서 로드."""
    repo = _get_repo()
    if repo is None:
        return None

    path = f"{ARCHIVE_DIR}/{date}.json"
    try:
        file_content = repo.get_contents(path)
        decoded = base64.b64decode(file_content.content).decode("utf-8")
        return json.loads(decoded)
    except GithubException:
        return None
