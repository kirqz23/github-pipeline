# This script loops from commits and create history of changes for a file

import base64
import logging
import os
import time
from json.decoder import JSONDecodeError
from typing import Dict

from dotenv import load_dotenv
from github import Github, GithubObject
from github.GitBlob import GitBlob
from github.GithubException import (GithubException,
                                    RateLimitExceededException,
                                    UnknownObjectException)
# from github.PaginatedList import PaginatedList
from github.Repository import Repository

logger = logging.getLogger(__name__)

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_FILE = os.getenv("GITHUB_FILE")
GITHUB_REPO = os.getenv("GITHUB_REPO")
DEFAULT_RETRY_COUNT = 100
DEFAULT_RETRY_SEC = 5


def _get_repo(repo_name: str) -> Repository:
    """Get github repository"""
    retry_count = 0
    while retry_count < DEFAULT_RETRY_COUNT:
        try:
            g = Github(GITHUB_TOKEN)
            repo = g.get_repo(repo_name)
            return repo
        except RateLimitExceededException:
            retry_count += 1
            wait_time = retry_count * DEFAULT_RETRY_SEC
            logger.info(
                f"""
                RateLimitExceededException: {retry_count},
                wait time (sec): {wait_time}
                """
            )
            time.sleep(wait_time)
    raise Exception("Retries count exceeded")


def _get_file_content(
    repo: Repository, file_path: str, ref: str = GithubObject.NotSet
) -> Dict:
    """Get github file content (if GithubException is raised
    fetch blob's content for files 1-100 MB)"""
    retry_count = 0
    while retry_count < DEFAULT_RETRY_COUNT:
        try:
            try:
                try:
                    contents = repo.get_contents(file_path, ref)
                    file_content = contents.decoded_content.decode()
                    return file_content
                except (UnknownObjectException, JSONDecodeError) as e:
                    logger.info(f"Cannot read file contents due to: {e}")
                    return None
            except (GithubException, AssertionError) as e:
                logger.info(f"Exception/AssertionError: {e}")
                try:
                    contents = _get_blob_content(repo, ref, file_path)
                    file_content = base64.b64decode(contents.content)
                    return file_content
                except (UnknownObjectException, JSONDecodeError) as e:
                    logger.info(f"Cannot read file contents due to: {e}")
                    return None
        except RateLimitExceededException:
            retry_count += 1
            wait_time = retry_count * DEFAULT_RETRY_SEC
            logger.info(
                f"""
                RateLimitExceededException: {retry_count},
                wait time (sec): {wait_time}
                """
            )
            time.sleep(wait_time)
    raise Exception("Retries count exceeded")


def _get_blob_content(repo, branch, path_name) -> GitBlob:
    """Find blob's sha and fetch its content"""
    ref = repo.get_git_ref(f"heads/{branch}")
    tree = repo.get_git_tree(ref.object.sha, recursive="/" in path_name).tree
    sha = [x.sha for x in tree if x.path == path_name]
    if not sha:
        logger.info(f"Cannot find sha for: {path_name} in branch {branch}")
        return None
    return repo.get_git_blob(sha[0])


repo = _get_repo(GITHUB_REPO)
commits = repo.get_commits(
    path=GITHUB_FILE, sha="1bacd894237084a8343fbc4952c161435937ee96"
)  # since last sha INCLUDED

# returns from the most recent
for commit in commits:
    print(commit)
    print(commit.commit.author.date)
    print(_get_file_content(repo, GITHUB_FILE, commit.sha))
