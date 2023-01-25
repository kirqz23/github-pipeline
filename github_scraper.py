# This script loops from commits and create history of changes for a file

import base64
import hashlib
import json
import logging
import os
import time
from dataclasses import asdict, dataclass
from json.decoder import JSONDecodeError
from typing import Dict, List

import pandas as pd
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
MAIN_BRANCH = "master"


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
                    file_content = json.loads(contents.decoded_content.decode())
                    return file_content
                except (UnknownObjectException, JSONDecodeError) as e:
                    logger.info(f"Cannot read file contents due to: {e}")
                    print(f"Cannot read file contents due to: {e}")
                    return None
            except (GithubException, AssertionError) as e:
                logger.info(f"Exception/AssertionError: {e}")
                try:
                    contents = _get_blob_content(repo, ref, file_path)
                    file_content = json.loads(base64.b64decode(contents.content))
                    return file_content
                except (UnknownObjectException, JSONDecodeError) as e:
                    logger.info(f"Cannot read file contents due to: {e}")
                    print(f"Cannot read blob contents due to: {e}")
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
            print(
                f"""
                RateLimitExceededException: {retry_count},
                wait time (sec): {wait_time}
                """
            )
            time.sleep(wait_time)
    raise Exception("Retries count exceeded")

def _get_blob_content(repo, commit, path_name) -> GitBlob:
    """Find blob's sha and fetch its content"""
    tree = repo.get_git_tree(commit, recursive="/" in path_name).tree
    sha = [x.sha for x in tree if x.path == path_name]
    if not sha:
        logger.info(f"Cannot find sha for: {path_name} for {commit}")
        return None
    return repo.get_git_blob(sha[0])


# OCR ver 1
@dataclass
class Contract:
    address: str
    name: str
    github_name: str
    status: str
    contract_version: str
    max_gas_price_per_gwei: int
    micro_link_per_eth: int
    observation_payment_link_gwei: int
    reasonable_gas_price_gwei: int
    transmission_payment_link_gwei: int
    bad_epoch_timeout: str
    max_contract_value_age: str
    max_faulty_node_count: int
    max_round_count: int
    observation_grace_period: str
    relative_deviation_threshold_ppb: int
    resend_interval: str
    round_interval: str
    transmission_stage_timeout: str
    transmission_stages: str
    decimals: int
    marketing_category: str
    marketing_history: str
    marketing_path: str
    min_submission_value: str
    max_submission_value: str
    deviation_threshold: str
    heartbeat: str
    minimum_answers: int
    payment: str
    is_deleted: int
    source: str
    pull_request: int


def _process_contracts(
    file_content: Dict, source: str, pull_request=None
) -> List[Dict]:
    """Retrieve contracts data from rdd directory file in json format"""
    contracts = file_content.get("contracts")
    result = []
    if contracts:
        for address, config in contracts.items():
            contract = Contract(
                address=address,
                name=config.get("name").replace(" / ", "-"),
                github_name=config.get("name"),
                status=config.get("status"),
                contract_version=config.get("contractVersion"),
                max_gas_price_per_gwei=config.get("billing", {}).get("maxGasPriceGwei"),
                micro_link_per_eth=config.get("billing", {}).get("microLinkPerEth"),
                observation_payment_link_gwei=config.get("billing", {}).get(
                    "observationPaymentLinkGwei"
                ),
                reasonable_gas_price_gwei=config.get("billing", {}).get(
                    "reasonableGasPriceGwei"
                ),
                transmission_payment_link_gwei=config.get("billing", {}).get(
                    "transmissionPaymentLinkGwei"
                ),
                bad_epoch_timeout=config.get("config", {}).get("badEpochTimeout"),
                max_contract_value_age=config.get("config", {}).get(
                    "maxContractValueAge"
                ),
                max_faulty_node_count=config.get("config", {}).get(
                    "maxFaultyNodeCount"
                ),
                max_round_count=config.get("config", {}).get("maxRoundCount"),
                observation_grace_period=config.get("config", {}).get(
                    "observationGracePeriod"
                ),
                relative_deviation_threshold_ppb=config.get("config", {}).get(
                    "relativeDeviationThresholdPPB"
                ),
                resend_interval=config.get("config", {}).get("resendInterval"),
                round_interval=config.get("config", {}).get("roundInterval"),
                transmission_stage_timeout=config.get("config", {}).get(
                    "transmissionStageTimeout"
                ),
                transmission_stages=config.get("config", {}).get("transmissionStages"),
                decimals=config.get("decimals"),
                marketing_category=config.get("marketing", {}).get("category"),
                marketing_history=config.get("marketing", {}).get("history"),
                marketing_path=config.get("marketing", {}).get("path"),
                min_submission_value=config.get("minSubmissionValue"),
                max_submission_value=config.get("maxSubmissionValue"),
                deviation_threshold=config.get("deviationThreshold"),
                heartbeat=config.get("heartbeat"),
                minimum_answers=config.get("minimumAnswers"),
                payment=config.get("payment"),
                is_deleted=0,
                source=source,
                pull_request=pull_request,
            )
            result.append(asdict(contract))
    return result


repo = _get_repo(GITHUB_REPO)
commits = repo.get_commits(
    path=GITHUB_FILE,  # sha="1bacd894237084a8343fbc4952c161435937ee96"
)  # since last sha INCLUDED
# returns from the most recent
commit_list = []
for commit in commits:
    commit_list.append(commit)
commit_list.reverse()
print(len(commit_list))
result = []
for commit in commit_list:
    print(f"Processing commit: {commit.sha}")
    # commit = repo.get_git_ref(f"heads/{ref}").object.sha
    file_content = _get_file_content(repo, GITHUB_FILE, commit.sha)
    # print(file_content)
    data = _process_contracts(file_content, commit.sha)
    added_contracts = []
    for contract in data:
        del contract["pull_request"]
        del contract["is_deleted"]
        del contract["source"]
        contract["md5"] = hashlib.md5(json.dumps(contract).encode("utf-8")).hexdigest()
        contract["commit"] = commit.sha
        contract["commit_date"] = commit.commit.author.date
        added_contracts.append(contract)
    result.extend(added_contracts)
df = pd.DataFrame(result)
df["next_md5"] = (
    df.sort_values(by=["commit_date"], ascending=True)
    .groupby(["address"])["md5"]
    .shift(-1)
)
df = df[df.md5 != df.next_md5]
df.drop(columns=["next_md5"], inplace=True)
