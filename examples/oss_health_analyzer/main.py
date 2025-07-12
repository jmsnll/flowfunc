import requests
import time
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any
import statistics

# Configure logging to silence third-party loggers
logging.getLogger("urllib3").setLevel(logging.ERROR)


def fetch_repo_details(repo_slug: str) -> Dict[str, Any]:
    """
    Fetches core metadata for a repository.
    
    Args:
        repo_slug: Repository slug (e.g., "torvalds/linux")
        
    Returns:
        Dictionary with repository details or error information
    """
    url = f"https://api.github.com/repos/{repo_slug}"
    
    try:
        response = requests.get(url, timeout=10)
        
        if response.status_code == 404:
            return {
                "error": "Repo not found",
                "repo_slug": repo_slug,
                "name": repo_slug,
                "stargazers_count": 0,
                "forks_count": 0,
                "open_issues_count": 0,
                "description": "Repository not found"
            }
        
        response.raise_for_status()
        data = response.json()
        
        return {
            "name": data.get("name", repo_slug),
            "stargazers_count": data.get("stargazers_count", 0),
            "forks_count": data.get("forks_count", 0),
            "open_issues_count": data.get("open_issues_count", 0),
            "description": data.get("description", ""),
            "repo_slug": repo_slug
        }
        
    except requests.RequestException as e:
        return {
            "error": f"Request failed: {str(e)}",
            "repo_slug": repo_slug,
            "name": repo_slug,
            "stargazers_count": 0,
            "forks_count": 0,
            "open_issues_count": 0,
            "description": "Request failed"
        }


def fetch_commit_activity(repo_slug: str) -> List[Dict[str, Any]]:
    """
    Fetches the last 25 commits to gauge recent activity.
    
    Args:
        repo_slug: Repository slug (e.g., "torvalds/linux")
        
    Returns:
        List of commit dictionaries with sha and author date
    """
    url = f"https://api.github.com/repos/{repo_slug}/commits"
    params = {"per_page": 25}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 404:
            return []
        
        response.raise_for_status()
        commits = response.json()
        
        return [
            {
                "sha": commit["sha"],
                "date": commit["commit"]["author"]["date"]
            }
            for commit in commits
        ]
        
    except requests.RequestException:
        return []


def fetch_open_issues(repo_slug: str) -> List[Dict[str, Any]]:
    """
    Fetches the last 15 open, non-pull-request issues.
    
    Args:
        repo_slug: Repository slug (e.g., "torvalds/linux")
        
    Returns:
        List of issue dictionaries with title, user, and created_at
    """
    url = f"https://api.github.com/repos/{repo_slug}/issues"
    params = {"per_page": 15, "state": "open"}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 404:
            return []
        
        response.raise_for_status()
        issues = response.json()
        
        # Filter out pull requests
        return [
            {
                "title": issue["title"],
                "user": issue["user"]["login"],
                "created_at": issue["created_at"]
            }
            for issue in issues
            if "pull_request" not in issue
        ]
        
    except requests.RequestException:
        return []


def analyze_commit_cadence(commits: List[Dict[str, Any]]) -> float:
    """
    Calculates the average time in days between the fetched commits.
    
    Args:
        commits: List of commit dictionaries from fetch_commit_activity
        
    Returns:
        Average time between commits in days, or 999.0 if insufficient data
    """
    if len(commits) < 2:
        return 999.0
    
    try:
        # Parse dates and sort them
        dates = []
        for commit in commits:
            date_str = commit["date"]
            # Parse ISO format date string
            date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            dates.append(date_obj)
        
        # Sort dates in descending order (most recent first)
        dates.sort(reverse=True)
        
        # Calculate time differences between consecutive commits
        time_diffs = []
        for i in range(len(dates) - 1):
            diff = dates[i] - dates[i + 1]
            time_diffs.append(diff.total_seconds() / (24 * 3600))  # Convert to days
        
        return statistics.mean(time_diffs) if time_diffs else 999.0
        
    except (ValueError, KeyError):
        return 999.0


def analyze_issue_staleness(issues: List[Dict[str, Any]]) -> float:
    """
    Calculates the average age of open issues in days.
    
    Args:
        issues: List of issue dictionaries from fetch_open_issues
        
    Returns:
        Average age of open issues in days, or 0.0 if no issues
    """
    if not issues:
        return 0.0
    
    try:
        now = datetime.now(timezone.utc)
        ages = []
        
        for issue in issues:
            created_str = issue["created_at"]
            created_date = datetime.fromisoformat(created_str.replace('Z', '+00:00'))
            
            # Calculate age in days
            age = (now - created_date).total_seconds() / (24 * 3600)
            ages.append(age)
        
        return statistics.mean(ages) if ages else 0.0
        
    except (ValueError, KeyError):
        return 0.0


def generate_health_score(
    details: Dict[str, Any], 
    commit_cadence: float, 
    issue_staleness: float
) -> Dict[str, Any]:
    """
    Combines all metrics into a single "health score" and summary for one repo.
    
    Args:
        details: Repository details from fetch_repo_details
        commit_cadence: Average commit cadence in days
        issue_staleness: Average issue age in days
        
    Returns:
        Dictionary containing health score and all raw metrics
    """
    # Handle errors from fetch_repo_details
    if "error" in details:
        return {
            "repo_name": details["repo_slug"],
            "health_score": 0.0,
            "error": details["error"],
            "stars": 0,
            "forks": 0,
            "avg_commit_cadence_days": commit_cadence,
            "avg_issue_staleness_days": issue_staleness,
            "open_issues_count": 0
        }
    
    # Calculate health score
    # Base score from popularity metrics
    score = (details["stargazers_count"] / 1000) + (details["forks_count"] / 500)
    
    # Penalize for slow commit cadence (lower cadence is better)
    score -= commit_cadence
    
    # Penalize for stale issues (lower staleness is better)
    score -= (issue_staleness / 10)
    
    # Ensure score is non-negative
    score = max(0.0, score)
    
    return {
        "repo_name": details["name"],
        "health_score": round(score, 2),
        "stars": details["stargazers_count"],
        "forks": details["forks_count"],
        "avg_commit_cadence_days": round(commit_cadence, 1),
        "avg_issue_staleness_days": round(issue_staleness, 1),
        "open_issues_count": details["open_issues_count"],
        "description": details["description"]
    }


def create_markdown_report(all_scores: List[Dict[str, Any]]) -> str:
    """
    Aggregates all individual health scores into a final, human-readable Markdown report.
    
    Args:
        all_scores: List of score dictionaries from generate_health_score
        
    Returns:
        Markdown string containing the complete report
    """
    # Separate successful and failed repositories
    successful_repos = [repo for repo in all_scores if "error" not in repo]
    failed_repos = [repo for repo in all_scores if "error" in repo]
    
    # Sort successful repositories by health score (descending)
    successful_repos.sort(key=lambda x: x["health_score"], reverse=True)
    
    # Generate report
    report_lines = [
        "# OSS Health Analysis Report",
        "",
        f"**Analysis Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"**Total Repositories Analyzed:** {len(all_scores)}",
        f"**Successful:** {len(successful_repos)}",
        f"**Failed:** {len(failed_repos)}",
        "",
        "## Repository Health Rankings",
        "",
        "| Rank | Repository | Health Score | Stars | Forks | Avg Commit Cadence (Days) | Avg Issue Age (Days) | Open Issues |",
        "|------|------------|--------------|-------|-------|---------------------------|---------------------|-------------|"
    ]
    
    # Add successful repositories to table
    for i, repo in enumerate(successful_repos, 1):
        report_lines.append(
            f"| {i} | {repo['repo_name']} | {repo['health_score']} | "
            f"{repo['stars']:,} | {repo['forks']:,} | {repo['avg_commit_cadence_days']} | "
            f"{repo['avg_issue_staleness_days']} | {repo['open_issues_count']} |"
        )
    
    # Add summary statistics
    if successful_repos:
        avg_score = statistics.mean([repo["health_score"] for repo in successful_repos])
        total_stars = sum([repo["stars"] for repo in successful_repos])
        total_forks = sum([repo["forks"] for repo in successful_repos])
        
        report_lines.extend([
            "",
            "## Summary Statistics",
            "",
            f"- **Average Health Score:** {avg_score:.2f}",
            f"- **Total Stars:** {total_stars:,}",
            f"- **Total Forks:** {total_forks:,}",
            f"- **Most Active Repository:** {successful_repos[0]['repo_name']} (Score: {successful_repos[0]['health_score']})",
            ""
        ])
    
    # Add failed repositories section
    if failed_repos:
        report_lines.extend([
            "## Failed Repositories",
            "",
            "The following repositories could not be analyzed:",
            ""
        ])
        
        for repo in failed_repos:
            report_lines.append(f"- **{repo['repo_name']}**: {repo['error']}")
    
    return "\n".join(report_lines) 


def to_list(x):
    """Converts a numpy array or any iterable to a list."""
    if hasattr(x, 'tolist'):
        return x.tolist()
    return list(x) 