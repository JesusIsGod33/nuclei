#!/usr/bin/env python3
"""
Repository Discovery & Multi-Clone Engine
Manages cloning, syncing, and branch resolution across multiple repositories.
"""

import asyncio
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse


@dataclass
class RepositoryConfig:
    """Repository configuration structure."""
    name: str
    url: str
    local_path: str
    branch: str = "main"
    sync_strategy: str = "merge"  # merge, rebase, or force
    auth_method: str = "ssh"  # ssh or https


class RepositoryResolver:
    """Autonomous repository discovery and management engine."""
    
    def __init__(self, manifest_path: str, base_workspace: str = None):
        """
        Initialize resolver with manifest file.
        
        Args:
            manifest_path: Path to JSON manifest defining target repositories
            base_workspace: Base path for all repository clones (defaults to ~/ecosystem)
        """
        self.manifest_path = Path(manifest_path)
        self.base_workspace = Path(base_workspace or "~/ecosystem").expanduser()
        self.repos: List[RepositoryConfig] = []
        self.log_path = self.base_workspace / "knowledge_base" / "logs"
        self.log_path.mkdir(parents=True, exist_ok=True)
        self.operation_log = self._create_log_file()
        
    def _create_log_file(self) -> Path:
        """Create timestamped log file for this session."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.log_path / f"resolver_{timestamp}.md"
        
        with open(log_file, "w") as f:
            f.write(f"# Repository Resolution Session\n")
            f.write(f"**Started:** {datetime.now().isoformat()}\n\n")
            f.write(f"**Manifest:** {self.manifest_path}\n")
            f.write(f"**Workspace:** {self.base_workspace}\n\n")
            f.write("---\n\n")
        
        return log_file
    
    def _log(self, message: str, level: str = "INFO"):
        """Append to operation log."""
        timestamp = datetime.now().isoformat()
        with open(self.operation_log, "a") as f:
            f.write(f"[{timestamp}] **{level}:** {message}\n")
        print(f"[{level}] {message}")
    
    def load_manifest(self) -> Dict:
        """Load and parse repository manifest JSON."""
        if not self.manifest_path.exists():
            self._log(f"Manifest not found: {self.manifest_path}", "ERROR")
            raise FileNotFoundError(f"Manifest not found: {self.manifest_path}")
        
        try:
            with open(self.manifest_path, "r") as f:
                manifest = json.load(f)
            self._log(f"Manifest loaded successfully")
            return manifest
        except json.JSONDecodeError as e:
            self._log(f"Failed to parse manifest JSON: {e}", "ERROR")
            raise
    
    def parse_repositories(self, manifest: Dict) -> List[RepositoryConfig]:
        """Parse repository definitions from manifest."""
        repos = []
        
        for repo_def in manifest.get("repositories", []):
            config = RepositoryConfig(
                name=repo_def["name"],
                url=repo_def["url"],
                local_path=str(self.base_workspace / repo_def.get("local_path", repo_def["name"])),
                branch=repo_def.get("branch", "main"),
                sync_strategy=repo_def.get("sync_strategy", "merge"),
                auth_method=repo_def.get("auth_method", "ssh")
            )
            repos.append(config)
            self._log(f"Parsed repository: {config.name} → {config.local_path}")
        
        self.repos = repos
        return repos
    
    def check_local_existence(self, repo: RepositoryConfig) -> bool:
        """Check if repository exists locally."""
        repo_path = Path(repo.local_path)
        exists = repo_path.exists() and (repo_path / ".git").exists()
        
        if exists:
            self._log(f"Repository '{repo.name}' found locally at {repo.local_path}")
        else:
            self._log(f"Repository '{repo.name}' NOT found locally at {repo.local_path}")
        
        return exists
    
    async def clone_repository(self, repo: RepositoryConfig) -> Tuple[bool, str]:
        """
        Clone repository asynchronously.
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        repo_path = Path(repo.local_path)
        repo_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            self._log(f"Cloning {repo.name} from {repo.url}...")
            
            result = await asyncio.create_subprocess_exec(
                "git", "clone", repo.url, repo.local_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await result.communicate()
            
            if result.returncode == 0:
                self._log(f"Successfully cloned {repo.name}")
                return True, stdout.decode()
            else:
                error_msg = stderr.decode()
                self._log(f"Failed to clone {repo.name}: {error_msg}", "ERROR")
                return False, error_msg
                
        except Exception as e:
            self._log(f"Exception during clone of {repo.name}: {e}", "ERROR")
            return False, str(e)
    
    async def sync_repository(self, repo: RepositoryConfig) -> Tuple[bool, str]:
        """
        Sync repository using configured strategy.
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        repo_path = Path(repo.local_path)
        
        if not repo_path.exists():
            return False, f"Repository path does not exist: {repo_path}"
        
        try:
            os.chdir(repo_path)
            
            # Fetch latest changes
            self._log(f"Fetching latest changes for {repo.name}...")
            result = await asyncio.create_subprocess_exec(
                "git", "fetch", "--all",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await result.communicate()
            
            if result.returncode != 0:
                return False, f"Fetch failed: {stderr.decode()}"
            
            # Apply sync strategy
            if repo.sync_strategy == "merge":
                return await self._merge_strategy(repo)
            elif repo.sync_strategy == "rebase":
                return await self._rebase_strategy(repo)
            elif repo.sync_strategy == "force":
                return await self._force_strategy(repo)
            else:
                return False, f"Unknown sync strategy: {repo.sync_strategy}"
                
        except Exception as e:
            self._log(f"Exception during sync of {repo.name}: {e}", "ERROR")
            return False, str(e)
    
    async def _merge_strategy(self, repo: RepositoryConfig) -> Tuple[bool, str]:
        """Apply merge sync strategy."""
        self._log(f"Applying merge strategy to {repo.name}...")
        
        result = await asyncio.create_subprocess_exec(
            "git", "merge", f"origin/{repo.branch}", "--allow-unrelated-histories",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await result.communicate()
        
        if result.returncode == 0:
            self._log(f"Merge successful for {repo.name}")
            return True, stdout.decode()
        else:
            error_msg = stderr.decode()
            self._log(f"Merge conflict in {repo.name}: {error_msg}", "WARN")
            return False, error_msg
    
    async def _rebase_strategy(self, repo: RepositoryConfig) -> Tuple[bool, str]:
        """Apply rebase sync strategy."""
        self._log(f"Applying rebase strategy to {repo.name}...")
        
        result = await asyncio.create_subprocess_exec(
            "git", "rebase", f"origin/{repo.branch}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await result.communicate()
        
        if result.returncode == 0:
            self._log(f"Rebase successful for {repo.name}")
            return True, stdout.decode()
        else:
            error_msg = stderr.decode()
            self._log(f"Rebase conflict in {repo.name}: {error_msg}", "WARN")
            return False, error_msg
    
    async def _force_strategy(self, repo: RepositoryConfig) -> Tuple[bool, str]:
        """Apply force sync strategy (reset to remote)."""
        self._log(f"Applying force strategy to {repo.name}...")
        
        result = await asyncio.create_subprocess_exec(
            "git", "reset", "--hard", f"origin/{repo.branch}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await result.communicate()
        
        if result.returncode == 0:
            self._log(f"Force reset successful for {repo.name}")
            return True, stdout.decode()
        else:
            return False, stderr.decode()
    
    async def resolve_all(self, sync: bool = True) -> Dict[str, bool]:
        """
        Resolve all repositories: check local existence, clone if missing, sync if requested.
        
        Args:
            sync: Whether to sync existing repositories
            
        Returns:
            Dict mapping repository names to success status
        """
        self._log(f"Starting resolution of {len(self.repos)} repositories...")
        
        results = {}
        clone_tasks = []
        
        # Phase 1: Check local existence and queue clones
        for repo in self.repos:
            exists = self.check_local_existence(repo)
            
            if not exists:
                clone_tasks.append(self.clone_repository(repo))
            else:
                results[repo.name] = True
        
        # Phase 2: Execute all clones concurrently
        if clone_tasks:
            self._log(f"Cloning {len(clone_tasks)} missing repositories concurrently...")
            clone_results = await asyncio.gather(*clone_tasks)
            
            for i, (success, message) in enumerate(clone_results):
                repo_name = self.repos[len(results) + i].name
                results[repo_name] = success
                if not success:
                    self._log(f"Clone failed for {repo_name}", "ERROR")
        
        # Phase 3: Sync all repositories if requested
        if sync:
            self._log(f"Syncing all repositories...")
            sync_tasks = [self.sync_repository(repo) for repo in self.repos if results.get(repo.name, False)]
            sync_results = await asyncio.gather(*sync_tasks)
            
            for i, (success, message) in enumerate(sync_results):
                if not success:
                    self._log(f"Sync warning for repository {i}: {message}", "WARN")
        
        self._log(f"Resolution complete. Results: {results}")
        return results
    
    def generate_summary(self) -> str:
        """Generate summary report of resolver operations."""
        summary = "\n## Resolution Summary\n\n"
        summary += f"- **Total Repositories:** {len(self.repos)}\n"
        summary += f"- **Workspace:** {self.base_workspace}\n"
        summary += f"- **Log File:** {self.operation_log}\n"
        
        return summary


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Repository Discovery & Multi-Clone Engine")
    parser.add_argument("manifest", help="Path to repository manifest JSON")
    parser.add_argument("--workspace", default="~/ecosystem", help="Base workspace directory")
    parser.add_argument("--sync", action="store_true", help="Sync repositories after cloning")
    parser.add_argument("--no-clone", action="store_true", help="Skip cloning, only sync existing repos")
    
    args = parser.parse_args()
    
    resolver = RepositoryResolver(args.manifest, args.workspace)
    manifest = resolver.load_manifest()
    resolver.parse_repositories(manifest)
    
    # Run async operations
    sync_mode = not args.no_clone or args.sync
    results = asyncio.run(resolver.resolve_all(sync=sync_mode))
    
    print(resolver.generate_summary())
    
    # Exit with error if any failed
    if not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
