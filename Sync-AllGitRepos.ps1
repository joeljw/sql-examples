# Sync-AllGitRepos.ps1
# Recursively finds git repos under a directory and updates them from their remote.
#
# Default mode (safe):
#   - Skips repos with uncommitted changes or untracked files
#   - Only fast-forwards (fails cleanly instead of rewriting history)
# Force mode (-Force):
#   - Discards ALL local changes, hard-resets to remote, removes untracked files

[CmdletBinding()]
param (
    [Parameter(Mandatory = $false)]
    [string]$RootPath = ".",

    [Parameter(Mandatory = $false)]
    [switch]$Force,      # -f / -Force : destructive sync

    [Parameter(Mandatory = $false)]
    [Alias("f")]
    [switch]$ForceAlias,

    [Parameter(Mandatory = $false)]
    [switch]$DryRun
)

# Support -f as an alias for -Force
if ($ForceAlias) { $Force = $true }

if ($Force) {
    Write-Host "*** FORCE MODE: local changes and untracked files will be DESTROYED ***`n" -ForegroundColor Red
}

$repos = Get-ChildItem -Path $RootPath -Directory -Recurse -Force -Filter ".git" |
         Where-Object { $_.FullName -notmatch '[\\/]\.git[\\/]' } |
         ForEach-Object { $_.Parent.FullName }

if ($repos.Count -eq 0) {
    Write-Host "No git repositories found under '$RootPath'" -ForegroundColor Yellow
    exit
}

Write-Host "Found $($repos.Count) repositor$(if ($repos.Count -eq 1) {'y'} else {'ies'})`n" -ForegroundColor Cyan

$results = @()

foreach ($repo in $repos) {
    Write-Host "==> $repo" -ForegroundColor Cyan

    try {
        Push-Location $repo

        # Must be on a branch
        $branch = git symbolic-ref --short HEAD 2>$null
        if (-not $branch) {
            Write-Host "    Skipped: detached HEAD state" -ForegroundColor Yellow
            $results += [pscustomobject]@{ Repo = $repo; Status = "Skipped (detached HEAD)" }
            continue
        }

        # Must have a remote
        $remote = git remote 2>$null | Select-Object -First 1
        if (-not $remote) {
            Write-Host "    Skipped: no remote configured" -ForegroundColor Yellow
            $results += [pscustomobject]@{ Repo = $repo; Status = "Skipped (no remote)" }
            continue
        }

        # Check for local modifications / untracked files
        $status = git status --porcelain 2>$null
        $isDirty = [bool]$status

        if ($isDirty -and -not $Force) {
            Write-Host "    Skipped: local changes present (use -Force to discard)" -ForegroundColor Yellow
            $results += [pscustomobject]@{ Repo = $repo; Status = "Skipped (dirty)" }
            continue
        }

        if ($DryRun) {
            $mode = if ($Force) { "hard reset to $remote/$branch + clean" } else { "fast-forward pull" }
            Write-Host "    [DRY RUN] Would: fetch $remote, then $mode" -ForegroundColor Magenta
            $results += [pscustomobject]@{ Repo = $repo; Status = "DryRun" }
            continue
        }

        # Fetch and prune stale remote-tracking branches
        git fetch $remote --prune 2>&1 | Out-Null

        if ($Force) {
            git reset --hard "$remote/$branch" 2>&1 | Out-Null
            git clean -fd 2>&1 | Out-Null
            Write-Host "    Force-synced to $remote/$branch (local changes discarded)" -ForegroundColor Green
            $results += [pscustomobject]@{ Repo = $repo; Status = "Force-synced" }
        }
        else {
            # Only update if a fast-forward is possible
            $mergeBase = git merge-base HEAD "$remote/$branch" 2>$null
            $localHead = git rev-parse HEAD 2>$null

            if ($mergeBase -eq $localHead) {
                git merge --ff-only "$remote/$branch" 2>&1 | Out-Null
                Write-Host "    Fast-forwarded to $remote/$branch" -ForegroundColor Green
                $results += [pscustomobject]@{ Repo = $repo; Status = "Updated" }
            }
            elseif ((git rev-parse "$remote/$branch" 2>$null) -eq $localHead) {
                Write-Host "    Already up to date" -ForegroundColor DarkGray
                $results += [pscustomobject]@{ Repo = $repo; Status = "Up to date" }
            }
            else {
                Write-Host "    Skipped: local branch has diverged from $remote/$branch (use -Force to overwrite)" -ForegroundColor Yellow
                $results += [pscustomobject]@{ Repo = $repo; Status = "Skipped (diverged)" }
            }
        }
    }
    catch {
        Write-Host "    ERROR: $_" -ForegroundColor Red
        $results += [pscustomobject]@{ Repo = $repo; Status = "Error: $_" }
    }
    finally {
        Pop-Location
    }
}

Write-Host "`n----- Summary -----" -ForegroundColor Cyan
$results | Format-Table -AutoSize
