# Issue tracker: GitHub

Issues and specs for this repo live as GitHub issues. Use the `gh` CLI for all operations.

## Repository

- GitHub repository: `link-duan/coactor`
- Infer the repository from `git remote -v`; `gh` does this automatically inside the clone.

## Conventions

- **Create an issue**: `gh issue create --title "..." --body "..."`. Use a heredoc for multi-line bodies.
- **Read an issue**: `gh issue view <number> --comments` and fetch its labels.
- **List issues**: `gh issue list --state open --json number,title,body,labels,comments` with appropriate label and state filters.
- **Comment on an issue**: `gh issue comment <number> --body "..."`.
- **Apply or remove labels**: `gh issue edit <number> --add-label "..."` or `--remove-label "..."`.
- **Close an issue**: `gh issue close <number> --comment "..."`.

## Pull requests as a triage surface

**PRs as a request surface: no.**

GitHub shares one number space across issues and pull requests. If a bare number is ambiguous, try `gh pr view <number>` and fall back to `gh issue view <number>`.

## Skill integration

- When a skill says **publish to the issue tracker**, create a GitHub issue.
- When a skill says **fetch the relevant ticket**, read the GitHub issue and its comments.
