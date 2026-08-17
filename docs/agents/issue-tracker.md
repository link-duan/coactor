# Issue tracker：GitHub

本仓库的 issue 与 spec 使用 GitHub Issues 管理。所有操作使用 `gh` CLI。

## 仓库

- GitHub repository：`link-duan/coactor`
- 在 clone 内通过 `git remote -v` 推断仓库；`gh` 会自动处理。

## 约定

- **创建 issue**：`gh issue create --title "..." --body "..."`。多行 body 使用 heredoc。
- **读取 issue**：`gh issue view <number> --comments`，同时获取 labels。
- **列出 issue**：`gh issue list --state open --json number,title,body,labels,comments`，按任务设置 label 与 state filter。
- **评论 issue**：`gh issue comment <number> --body "..."`。
- **添加或移除 label**：`gh issue edit <number> --add-label "..."` 或 `--remove-label "..."`。
- **关闭 issue**：`gh issue close <number> --comment "..."`。

## PR 不是需求入口

**不使用 PR 作为需求入口。**

GitHub 的 issue 与 pull request 共用 number space。若裸编号存在歧义，先尝试 `gh pr view <number>`，失败后再使用 `gh issue view <number>`。

## Skill 集成

- Skill 要求 **publish to the issue tracker** 时，创建 GitHub issue。
- Skill 要求 **fetch the relevant ticket** 时，读取 GitHub issue 及其 comments。
