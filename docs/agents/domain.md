# 领域文档

本仓库只有一个 bounded context。工程技能在探索、编写 spec 或修改代码时必须使用项目领域文档。

## 探索前

- 阅读仓库根目录的 `CONTEXT.md`。
- 阅读 `docs/adr/` 下与修改区域相关的 ADR。
- 如果任一位置不存在，直接继续，不要预先建议创建。

## 词汇

Issue 标题、spec、实现计划、测试名称和 code review feedback 必须使用 `CONTEXT.md` 中定义的 canonical term。避免使用 `_Avoid_` 中明确列出的同义词。

如果缺少需要的概念，先判断现有词汇是否已经覆盖。只有确实存在领域空缺时，才通过 domain-modeling workflow 记录新词汇。

## ADR

任何与 accepted ADR 冲突的方案都必须明确指出，不能静默覆盖。Supersede 旧决策的新 ADR 必须列出被取代的 ADR，并保留旧决策中仍然有效的部分。

## 布局

```text
/
├── CONTEXT.md
└── docs/
    └── adr/
```
