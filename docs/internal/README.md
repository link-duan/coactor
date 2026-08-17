# 内部文档

CoActor 的对外文档使用英文，只描述使用方法、当前保证和限制；内部文档使用中文，记录领域语言、决策原因、路线图、调研与发布流程。

## 文档入口

- [`CONTEXT.md`](../../CONTEXT.md)：领域词汇表，只定义概念，不记录实现细节。
- [ADR](../adr/README.md)：难以逆转且存在真实取舍的架构决策。
- [产品路线图](product-roadmap.md)：未交付方向，不构成发布承诺。
- [技术调研](../research/)：形成决策前的事实与方案比较。
- [S3 发布资格验证](release/s3-qualification.md)：不能由普通 CI 证明的真实环境验证。
- [Agent 规则](../agents/)：Issue tracker、triage 与领域文档约定。

## 内容边界

| 内容 | 事实来源 |
| --- | --- |
| 公开 API | Rust 源码与 rustdoc |
| 对外运行保证 | `docs/runtime.md` |
| S3 部署要求 | `docs/s3.md` |
| 领域术语 | `CONTEXT.md` |
| 决策及原因 | `docs/adr/` |
| 未交付方向 | `docs/internal/product-roadmap.md` |

根 README 不链接 ADR、research、Agent 规则或内部发布流程。

Getting Started 必须使用真实 `Server`、`Client` 和 Coordination Store；`TestServer` 只能出现在测试文档。公开文档不复制完整 API 参数、默认值或内部算法，这些内容分别属于 rustdoc、源码和 ADR。
