---
status: accepted
---

# Infrastructure boundaries

CoActor 自己定义并验证 Actor identity、注册、寻址、lifecycle、routing、ownership、fencing 与 failure semantics，但不以自研共识算法、对象存储、嵌入式数据库或通用网络传输为目标。成熟基础设施通过少量明确边界接入，其实际保证必须由一手资料、contract tests 与端到端测试确认；采用某个组件本身不能自动证明 CoActor 的分布式正确性。

因此 Node 间传输复用 gRPC/Protobuf，ownership authority 复用 AWS S3 conditional operations，未来 Actor Store 复用成熟本地 KV；这些技术只承担基础能力，不定义 Actor 领域语义。CoActor 仍负责决定何时接纳 command、何时 self-fence、何时允许 takeover、何种失败可以重试，以及 Handler Reply 能证明什么。
