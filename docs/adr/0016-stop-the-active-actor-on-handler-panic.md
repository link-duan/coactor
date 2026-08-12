# Stop the Active Actor on handler panic

CoActor 首版对 command handler panic 采用 fail-stop：终止整个 Active Actor，使当前和 mailbox 中已接纳但未处理的 command 返回 `ActorStopped`，不调用 deactivation lifecycle，并移除本地路由。后续 command 可以创建新的空状态实例，但 runtime 不自动重放 command 或原地重启。没有 durable mailbox 和状态恢复时，局部继续执行或自动重放都可能暴露已被 panic 部分修改的内存状态或重复业务副作用；终止实例是更清晰的首版失败边界。
