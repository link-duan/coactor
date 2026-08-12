# Treat distributed Actor lifecycle as core

跨节点 ownership、故障恢复和迁移属于 CoActor 的长期核心能力，而不是可有可无的附加组件；否则 CoActor 只会成为另一个进程内 Actor library。实现采用分阶段验证，首个版本只构建单进程生命周期切片，并明确排除状态存储、distributed ownership、fencing、恢复与迁移。任何阶段都不能把本地 Active Actor registry、进程存活或 Handler Reply 描述成全局 ownership、durable delivery 或 durable ACK。后续阶段必须把 ownership 与状态恢复整体推进，并分别证明旧 Owner 写入被 fencing、已 durable ACK 的命令可恢复，以及迁移期间命令去向明确。
