# Omit state storage from the first version

CoActor 首个版本只验证进程内 Actor identity、按 key 启动、有界 mailbox、单 Actor 串行执行和 idle passivation，不引入 Actor-scoped KV、journal、snapshot、checkpoint 或 restore。状态存储会同时牵引本地 KV 引擎、对象布局、恢复协议、ownership fencing 与故障状态机；在基础执行语义尚未通过 vertical slice 验证前加入这些能力会扩大首版边界。此前形成的 Actor Store 与 S3 方案保留为后续候选设计，但不得出现在首版 API 或保证中；首版 Active Actor 在 passivation、crash 或进程重启后从空状态重新启动。
