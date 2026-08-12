# Keep Actor methods non-reentrant

CoActor 首版让一个 Actor method future 从开始到结束独占 Active Actor，即使 future 正在 `.await` 外部 IO，也不从 mailbox 执行下一条 command。允许 await 点可重入会使 Actor 状态在单个方法尚未完成时被其他方法修改，要求 consumer 理解交错执行、不变量重验和循环调用死锁。首版优先提供明确的单 Actor 串行语义；长时间 IO 会阻塞该 Actor，consumer 需自行设置外部调用边界并避免无限等待。
