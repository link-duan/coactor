# S3 发布资格验证

CoActor 内置 Coordination Store 的设计依赖 AWS S3 conditional-write 语义。本地可编程 HTTP fixture 可以验证请求契约，但不能证明其行为与真实 AWS 服务完全等价。

本文件是维护者发布流程，不属于对外运行保证。

## 合并门禁

常规测试必须验证：

- Node Lease 与 Actor Owner Record 使用精确、可读的 object key；
- record body 不重复 key 已携带的 identity；
- create 使用 `If-None-Match`，replace/delete 使用 `If-Match`；
- Node Lease generation 正确递增；
- conflict 与 indeterminate mutation 分类正确；
- 模拟响应丢失后执行精确 read-back；
- Node Directory cache freshness 与 singleflight 行为。

S3 Store 从 AWS SDK S3 Client 构造。测试和部署代码通过 AWS SDK 配置 credentials、region、endpoint、retry、HTTP 与 timeout，不增加 CoActor 专有凭证字段。

## 真实 AWS 资格验证

生产发布前，应使用临时凭证、专用的现有 AWS bucket 和本次运行独占的 canonical prefix，针对相同协议执行验证。

资格验证至少覆盖：

- conditional create、replace 与 delete；
- ETag/revision 保留；
- 成功写入后的 read/list consistency；
- mutation 已应用但响应丢失后的精确 read-back；
- 按 storage revision 接管过期 Node Lease；
- graceful shutdown 的 conditional lease deletion；
- 新 Node Session ID 以更高 epoch 接管 Actor ownership；
- Node authority 丢失后 runtime self-fence。

真实 AWS 资格验证保持为外部发布门禁，不进入常规本地或 CI 流程。只有在目标部署配置中通过验证后，才能对外声称相关版本已针对 AWS S3 完成验证；否则应准确描述为“按 AWS S3 语义设计”。

## Lifecycle policy

进程 crash 后遗留的 Node Lease object 在过期后会被 runtime 忽略，CoActor 不运行分布式 janitor。

生产 bucket 应配置 Lifecycle expiration 清理过期 Node object。若启用了 S3 Versioning，还必须配置 noncurrent-version expiration，避免被替换和删除的 lease version 长期累积。
