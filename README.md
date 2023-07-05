## prom-stream-downsample

> 基于 prometheus 的原生流式数据降采样组件, 可配置不同降采样策略和降采样函数。



### 1. 配置说明

> ```
> 注意，生成的 downsample 指标会以  
> xxx:downsample_5m_avg
> 格式命名，即 
> 原指标名 + ":downsample_" + 降采样周期 + "_" + 降采样函数
> ```

> ```yaml
> global_config:
>   enabled_stream: true  # 是否开启流式传输,prometheus 2.0+以上支持；关闭后默认使用 sample 模式
>   enabled_downsample: true # 是否开启降采样
>   enabled_proxy: true  # 是否开启 proxy 功能,proxy用来为做反代，自动替换指标名
>   enabled_metric_reuse: true # 是否开启指标重用(下一级采样会用上一级的数据)
>   prometheus:
>     remote_read_url: http://10.0.0.105:9090/api/v1/read   # row data 读地址
>     remote_write_url: http://10.0.0.105:9090/api/v1/write # downsample 结果写入地址
>   resolutions:  # 降采样策略；前者表示具体的降采样，后者在 proxy 开启的情况下会自动将原 metric 替换为 downsample metric
>     - 5m,7d		# 配置5m降采样，在 range_query 大于 7d 时自动替换
>     - 10m,15d   # 配置10m降采样，在 range_query 大于 15d 时自动替换
>     - 1h,30d    # 配置1h降采样，在 range_query 大于 30d 时自动替换
> 
> # 生成的 downsample 会重命名为 xxx:downsample_5m_avg
> downsample_config:
>   - label_name: __name__  # 这段配置的含义是: 将 {__name__=prometheus_tsdb_head_chunks},将窗口内的点以 5m/10/1h 为采样周期，分别执行 aggregations 中的降采样算法 
>     job_name: test-01
>     label_value: prometheus_tsdb_head_chunks
>     matcher_type: =   # 支持 = / =~ 
>     aggregations:
>       - sum 	# 和
>       - avg		# 平均数
>       - first	# 第一个点
>       - last	# 最后一个点
>       - median	# 中位数
>       - mode	# 众数
>       - min		# 最小值
>       - max		# 最大值
>       - random	# 随机选点
>       - stddev	# 方差  3-sigma离群
>       - sumsq   # 平方和
>       - count	# 点数量
>       - p50		# 50分位
>       - p90		# 90分位
>       - p95		# 95分位
>       - p99		# 99分位
>       - p999	# 999分位
> 
>   - label_name: job 	# 这段配置的含义是: 将 {job=~promethe.+},将窗口内的点以 5m/10/1h 为采样周期，分别执行 aggregations 中的降采样算法 
>     job_name: test-02
>     label_value: promethe.+
>     matcher_type: =~
>     aggregations:
>       - count
>       - last
> 
> proxy_config:
>   listen_addr: :9119	# proxy 端口
>   prometheus_addr: http://10.0.0.105:9090/	# 反代要查询的 prometheus 节点
>   proxy_metrics:		# 反代指标配置
>     - metric_name: prometheus_tsdb_head_chunks	# 表示自动替换 prometheus_tsdb_head_chunks 指标为 min 的降采样指标
>       aggregation: min
>     - metric_name: test_heavy_query				# 表示自动替换 test_heavy_query 指标为 avg 的降采样指标
>       aggregation: avg
> ```
>
> proxy 开启后，只需修改 grafana 的query 地址为 http://prom-stream-downsample:9119/ 即可
>
> 注意，proxy 插件目前会对 /api/v1/query_range /api/v1/query 接口做自动替换；同时对于替换后的 range vector 不匹配导致无数据问题也做了适配；
> proxy 会根据 resolutions 配置自动 替换合适指标 和 调整 range vector范围 (query_range/query都会调整)
