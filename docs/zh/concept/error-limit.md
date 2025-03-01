# Error Limit

## 简介

SeaTunnel 提供了强大的错误限制功能,允许您控制数据同步过程中的错误处理。当错误记录数量或比例超过设定的阈值时,任务会自动失败。
这个功能在需要确保数据质量和可靠性时非常有用。错误限制主要通过两个关键参数控制:`error.limit.record` 和 `error.limit.percentage`。
本文档将指导您如何使用这些参数并有效地利用它们。

## 支持的引擎

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark<br/>

## 配置

要使用错误限制功能,您需要在作业配置中配置 `error.limit.record` 或 `error.limit.percentage` 参数。

示例配置:

```hocon
env {
    job.mode = STREAMING
    job.name = SeaTunnel_Job
    
    # 当错误记录数超过1000时任务失败
    error.limit.record = 1000
    
    # 当错误记录比例超过10%时任务失败
    error.limit.percentage = 0.1
}

source {
    MySQL-CDC {
      // 忽略其他配置...
    }
}

transform {
}

sink {
    Console {
    }
}
```

我们将 `error.limit.record` 和 `error.limit.percentage` 放在 `env` 参数中完成错误限制配置。
您可以同时配置这两个参数或只配置其中一个。每个参数的含义如下:

### error.limit.record

- 类型:整数
- 必选:否
- 默认值:无限制
- 描述:允许的最大错误记录数。当错误记录数超过此值时,任务将失败。

### error.limit.percentage

- 类型:浮点数(0.0-1.0)
- 必选:否
- 默认值:无限制
- 描述:允许的最大错误记录比例。当错误记录比例超过此值时,任务将失败。比例计算方式为:错误记录数/总记录数。

## 使用说明

1. 两个限制可以同时配置,任一限制达到阈值就会触发任务失败

2. 如果都不配置,则表示不启用错误限制功能

3. percentage 的值范围应该在 0.0-1.0 之间

4. 当任务因错误限制失败时,会提供详细的错误信息,包括:
    - 当前错误记录数
    - 当前错误记录比例
    - 触发失败的具体原因

5. 错误限制功能支持所有 Source 和 Sink 连接器

## 环境变量配置

除了在配置文件中设置,您也可以通过环境变量来配置错误限制:

```bash
export ERROR_LIMIT_COUNT=1000
export ERROR_LIMIT_PERCENTAGE=0.1
```

这种方式特别适合在不同环境中灵活配置错误限制。