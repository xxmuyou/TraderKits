# TraderKits

> 一站式 Crypto 数据 SDK —— 实时行情、历史数据、Alpha 因子，全部触手可及。

[![PyPI version](https://img.shields.io/pypi/v/traderkits.svg)](https://pypi.org/project/traderkits/)
[![Python](https://img.shields.io/pypi/pyversions/traderkits.svg)](https://pypi.org/project/traderkits/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

**[English Documentation](../README.md)**

---

## 项目简介

**TraderKits** 是一个专为 Crypto 市场设计的数据基础设施 SDK，旨在为量化研究人员、交易员和开发者提供统一、简洁的数据接口。

无论你需要：
- 实时行情与订单簿数据
- 结构化的 K 线与历史交易数据
- 截面因子（Alpha / 风险因子）的计算与复用

TraderKits 都希望成为你工作流中那个"开箱即用"的数据层。

> 项目目前处于 **早期开发阶段（Alpha）**，接口设计和功能范围仍在快速迭代中。欢迎所有对 Crypto 数据感兴趣的朋友加入讨论、提出 Issue 或贡献代码。

---

## 核心功能

| 模块 | 说明 | 状态 |
|------|------|------|
| 实时数据接口 | WebSocket / REST 统一封装，支持主流交易所 | 🚧 开发中 |
| 历史数据接口 | K 线、成交、资金费率等历史数据拉取与缓存 | 🚧 开发中 |
| Alpha 因子库 | 截面 Alpha 因子的思路实现与回测辅助 | 📋 规划中 |
| 风险因子模块 | 波动率、流动性、相关性等风险截面因子 | 📋 规划中 |
| TypeScript 版本 | 面向前端/Node.js 场景的同等功能移植 | 📋 规划中 |

---

## 安装

推荐使用 [uv](https://github.com/astral-sh/uv) 进行包管理。

> 注意：包尚未正式发布，安装方式待 PyPI 上线后补充。

---

## 快速上手

> 接口设计中，待初版发布后补充使用示例。

---

## 项目规划

```
TraderKits
├── packages/
│   ├── python/            # Python SDK
│   │   ├── data/          # 数据接口层（实时 & 历史）
│   │   ├── factors/       # Alpha / 风险因子
│   │   └── utils/         # 通用工具
│   └── typescript/        # TypeScript SDK（规划中）
└── docs/                  # 文档
```

### 路线图（Roadmap）

- [ ] Python SDK 基础架构搭建
- [ ] 支持 Binance、OKX 等主流交易所实时数据接入
- [ ] 历史 K 线 / 成交数据统一接口
- [ ] 发布至 PyPI
- [ ] 截面 Alpha 因子初版（动量、资金费率偏离等）
- [ ] 风险因子模块（波动率、流动性评分等）
- [ ] TypeScript SDK

---

## 因子思路（Alpha & 风险因子）

TraderKits 不仅仅是数据管道，我们也会持续整理和分享在 Crypto 市场中有意义的截面因子思路，例如：

- **资金费率因子**：利用永续合约资金费率的截面分布构建多空信号
- **链上数据因子**：大额转账、交易所净流入等链上指标
- **流动性因子**：买卖价差、深度不对称度
- **波动率因子**：已实现波动率、隐含波动率截面排序
- **动量与反转**：短周期反转 vs. 中期动量在 Crypto 的表现

> 以上为初步思路，后续将陆续在 `traderkits/factors/` 中提供参考实现，并附上简要的逻辑说明。

---

## 支持语言

| 语言 | 状态 |
|------|------|
| Python | 🚧 开发中 |
| TypeScript | 📋 规划中 |

---

## 参与贡献

项目处于初期，**非常欢迎** 各种形式的参与：

- 🐛 **发现 Bug**：请直接开 [Issue](../../issues) 描述问题和复现步骤
- 💡 **因子 / 功能思路**：欢迎在 Issue 中分享你对 Crypto 数据或因子的想法
- 🔧 **代码贡献**：Fork 后提交 PR，无论大小改动都欢迎
- 📖 **文档完善**：发现文档有歧义或缺失，直接修改提 PR 即可

如果你在使用过程中有任何问题，或者想聊聊 Crypto 数据和量化方向，都欢迎在 Issue 区留言，让我们一起把这个工具做得更好。

---

## License

[MIT](../LICENSE)
