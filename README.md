# TraderKits

> An all-in-one Crypto data SDK — real-time quotes, historical data, and Alpha factors, all at your fingertips.

[![PyPI version](https://img.shields.io/pypi/v/traderkits.svg)](https://pypi.org/project/traderkits/)
[![Python](https://img.shields.io/pypi/pyversions/traderkits.svg)](https://pypi.org/project/traderkits/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

**[中文文档](docs/README.zh.md)**

---

## Overview

**TraderKits** is a data infrastructure SDK built for the Crypto market, providing quantitative researchers, traders, and developers with a unified and clean data interface.

Whether you need:
- Real-time quotes and order book data
- Structured K-lines and historical trade data
- Cross-sectional factor computation (Alpha / risk factors)

TraderKits aims to be the plug-and-play data layer in your workflow.

> The project is currently in **early development (Alpha)**. API design and feature scope are still rapidly iterating. Everyone interested in Crypto data is welcome to join the discussion, open Issues, or contribute code.

---

## Features

| Module | Description | Status |
|--------|-------------|--------|
| Real-time Data | Unified WebSocket / REST wrapper for major exchanges | 🚧 In Progress |
| Historical Data | K-lines, trades, funding rates — fetch & cache | 🚧 In Progress |
| Alpha Factor Library | Cross-sectional Alpha factor implementations & backtesting helpers | 📋 Planned |
| Risk Factor Module | Volatility, liquidity, correlation cross-sectional factors | 📋 Planned |
| TypeScript SDK | Feature-equivalent port for frontend / Node.js environments | 📋 Planned |

---

## Installation

[uv](https://github.com/astral-sh/uv) is recommended for package management.

> Note: The package has not been officially released yet. Installation instructions will be added once available on PyPI.

---

## Quick Start

> API still under design — usage examples will be added after the initial release.

---

## Project Structure

```
TraderKits
├── packages/
│   ├── python/            # Python SDK
│   │   ├── data/          # Data interface layer (real-time & historical)
│   │   ├── factors/       # Alpha / risk factors
│   │   └── utils/         # Shared utilities
│   └── typescript/        # TypeScript SDK (planned)
└── docs/                  # Documentation
```

### Roadmap

- [ ] Python SDK foundation
- [ ] Real-time data integration for Binance, OKX, and other major exchanges
- [ ] Unified interface for historical K-lines / trades
- [ ] Publish to PyPI
- [ ] Initial cross-sectional Alpha factors (momentum, funding rate deviation, etc.)
- [ ] Risk factor module (volatility, liquidity score, etc.)
- [ ] TypeScript SDK

---

## Factor Ideas (Alpha & Risk Factors)

TraderKits is more than a data pipeline — we will continuously curate and share meaningful cross-sectional factor ideas for the Crypto market, such as:

- **Funding Rate Factor**: Build long-short signals from the cross-sectional distribution of perpetual contract funding rates
- **On-chain Data Factor**: Large transfer flows, net exchange inflows, and other on-chain metrics
- **Liquidity Factor**: Bid-ask spread, depth asymmetry
- **Volatility Factor**: Realized volatility, implied volatility cross-sectional ranking
- **Momentum & Reversal**: Short-term reversal vs. medium-term momentum in Crypto

> These are preliminary ideas. Reference implementations will be gradually added under `packages/python/factors/` with brief explanations.

---

## Supported Languages

| Language | Status |
|----------|--------|
| Python | 🚧 In Progress |
| TypeScript | 📋 Planned |

---

## Contributing

The project is in its early stages and **all forms of contribution are very welcome**:

- 🐛 **Found a bug?** Open an [Issue](../../issues) with a description and reproduction steps
- 💡 **Factor / feature ideas?** Share your thoughts on Crypto data or factors in the Issues
- 🔧 **Code contributions:** Fork and submit a PR — all changes, big or small, are welcome
- 📖 **Documentation:** Fix ambiguities or gaps by editing and submitting a PR

If you have any questions during usage or want to discuss Crypto data and quantitative strategies, feel free to leave a comment in the Issues. Let's build this together.

---

## License

[MIT](LICENSE)
