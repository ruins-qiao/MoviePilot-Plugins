# MoviePilot-Plugins（V3 第三方插件仓库）

用于存放和分发兼容 **MoviePilot V3** 的第三方插件源码、市场索引与测试用例，目录结构严格遵循官方 [MoviePilot-Plugins](https://github.com/jxxghp/MoviePilot-Plugins) V3 规范。

## 目录结构

```text
.
├── .github/
│   ├── scripts/
│   │   ├── check_plugin_versions.py       # 校验插件代码与 package.v3.json 版本一致性
│   │   └── select_plugin_release_dir.sh   # 按索引定位发布目录
│   └── workflows/
│       ├── check.yml                      # Push/PR 自动执行编译与版本一致性检查
│       └── release.yml                    # 支持 release: true 插件自动打包发布
├── icons/
│   └── U115RapidTransfer.png              # 115秒传助手专属图标
├── plugins.v3/
│   └── u115rapidtransfer/                 # 115秒传助手插件源码
│       ├── __init__.py
│       ├── rapid_proxy.py
│       └── README.md
├── tests/
│   └── v3/
│       └── u115rapidtransfer/
│           └── test_plugin.py             # 插件单元测试
├── package.v3.json                        # V3 插件市场索引
├── pytest.ini                             # pytest 测试配置
├── LICENSE
└── README.md
```

## 插件列表

| 插件 ID | 名称 | 版本 | 说明 |
| --- | --- | --- | --- |
| `U115RapidTransfer` | 115秒传助手 | `1.1.2` | 整理到 115 网盘时仅允许秒传（禁止普通 OSS 上传），秒传失败后等待指定时间自动重试。 |

## 如何在 MoviePilot V3 中安装

1. 将本仓库推送到你的 GitHub（例如 `https://github.com/<你的用户名>/<仓库名>`）。
2. 在 MoviePilot V3 的 **插件市场 -> 自定义插件库** 中填入你的仓库地址，保存后刷新插件市场即可安装 **115秒传助手**。
3. 如需启用 GitHub Release 压缩包分发，可在 `package.v3.json` 对应条目中增加 `"release": true`，推送到 `main` 分支后 `.github/workflows/release.yml` 会自动打包并创建 Release。

## 本地开发校验

假设 `MoviePilot` V3 主仓库位于同级目录 `../MoviePilot`：

```bash
python3 -m compileall plugins.v3/u115rapidtransfer tests/v3/u115rapidtransfer
python3 .github/scripts/check_plugin_versions.py package.v3.json
../MoviePilot/.venv/bin/python -m pytest tests/v3/u115rapidtransfer
git diff --check
```
# MoviePilot-Plugins
