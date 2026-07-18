# 播客Web应用后端设计文档

## 项目概述

基于WSL2 + AMD ROCm算力核心的播客Web应用后端，采用微服务架构（方案C），支持从方案一（本地全栈自用）平滑升级到方案二（分离式服务/多用户SaaS）。

- **LLM接入**：通义千问 API（Qwen）
- **TTS引擎**：Voicebox ROCm版（WSL2 Docker部署）
- **架构模式**：分层微服务 + PostgreSQL + Redis + Celery

## 整体架构

```
┌─────────────┐     ┌──────────────────────────────────────────┐
│  Web前端     │────▶│  API Gateway (FastAPI)                   │
│  Next.js     │     │  - 认证鉴权 / 限流 / 路由转发           │
└─────────────┘     │  - 统一入口 :8000                        │
                    └──────┬──────┬──────┬──────┬──────────────┘
                           │      │      │      │
              ┌────────────┘      │      │      └────────────┐
              ▼                   ▼      ▼                   ▼
     ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
     │ Script Svc   │  │ Audio Svc    │  │ Project Svc  │  │ Task Svc     │
     │ :8001        │  │ :8002        │  │ :8003        │  │ :8004        │
     │              │  │              │  │              │  │              │
     │ - LLM脚本    │  │ - Voicebox   │  │ - 项目管理   │  │ - 任务调度   │
     │ - 文档解析   │  │ - 音频生成   │  │ - 版本管理   │  │ - 进度推送   │
     │ - Map-Reduce │  │ - 后期处理   │  │ - 音色管理   │  │ - 失败重试   │
     └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
            │                 │                  │                 │
            ▼                 ▼                  ▼                 ▼
     ┌─────────────────────────────────────────────────────────────────┐
     │  PostgreSQL          │  Redis          │  MinIO / 本地存储     │
     │  - 用户/项目/脚本    │  - 任务队列     │  - 音频文件           │
     │  - 音色/生成记录     │  - 缓存         │  - 上传文档           │
     │  - 计费统计          │  - SSE推送      │  - BGM素材            │
     └─────────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    │  WSL2 算力节点     │
                    │  Voicebox :17493  │
                    │  ROCm 加速        │
                    └───────────────────┘
```

## 四大微服务详细设计

### Script Service（脚本服务 :8001）

**职责**：大模型驱动的播客脚本生成与文档解析

**核心能力**：
- 接入通义千问 API，通过强约束 Prompt 模板生成结构化播客脚本（JSON格式）
- 支持 PDF/Word/Markdown/TXT 文档上传解析，提取纯文本
- 万字长文自动 Map-Reduce 分块摘要 → 连贯脚本生成
- 三种播客模式的 Prompt 模板切换：单人旁白、双人对话、多人访谈
- JSON 修复 + 字段校验（json-repair），确保 LLM 输出可解析
- 脚本版本管理，支持多版本对比、多模型效果对比

**关键API**：
- `POST /api/script/generate` — 从主题/文本生成播客脚本
- `POST /api/script/generate-from-doc` — 从上传文档生成脚本
- `GET /api/script/{project_id}/versions` — 获取脚本版本列表
- `GET /api/script/{script_id}` — 获取脚本详情
- `PUT /api/script/{script_id}` — 更新脚本内容

**Prompt模板设计**：
```
系统提示：你是专业播客编剧，根据用户输入生成结构化JSON脚本。
输出格式约束：
{
  "title": "播客标题",
  "characters": [{"id": 1, "name": "角色名", "personality": "性格描述"}],
  "lines": [
    {"character_id": 1, "text": "台词内容", "emotion": "neutral/excited/serious/humorous", "pace": "normal/fast/slow"}
  ],
  "bgm_hints": [{"position": "intro", "style": "tech/casual/dramatic"}]
}
```

**文档解析流程**：
1. 上传文件 → 2. 按类型提取纯文本 → 3. 判断文本长度
   - 短文本（<4000字）：直接送入LLM生成脚本
   - 长文本（≥4000字）：Map-Reduce分块摘要 → 合并摘要 → 生成脚本

### Audio Service（音频服务 :8002）

**职责**：Voicebox对接、音频生成与后期处理

**核心能力**：
- 封装 Voicebox REST API，按角色批量提交 TTS 任务
- 调用 Voicebox 内置音频效果（8类 Pedalboard 效果 + 4套预设）
- FFmpeg + pedalboard 后期流水线
- BGM 素材库管理，按风格自动匹配

**后期流水线步骤**：
1. 按角色生成干音（调用Voicebox TTS API）
2. 干音效果处理（调用Voicebox音频效果API，或本地pedalboard处理）
3. 响度标准化（-16 LUFS，FFmpeg loudnorm）
4. 插入节奏停顿（角色切换间隔0.5-1.5秒随机停顿）
5. BGM混音：侧链压缩（人声闪避）、音量自动调节
6. 片头片尾淡入淡出
7. 输出最终成品音频

**关键API**：
- `POST /api/audio/generate` — 提交音频生成任务
- `GET /api/audio/task/{task_id}` — 查询任务状态
- `GET /api/audio/download/{file_id}` — 下载音频文件
- `POST /api/audio/bgm/library` — 管理BGM素材库
- `GET /api/audio/bgm/library` — 获取BGM列表

**Voicebox对接**：
```python
class VoiceboxClient:
    """Voicebox REST API 封装，支持单节点和多节点调度"""
    async def list_models(self) -> list[str]
    async def generate_tts(self, text: str, voice_id: str, model: str, **kwargs) -> bytes
    async def apply_effect(self, audio: bytes, effect_chain: list) -> bytes
    async def get_voices(self) -> list[dict]
    async def health_check(self) -> bool
```

### Project Service（项目管理服务 :8003）

**职责**：项目CRUD、音色管理、配置存储

**核心能力**：
- 项目 CRUD：创建/读取/更新/删除播客项目
- 角色配置：自定义角色数量、名称、性格、绑定对应音色
- 音色档案管理：创建、克隆、导入导出、默认效果绑定
- 脚本版本、音频版本的关联存储
- 用户数据隔离

**关键API**：
- `POST /api/project` — 创建项目
- `GET /api/project/{id}` — 获取项目详情
- `PUT /api/project/{id}` — 更新项目配置
- `DELETE /api/project/{id}` — 删除项目
- `GET /api/project/{id}/characters` — 获取角色列表
- `POST /api/voice-profile` — 创建音色档案
- `GET /api/voice-profile` — 获取音色列表

### Task Service（任务调度服务 :8004）

**职责**：异步任务编排、进度推送、算力调度

**核心能力**：
- Celery 异步任务队列，编排全流程：脚本生成 → 干音生成 → 后期混音
- SSE 实时进度推送，前端可监听每个阶段状态
- 失败自动重试（最多3次）、超时处理（单步超时5分钟）
- 显存/GPU 利用率监控，动态控制并发数
- 方案二：支持多算力节点负载均衡

**任务流程编排**：
```
PodcastGenerationTask:
  Step 1: generate_script → Script Service
  Step 2: generate_dry_audio (per character) → Audio Service (并行)
  Step 3: post_processing → Audio Service
  Step 4: finalize → 存储 + 通知
```

**关键API**：
- `POST /api/task/podcast` — 提交播客生成全流程任务
- `GET /api/task/{id}/status` — 查询任务状态
- `GET /api/task/{id}/events` — SSE进度事件流
- `POST /api/task/{id}/cancel` — 取消任务
- `GET /api/task/gpu-status` — 查询GPU状态

## API Gateway

统一入口，所有前端请求经过 Gateway：

- **认证鉴权**：
  - 方案一：简单 API Key 校验
  - 方案二：JWT Token 校验 + 用户注册登录
- **限流**：按用户/IP 限速，防止滥用
- **路由转发**：按路径前缀分发到各微服务
  - `/api/script/*` → Script Service :8001
  - `/api/audio/*` → Audio Service :8002
  - `/api/project/*` → Project Service :8003
  - `/api/task/*` → Task Service :8004
- **跨域处理**：CORS 配置

**方案一部署模式**：Gateway 和所有微服务在同一 docker-compose 中，内网通信
**方案二部署模式**：Gateway 独立部署对外，微服务在内网

## 数据模型

### 核心表结构

```sql
-- 用户表（方案二启用，方案一使用默认用户）
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE,
    password_hash VARCHAR(255),
    role VARCHAR(20) DEFAULT 'user',  -- user/admin
    api_key VARCHAR(64) UNIQUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- 项目表
CREATE TABLE projects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES users(id),
    name VARCHAR(200) NOT NULL,
    description TEXT,
    mode VARCHAR(10) NOT NULL,  -- solo/duo/group
    config JSONB DEFAULT '{}',  -- 风格、时长目标等配置
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 角色表
CREATE TABLE characters (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    name VARCHAR(50) NOT NULL,
    personality TEXT,
    voice_id VARCHAR(100),
    "order" INT DEFAULT 0
);

-- 脚本表
CREATE TABLE scripts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    version INT NOT NULL,
    content JSONB NOT NULL,  -- 结构化JSON脚本
    model VARCHAR(50),  -- 使用的LLM模型
    prompt_template VARCHAR(20),  -- 使用的Prompt模板
    source_type VARCHAR(20),  -- topic/document/text
    source_text TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- 音频任务表
CREATE TABLE audio_tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID REFERENCES projects(id),
    script_id UUID REFERENCES scripts(id),
    status VARCHAR(20) DEFAULT 'pending',  -- pending/processing/completed/failed
    progress JSONB DEFAULT '{}',  -- 各步骤进度
    error TEXT,
    config JSONB DEFAULT '{}',  -- 生成配置
    created_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP
);

-- 音频文件表
CREATE TABLE audio_files (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    audio_task_id UUID REFERENCES audio_tasks(id),
    character_id UUID REFERENCES characters(id),
    file_path VARCHAR(500) NOT NULL,
    duration FLOAT,
    format VARCHAR(10) DEFAULT 'mp3',
    file_type VARCHAR(20),  -- dry_voice/final_mix/bgm
    created_at TIMESTAMP DEFAULT NOW()
);

-- 音色档案表
CREATE TABLE voice_profiles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES users(id),
    name VARCHAR(100) NOT NULL,
    description TEXT,
    voice_id VARCHAR(100) NOT NULL,  -- Voicebox音色ID
    samples JSONB DEFAULT '[]',  -- 克隆采样文件列表
    default_effects JSONB DEFAULT '[]',  -- 默认效果链
    created_at TIMESTAMP DEFAULT NOW()
);

-- BGM素材表
CREATE TABLE bgm_library (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL,
    style VARCHAR(50) NOT NULL,  -- tech/casual/dramatic/ambient/upbeat
    file_path VARCHAR(500) NOT NULL,
    duration FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);
```

## 方案一/方案二切换策略

通过抽象接口层实现切换，核心业务代码无需改动：

| 组件 | 方案一实现 | 方案二实现 |
|------|-----------|-----------|
| 认证 | `APIKeyAuth` | `JWTAuth` |
| 算力调度 | `SingleNodeVoicebox` | `MultiNodeScheduler` |
| 存储 | `LocalStorage` | `S3Storage` |
| 部署 | docker-compose 一键 | 多机 docker-compose / K8s |

切换方式：通过环境变量 `DEPLOY_MODE=local|cloud` 选择实现类，依赖注入到各服务。

## 技术栈

| 组件 | 技术选型 | 说明 |
|------|---------|------|
| Web框架 | FastAPI | 异步高性能，自动API文档 |
| 任务队列 | Celery + Redis | 成熟的异步任务方案 |
| 数据库 | PostgreSQL | JSONB支持、可靠的事务 |
| ORM | SQLAlchemy 2.0 + Alembic | 异步ORM + 数据库迁移 |
| 缓存 | Redis | 缓存 + 任务队列 + SSE |
| 音频处理 | FFmpeg + pedalboard | 专业音频处理 |
| 文档解析 | PyPDF2 + python-docx + markdown | 多格式文档解析 |
| LLM接入 | OpenAI兼容SDK（通义千问） | Qwen API兼容OpenAI格式 |
| JSON修复 | json-repair | 修复LLM输出格式问题 |
| 容器化 | Docker + docker-compose | 一键部署 |

## 项目目录结构

```
podcast-app/
├── docker-compose.yml
├── .env.example
├── services/
│   ├── gateway/
│   │   ├── Dockerfile
│   │   ├── main.py
│   │   ├── auth/
│   │   │   ├── api_key.py
│   │   │   └── jwt_auth.py
│   │   ├── middleware/
│   │   │   ├── rate_limit.py
│   │   │   └── cors.py
│   │   └── router.py
│   ├── script/
│   │   ├── Dockerfile
│   │   ├── main.py
│   │   ├── llm/
│   │   │   ├── qwen_client.py
│   │   │   ├── prompt_templates.py
│   │   │   └── json_fixer.py
│   │   ├── parser/
│   │   │   ├── pdf_parser.py
│   │   │   ├── docx_parser.py
│   │   │   ├── md_parser.py
│   │   │   └── map_reduce.py
│   │   ├── models.py
│   │   └── routers/
│   │       └── script.py
│   ├── audio/
│   │   ├── Dockerfile
│   │   ├── main.py
│   │   ├── voicebox/
│   │   │   ├── client.py
│   │   │   └── multi_node.py
│   │   ├── pipeline/
│   │   │   ├── effects.py
│   │   │   ├── mixing.py
│   │   │   └── loudness.py
│   │   ├── models.py
│   │   └── routers/
│   │       └── audio.py
│   ├── project/
│   │   ├── Dockerfile
│   │   ├── main.py
│   │   ├── models.py
│   │   └── routers/
│   │       ├── project.py
│   │       └── voice_profile.py
│   └── task/
│       ├── Dockerfile
│       ├── main.py
│       ├── celery_app.py
│       ├── tasks/
│       │   └── podcast_task.py
│       ├── gpu_monitor.py
│       ├── models.py
│       └── routers/
│           └── task.py
├── shared/
│   ├── database.py
│   ├── config.py
│   ├── storage/
│   │   ├── local.py
│   │   └── s3.py
│   └── events.py
├── migrations/
│   └── alembic/
├── storage/
│   ├── audio/
│   ├── documents/
│   └── bgm/
└── tests/
    ├── test_script_service.py
    ├── test_audio_service.py
    ├── test_project_service.py
    └── test_task_service.py
```

## 保留的Voicebox原生功能

底层 Voicebox 服务完整保留，播客业务层全部通过 REST API 调用，不侵入底层代码：

- 7套原生TTS引擎（Qwen3-TTS 0.6B/1.7B、Qwen CustomVoice、LuxTTS、Chatterbox、HumeAI TADA、Kokoro）
- 23种语言、零样本音色克隆、50+预设音色、副语言情绪标签
- 8类 Pedalboard 音频效果、4套内置预设、自定义效果链
- Stories 多轨时间线编辑器
- 音色档案创建、多采样克隆、导入导出
- Captures 录音/转录管理、Whisper STT
- 完整 REST API + /docs 接口文档
- MCP 服务、Agent 语音输出
- ROCm/CUDA/DirectML/CPU 全硬件加速
- 原生Web界面（:17493）独立可访问

## 部署配置

### docker-compose.yml 核心结构

```yaml
version: "3.8"
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: podcast
      POSTGRES_USER: podcast
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    volumes:
      - pgdata:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  gateway:
    build: ./services/gateway
    ports:
      - "8000:8000"
    environment:
      - DEPLOY_MODE=local
      - SCRIPT_SERVICE_URL=http://script:8001
      - AUDIO_SERVICE_URL=http://audio:8002
      - PROJECT_SERVICE_URL=http://project:8003
      - TASK_SERVICE_URL=http://task:8004
    depends_on:
      - script
      - audio
      - project
      - task

  script:
    build: ./services/script
    ports:
      - "8001:8001"
    environment:
      - DATABASE_URL=postgresql+asyncpg://podcast:${DB_PASSWORD}@postgres:5432/podcast
      - QWEN_API_KEY=${QWEN_API_KEY}
    depends_on:
      - postgres

  audio:
    build: ./services/audio
    ports:
      - "8002:8002"
    environment:
      - DATABASE_URL=postgresql+asyncpg://podcast:${DB_PASSWORD}@postgres:5432/podcast
      - VOICEBOX_URL=http://host.docker.internal:17493
    volumes:
      - ./storage:/app/storage
    depends_on:
      - postgres

  project:
    build: ./services/project
    ports:
      - "8003:8003"
    environment:
      - DATABASE_URL=postgresql+asyncpg://podcast:${DB_PASSWORD}@postgres:5432/podcast
    depends_on:
      - postgres

  task:
    build: ./services/task
    ports:
      - "8004:8004"
    environment:
      - DATABASE_URL=postgresql+asyncpg://podcast:${DB_PASSWORD}@postgres:5432/podcast
      - REDIS_URL=redis://redis:6379/0
      - SCRIPT_SERVICE_URL=http://script:8001
      - AUDIO_SERVICE_URL=http://audio:8002
    depends_on:
      - postgres
      - redis

  celery-worker:
    build: ./services/task
    command: celery -A celery_app worker -l info
    environment:
      - DATABASE_URL=postgresql+asyncpg://podcast:${DB_PASSWORD}@postgres:5432/podcast
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      - postgres
      - redis

volumes:
  pgdata:
```

## 错误处理策略

- **LLM调用失败**：自动重试3次，指数退避；返回具体错误信息（API Key无效/额度不足/网络超时）
- **Voicebox不可用**：健康检查探针，标记算力节点为不可用；任务排队等待恢复
- **音频生成失败**：按角色粒度重试，不影响已成功的角色
- **数据库连接异常**：SQLAlchemy 连接池自动重连
- **任务超时**：单步超时5分钟，全任务超时30分钟，自动标记为failed
