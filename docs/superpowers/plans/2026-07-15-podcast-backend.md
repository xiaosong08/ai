# 播客Web应用后端实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 构建播客Web应用的微服务后端，集成Voicebox TTS引擎和通义千问LLM，支持文档智能生成播客脚本、单人/双人/多人播客模式、自动化后期流水线。

**Architecture:** 四大微服务（Script/Audio/Project/Task）+ API Gateway，PostgreSQL持久化，Redis任务队列与缓存，Celery异步任务编排，通过抽象接口层支持方案一/方案二切换。

**Tech Stack:** Python 3.11, FastAPI, SQLAlchemy 2.0 (async), Alembic, PostgreSQL, Redis, Celery, httpx, FFmpeg, pedalboard, PyPDF2, python-docx, json-repair, Docker Compose

---

## 文件结构总览

```
podcast-app/
├── docker-compose.yml
├── .env.example
├── pyproject.toml                    # 项目依赖和配置
├── shared/
│   ├── __init__.py
│   ├── config.py                     # 全局配置（环境变量加载）
│   ├── database.py                   # 数据库连接、会话管理
│   ├── models.py                     # SQLAlchemy ORM 模型
│   ├── schemas.py                    # Pydantic 请求/响应模型
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── base.py                   # 存储抽象接口
│   │   └── local.py                  # 本地文件系统实现
│   └── events.py                     # SSE事件定义
├── services/
│   ├── gateway/
│   │   ├── __init__.py
│   │   ├── main.py                   # Gateway入口
│   │   ├── auth/
│   │   │   ├── __init__.py
│   │   │   ├── api_key.py            # API Key认证
│   │   │   └── jwt_auth.py           # JWT认证（方案二）
│   │   ├── middleware/
│   │   │   ├── __init__.py
│   │   │   ├── rate_limit.py         # 限流中间件
│   │   │   └── cors.py               # CORS配置
│   │   └── router.py                 # 路由转发
│   ├── script/
│   │   ├── __init__.py
│   │   ├── main.py                   # Script Service入口
│   │   ├── llm/
│   │   │   ├── __init__.py
│   │   │   ├── qwen_client.py        # 通义千问API客户端
│   │   │   ├── prompt_templates.py   # Prompt模板
│   │   │   └── json_fixer.py         # JSON修复与校验
│   │   ├── parser/
│   │   │   ├── __init__.py
│   │   │   ├── pdf_parser.py         # PDF解析
│   │   │   ├── docx_parser.py        # Word解析
│   │   │   ├── md_parser.py          # Markdown解析
│   │   │   └── map_reduce.py         # Map-Reduce分块处理
│   │   └── routers/
│   │       ├── __init__.py
│   │       └── script.py             # 脚本API路由
│   ├── audio/
│   │   ├── __init__.py
│   │   ├── main.py                   # Audio Service入口
│   │   ├── voicebox/
│   │   │   ├── __init__.py
│   │   │   ├── client.py             # Voicebox API客户端
│   │   │   └── multi_node.py         # 多节点调度器
│   │   ├── pipeline/
│   │   │   ├── __init__.py
│   │   │   ├── effects.py            # 音频效果处理
│   │   │   ├── mixing.py             # BGM混音与侧链压缩
│   │   │   └── loudness.py           # 响度标准化
│   │   └── routers/
│   │       ├── __init__.py
│   │       └── audio.py              # 音频API路由
│   ├── project/
│   │   ├── __init__.py
│   │   ├── main.py                   # Project Service入口
│   │   └── routers/
│   │       ├── __init__.py
│   │       ├── project.py            # 项目CRUD路由
│   │       └── voice_profile.py      # 音色档案路由
│   └── task/
│       ├── __init__.py
│       ├── main.py                   # Task Service入口
│       ├── celery_app.py             # Celery配置
│       ├── tasks/
│       │   ├── __init__.py
│       │   └── podcast_task.py       # 播客生成全流程任务
│       ├── gpu_monitor.py            # GPU状态监控
│       └── routers/
│           ├── __init__.py
│           └── task.py               # 任务API路由
├── migrations/
│   ├── env.py                        # Alembic环境配置
│   └── versions/
│       └── 001_initial.py            # 初始迁移
├── storage/
│   ├── audio/
│   ├── documents/
│   └── bgm/
└── tests/
    ├── conftest.py                   # 测试 fixtures
    ├── test_script_service.py
    ├── test_audio_service.py
    ├── test_project_service.py
    └── test_task_service.py
```

---

## Task 1: 项目脚手架与共享基础设施

**Files:**
- Create: `podcast-app/pyproject.toml`
- Create: `podcast-app/.env.example`
- Create: `podcast-app/shared/__init__.py`
- Create: `podcast-app/shared/config.py`
- Create: `podcast-app/shared/database.py`
- Create: `podcast-app/shared/models.py`
- Create: `podcast-app/shared/schemas.py`
- Create: `podcast-app/shared/storage/__init__.py`
- Create: `podcast-app/shared/storage/base.py`
- Create: `podcast-app/shared/storage/local.py`
- Create: `podcast-app/shared/events.py`

- [ ] **Step 1: 创建项目目录结构**

```bash
cd /workspace
mkdir -p podcast-app/{shared/storage,services/{gateway/{auth,middleware},script/{llm,parser,routers},audio/{voicebox,pipeline,routers},project/routers,task/{tasks,routers}},migrations/versions,storage/{audio,documents,bgm},tests}
touch podcast-app/shared/__init__.py
touch podcast-app/shared/storage/__init__.py
touch podcast-app/services/__init__.py
touch podcast-app/services/gateway/__init__.py
touch podcast-app/services/gateway/auth/__init__.py
touch podcast-app/services/gateway/middleware/__init__.py
touch podcast-app/services/script/__init__.py
touch podcast-app/services/script/llm/__init__.py
touch podcast-app/services/script/parser/__init__.py
touch podcast-app/services/script/routers/__init__.py
touch podcast-app/services/audio/__init__.py
touch podcast-app/services/audio/voicebox/__init__.py
touch podcast-app/services/audio/pipeline/__init__.py
touch podcast-app/services/audio/routers/__init__.py
touch podcast-app/services/project/__init__.py
touch podcast-app/services/project/routers/__init__.py
touch podcast-app/services/task/__init__.py
touch podcast-app/services/task/tasks/__init__.py
touch podcast-app/services/task/routers/__init__.py
```

- [ ] **Step 2: 创建 pyproject.toml**

```toml
[project]
name = "podcast-app"
version = "0.1.0"
description = "Podcast Web Application Backend - Microservices Architecture"
requires-python = ">=3.11"
dependencies = [
    "fastapi>=0.111.0",
    "uvicorn[standard]>=0.30.0",
    "sqlalchemy[asyncio]>=2.0.30",
    "asyncpg>=0.29.0",
    "alembic>=1.13.0",
    "celery[redis]>=5.4.0",
    "redis>=5.0.0",
    "httpx>=0.27.0",
    "pydantic>=2.7.0",
    "pydantic-settings>=2.3.0",
    "python-multipart>=0.0.9",
    "python-dotenv>=1.0.0",
    "json-repair>=0.25.0",
    "PyPDF2>=3.0.0",
    "python-docx>=1.1.0",
    "markdown>=3.6",
    "pedalboard>=0.9.0",
    "soundfile>=0.12.0",
    "numpy>=1.26.0",
    "passlib>=1.7.4",
    "python-jose[cryptography]>=3.3.0",
    "aioredis>=2.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.2.0",
    "pytest-asyncio>=0.23.0",
    "httpx>=0.27.0",
    "pytest-cov>=5.0.0",
]

[build-system]
requires = ["setuptools>=70.0"]
build-backend = "setuptools.build_meta"
```

- [ ] **Step 3: 创建 .env.example**

```env
# 部署模式：local（方案一）/ cloud（方案二）
DEPLOY_MODE=local

# 数据库
DATABASE_URL=postgresql+asyncpg://podcast:podcast123@localhost:5432/podcast

# Redis
REDIS_URL=redis://localhost:6379/0

# 通义千问 API
QWEN_API_KEY=your_qwen_api_key_here
QWEN_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
QWEN_MODEL=qwen-plus

# Voicebox
VOICEBOX_URL=http://localhost:17493

# 认证（方案一用API Key，方案二用JWT）
API_KEY=podcast-local-dev-key
JWT_SECRET=your-jwt-secret-change-in-production

# 存储
STORAGE_PATH=./storage

# 服务端口
GATEWAY_PORT=8000
SCRIPT_SERVICE_PORT=8001
AUDIO_SERVICE_PORT=8002
PROJECT_SERVICE_PORT=8003
TASK_SERVICE_PORT=8004
```

- [ ] **Step 4: 创建 shared/config.py**

```python
"""全局配置 - 从环境变量加载"""
from pydantic_settings import BaseSettings
from pathlib import Path


class Settings(BaseSettings):
    # 部署模式
    deploy_mode: str = "local"

    # 数据库
    database_url: str = "postgresql+asyncpg://podcast:podcast123@localhost:5432/podcast"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # 通义千问
    qwen_api_key: str = ""
    qwen_base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    qwen_model: str = "qwen-plus"

    # Voicebox
    voicebox_url: str = "http://localhost:17493"

    # 认证
    api_key: str = "podcast-local-dev-key"
    jwt_secret: str = "change-me-in-production"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 1440

    # 存储
    storage_path: str = "./storage"

    # 服务端口
    gateway_port: int = 8000
    script_service_port: int = 8001
    audio_service_port: int = 8002
    project_service_port: int = 8003
    task_service_port: int = 8004

    # 服务间通信URL（Docker内网或localhost）
    script_service_url: str = "http://localhost:8001"
    audio_service_url: str = "http://localhost:8002"
    project_service_url: str = "http://localhost:8003"
    task_service_url: str = "http://localhost:8004"

    # Celery
    celery_broker_url: str = "redis://localhost:6379/1"
    celery_result_backend: str = "redis://localhost:6379/2"

    # 音频
    target_loudness: float = -16.0
    default_format: str = "mp3"
    sample_rate: int = 44100

    # Map-Reduce
    chunk_size: int = 3000
    chunk_overlap: int = 300

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
```

- [ ] **Step 5: 创建 shared/database.py**

```python
"""数据库连接与会话管理"""
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from shared.config import settings

engine = create_async_engine(settings.database_url, echo=False, pool_size=10, max_overflow=20)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncSession:
    """FastAPI依赖注入：获取数据库会话"""
    async with async_session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
```

- [ ] **Step 6: 创建 shared/models.py — SQLAlchemy ORM 模型**

```python
"""数据库 ORM 模型"""
import uuid
from datetime import datetime
from sqlalchemy import String, Text, Float, Integer, ForeignKey, DateTime, JSON
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from shared.database import Base


class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    username: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    email: Mapped[str | None] = mapped_column(String(100), unique=True)
    password_hash: Mapped[str | None] = mapped_column(String(255))
    role: Mapped[str] = mapped_column(String(20), default="user")
    api_key: Mapped[str | None] = mapped_column(String(64), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    projects = relationship("Project", back_populates="user", lazy="selectin")
    voice_profiles = relationship("VoiceProfile", back_populates="user", lazy="selectin")


class Project(Base):
    __tablename__ = "projects"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"))
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    mode: Mapped[str] = mapped_column(String(10), nullable=False)  # solo/duo/group
    config: Mapped[dict] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="projects")
    characters = relationship("Character", back_populates="project", cascade="all, delete-orphan", lazy="selectin")
    scripts = relationship("Script", back_populates="project", cascade="all, delete-orphan", lazy="selectin")
    audio_tasks = relationship("AudioTask", back_populates="project", lazy="selectin")


class Character(Base):
    __tablename__ = "characters"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"))
    name: Mapped[str] = mapped_column(String(50), nullable=False)
    personality: Mapped[str | None] = mapped_column(Text)
    voice_id: Mapped[str | None] = mapped_column(String(100))
    order: Mapped[int] = mapped_column(Integer, default=0)

    project = relationship("Project", back_populates="characters")


class Script(Base):
    __tablename__ = "scripts"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"))
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    content: Mapped[dict] = mapped_column(JSONB, nullable=False)
    model: Mapped[str | None] = mapped_column(String(50))
    prompt_template: Mapped[str | None] = mapped_column(String(20))
    source_type: Mapped[str | None] = mapped_column(String(20))  # topic/document/text
    source_text: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    project = relationship("Project", back_populates="scripts")
    audio_tasks = relationship("AudioTask", back_populates="script", lazy="selectin")


class AudioTask(Base):
    __tablename__ = "audio_tasks"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("projects.id"))
    script_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("scripts.id"))
    status: Mapped[str] = mapped_column(String(20), default="pending")  # pending/processing/completed/failed
    progress: Mapped[dict] = mapped_column(JSONB, default=dict)
    error: Mapped[str | None] = mapped_column(Text)
    config: Mapped[dict] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime)

    project = relationship("Project", back_populates="audio_tasks")
    script = relationship("Script", back_populates="audio_tasks")
    audio_files = relationship("AudioFile", back_populates="audio_task", lazy="selectin")


class AudioFile(Base):
    __tablename__ = "audio_files"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    audio_task_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("audio_tasks.id"))
    character_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("characters.id"))
    file_path: Mapped[str] = mapped_column(String(500), nullable=False)
    duration: Mapped[float | None] = mapped_column(Float)
    format: Mapped[str] = mapped_column(String(10), default="mp3")
    file_type: Mapped[str | None] = mapped_column(String(20))  # dry_voice/final_mix/bgm

    audio_task = relationship("AudioTask", back_populates="audio_files")


class VoiceProfile(Base):
    __tablename__ = "voice_profiles"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"))
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    voice_id: Mapped[str] = mapped_column(String(100), nullable=False)
    samples: Mapped[dict] = mapped_column(JSONB, default=list)
    default_effects: Mapped[dict] = mapped_column(JSONB, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="voice_profiles")


class BgmLibrary(Base):
    __tablename__ = "bgm_library"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    style: Mapped[str] = mapped_column(String(50), nullable=False)  # tech/casual/dramatic/ambient/upbeat
    file_path: Mapped[str] = mapped_column(String(500), nullable=False)
    duration: Mapped[float | None] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
```

- [ ] **Step 7: 创建 shared/schemas.py — Pydantic 请求/响应模型**

```python
"""Pydantic 请求/响应 Schema"""
import uuid
from datetime import datetime
from pydantic import BaseModel, Field


# ============ 项目相关 ============

class ProjectCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: str | None = None
    mode: str = Field(..., pattern="^(solo|duo|group)$")
    config: dict = Field(default_factory=dict)


class ProjectUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    mode: str | None = None
    config: dict | None = None


class ProjectResponse(BaseModel):
    id: uuid.UUID
    name: str
    description: str | None
    mode: str
    config: dict
    created_at: datetime
    updated_at: datetime
    characters: list["CharacterResponse"] = Field(default_factory=list)

    model_config = {"from_attributes": True}


# ============ 角色相关 ============

class CharacterCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=50)
    personality: str | None = None
    voice_id: str | None = None
    order: int = 0


class CharacterResponse(BaseModel):
    id: uuid.UUID
    name: str
    personality: str | None
    voice_id: str | None
    order: int

    model_config = {"from_attributes": True}


# ============ 脚本相关 ============

class ScriptGenerateRequest(BaseModel):
    project_id: uuid.UUID
    source_type: str = Field(..., pattern="^(topic|text|document)$")
    source_text: str | None = None
    mode: str = Field(..., pattern="^(solo|duo|group)$")
    characters: list[CharacterCreate] | None = None
    style: str = "casual"  # casual/tech/dramatic/educational
    target_duration_minutes: int = 5


class ScriptGenerateFromDocRequest(BaseModel):
    project_id: uuid.UUID
    mode: str = Field(..., pattern="^(solo|duo|group)$")
    characters: list[CharacterCreate] | None = None
    style: str = "casual"
    target_duration_minutes: int = 5


class ScriptLineSchema(BaseModel):
    character_id: int
    text: str
    emotion: str = "neutral"
    pace: str = "normal"


class ScriptContentSchema(BaseModel):
    title: str
    characters: list[dict]
    lines: list[ScriptLineSchema]
    bgm_hints: list[dict] = Field(default_factory=list)


class ScriptResponse(BaseModel):
    id: uuid.UUID
    project_id: uuid.UUID
    version: int
    content: ScriptContentSchema
    model: str | None
    prompt_template: str | None
    source_type: str | None
    created_at: datetime

    model_config = {"from_attributes": True}


class ScriptUpdateRequest(BaseModel):
    content: ScriptContentSchema


# ============ 音频相关 ============

class AudioGenerateRequest(BaseModel):
    project_id: uuid.UUID
    script_id: uuid.UUID
    model: str = "qwen3-tts-0.6b"
    apply_effects: bool = True
    bgm_style: str | None = None
    output_format: str = "mp3"


class AudioTaskResponse(BaseModel):
    id: uuid.UUID
    project_id: uuid.UUID
    script_id: uuid.UUID | None
    status: str
    progress: dict
    error: str | None
    config: dict
    created_at: datetime
    completed_at: datetime | None
    audio_files: list["AudioFileResponse"] = Field(default_factory=list)

    model_config = {"from_attributes": True}


class AudioFileResponse(BaseModel):
    id: uuid.UUID
    character_id: uuid.UUID | None
    file_path: str
    duration: float | None
    format: str
    file_type: str | None

    model_config = {"from_attributes": True}


# ============ 音色相关 ============

class VoiceProfileCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    description: str | None = None
    voice_id: str
    samples: list[str] = Field(default_factory=list)
    default_effects: list[dict] = Field(default_factory=list)


class VoiceProfileResponse(BaseModel):
    id: uuid.UUID
    name: str
    description: str | None
    voice_id: str
    samples: list
    default_effects: list
    created_at: datetime

    model_config = {"from_attributes": True}


# ============ 任务相关 ============

class PodcastTaskRequest(BaseModel):
    project_id: uuid.UUID
    source_type: str = "topic"
    source_text: str | None = None
    mode: str = "duo"
    characters: list[CharacterCreate] | None = None
    style: str = "casual"
    target_duration_minutes: int = 5
    tts_model: str = "qwen3-tts-0.6b"
    bgm_style: str | None = None
    output_format: str = "mp3"


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    progress: dict
    error: str | None


class GpuStatusResponse(BaseModel):
    available: bool
    gpu_name: str | None
    vram_total_mb: int | None
    vram_used_mb: int | None
    vram_free_mb: int | None
    gpu_utilization: float | None


# ============ BGM相关 ============

class BgmCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    style: str = Field(..., pattern="^(tech|casual|dramatic|ambient|upbeat)$")


class BgmResponse(BaseModel):
    id: uuid.UUID
    name: str
    style: str
    file_path: str
    duration: float | None

    model_config = {"from_attributes": True}


# ============ 通用 ============

class HealthResponse(BaseModel):
    status: str
    service: str
    version: str = "0.1.0"


class ErrorResponse(BaseModel):
    error: str
    detail: str | None = None
```

- [ ] **Step 8: 创建 shared/storage/base.py 和 local.py**

```python
# shared/storage/base.py
"""存储抽象接口"""
from abc import ABC, abstractmethod
from pathlib import Path


class StorageProvider(ABC):
    @abstractmethod
    async def save(self, path: str, data: bytes) -> str:
        """保存文件，返回完整路径"""
        ...

    @abstractmethod
    async def read(self, path: str) -> bytes:
        """读取文件"""
        ...

    @abstractmethod
    async def delete(self, path: str) -> bool:
        """删除文件"""
        ...

    @abstractmethod
    async def exists(self, path: str) -> bool:
        """文件是否存在"""
        ...

    @abstractmethod
    def get_url(self, path: str) -> str:
        """获取文件访问URL"""
        ...
```

```python
# shared/storage/local.py
"""本地文件系统存储实现"""
import os
from pathlib import Path
from shared.storage.base import StorageProvider
from shared.config import settings


class LocalStorage(StorageProvider):
    def __init__(self, base_path: str | None = None):
        self.base_path = Path(base_path or settings.storage_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    async def save(self, path: str, data: bytes) -> str:
        full_path = self.base_path / path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(data)
        return str(full_path)

    async def read(self, path: str) -> bytes:
        full_path = self.base_path / path
        return full_path.read_bytes()

    async def delete(self, path: str) -> bool:
        full_path = self.base_path / path
        if full_path.exists():
            full_path.unlink()
            return True
        return False

    async def exists(self, path: str) -> bool:
        return (self.base_path / path).exists()

    def get_url(self, path: str) -> str:
        return f"/storage/{path}"
```

```python
# shared/storage/__init__.py
from shared.config import settings
from shared.storage.base import StorageProvider
from shared.storage.local import LocalStorage


def get_storage() -> StorageProvider:
    if settings.deploy_mode == "cloud":
        # 方案二：可替换为 S3Storage
        raise NotImplementedError("S3Storage not implemented yet")
    return LocalStorage()
```

- [ ] **Step 9: 创建 shared/events.py — SSE事件定义**

```python
"""SSE 事件定义"""
from dataclasses import dataclass, field
from datetime import datetime
import json


@dataclass
class TaskEvent:
    """播客生成任务进度事件"""
    task_id: str
    step: str          # script_generation / dry_audio / post_processing / finalize
    status: str        # started / progress / completed / failed
    progress: float = 0.0
    message: str = ""
    data: dict = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_sse(self) -> str:
        return f"data: {json.dumps(self.__dict__, ensure_ascii=False)}\n\n"


# 步骤权重（用于计算总进度）
STEP_WEIGHTS = {
    "script_generation": 0.2,
    "dry_audio": 0.5,
    "post_processing": 0.25,
    "finalize": 0.05,
}
```

- [ ] **Step 10: 安装依赖并验证**

```bash
cd /workspace/podcast-app
pip install -e ".[dev]"
python -c "from shared.config import settings; print(f'Config loaded: {settings.deploy_mode}')"
```

Expected: `Config loaded: local`

- [ ] **Step 11: Commit**

```bash
git add podcast-app/
git commit -m "feat: scaffold project structure with shared infrastructure (config, database, models, schemas, storage)"
```

---

## Task 2: 数据库迁移

**Files:**
- Create: `podcast-app/migrations/env.py`
- Create: `podcast-app/migrations/versions/001_initial.py`
- Create: `podcast-app/alembic.ini`

- [ ] **Step 1: 初始化 Alembic**

```bash
cd /workspace/podcast-app
pip install alembic
```

- [ ] **Step 2: 创建 alembic.ini**

```ini
[alembic]
script_location = migrations
sqlalchemy.url = postgresql+asyncpg://podcast:podcast123@localhost:5432/podcast

[loggers]
keys = root,sqlalchemy,alembic

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARN
handlers = console

[logger_sqlalchemy]
level = WARN
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
datefmt = %H:%M:%S
```

- [ ] **Step 3: 创建 migrations/env.py**

```python
"""Alembic 环境配置 - 支持异步"""
import asyncio
from logging.config import fileConfig
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config
from alembic import context

from shared.database import Base
from shared.models import User, Project, Character, Script, AudioTask, AudioFile, VoiceProfile, BgmLibrary  # noqa

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True, dialect_opts={"paramstyle": "named"})
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection):
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

- [ ] **Step 4: 创建初始迁移 001_initial.py**

```python
"""初始数据库迁移 - 创建所有表

Revision ID: 001
Revises:
Create Date: 2026-07-15
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("username", sa.String(50), unique=True, nullable=False),
        sa.Column("email", sa.String(100), unique=True),
        sa.Column("password_hash", sa.String(255)),
        sa.Column("role", sa.String(20), server_default="user"),
        sa.Column("api_key", sa.String(64), unique=True),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
    )
    op.create_table(
        "projects",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("user_id", UUID(as_uuid=True), sa.ForeignKey("users.id")),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("description", sa.Text),
        sa.Column("mode", sa.String(10), nullable=False),
        sa.Column("config", JSONB, server_default="{}"),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime, server_default=sa.func.now()),
    )
    op.create_table(
        "characters",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("project_id", UUID(as_uuid=True), sa.ForeignKey("projects.id", ondelete="CASCADE")),
        sa.Column("name", sa.String(50), nullable=False),
        sa.Column("personality", sa.Text),
        sa.Column("voice_id", sa.String(100)),
        sa.Column("order", sa.Integer, server_default="0"),
    )
    op.create_table(
        "scripts",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("project_id", UUID(as_uuid=True), sa.ForeignKey("projects.id", ondelete="CASCADE")),
        sa.Column("version", sa.Integer, nullable=False),
        sa.Column("content", JSONB, nullable=False),
        sa.Column("model", sa.String(50)),
        sa.Column("prompt_template", sa.String(20)),
        sa.Column("source_type", sa.String(20)),
        sa.Column("source_text", sa.Text),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
    )
    op.create_table(
        "audio_tasks",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("project_id", UUID(as_uuid=True), sa.ForeignKey("projects.id")),
        sa.Column("script_id", UUID(as_uuid=True), sa.ForeignKey("scripts.id")),
        sa.Column("status", sa.String(20), server_default="pending"),
        sa.Column("progress", JSONB, server_default="{}"),
        sa.Column("error", sa.Text),
        sa.Column("config", JSONB, server_default="{}"),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime),
    )
    op.create_table(
        "audio_files",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("audio_task_id", UUID(as_uuid=True), sa.ForeignKey("audio_tasks.id")),
        sa.Column("character_id", UUID(as_uuid=True), sa.ForeignKey("characters.id")),
        sa.Column("file_path", sa.String(500), nullable=False),
        sa.Column("duration", sa.Float),
        sa.Column("format", sa.String(10), server_default="mp3"),
        sa.Column("file_type", sa.String(20)),
    )
    op.create_table(
        "voice_profiles",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("user_id", UUID(as_uuid=True), sa.ForeignKey("users.id")),
        sa.Column("name", sa.String(100), nullable=False),
        sa.Column("description", sa.Text),
        sa.Column("voice_id", sa.String(100), nullable=False),
        sa.Column("samples", JSONB, server_default="[]"),
        sa.Column("default_effects", JSONB, server_default="[]"),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
    )
    op.create_table(
        "bgm_library",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(100), nullable=False),
        sa.Column("style", sa.String(50), nullable=False),
        sa.Column("file_path", sa.String(500), nullable=False),
        sa.Column("duration", sa.Float),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("bgm_library")
    op.drop_table("voice_profiles")
    op.drop_table("audio_files")
    op.drop_table("audio_tasks")
    op.drop_table("scripts")
    op.drop_table("characters")
    op.drop_table("projects")
    op.drop_table("users")
```

- [ ] **Step 5: 验证迁移脚本语法**

```bash
cd /workspace/podcast-app
python -c "from migrations.versions.001_initial import upgrade, downgrade; print('Migration script OK')"
```

Expected: `Migration script OK`

- [ ] **Step 6: Commit**

```bash
git add podcast-app/migrations podcast-app/alembic.ini
git commit -m "feat: add database migration with Alembic for all tables"
```

---

## Task 3: Project Service（项目管理服务）

**Files:**
- Create: `podcast-app/services/project/main.py`
- Create: `podcast-app/services/project/routers/project.py`
- Create: `podcast-app/services/project/routers/voice_profile.py`
- Test: `podcast-app/tests/test_project_service.py`

- [ ] **Step 1: 创建 Project Service 入口 main.py**

```python
# services/project/main.py
"""Project Service - 项目管理与音色档案"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from fastapi import FastAPI
from shared.config import settings
from services.project.routers.project import router as project_router
from services.project.routers.voice_profile import router as voice_profile_router

app = FastAPI(title="Podcast Project Service", version="0.1.0")

app.include_router(project_router, prefix="/api/project", tags=["project"])
app.include_router(voice_profile_router, prefix="/api/voice-profile", tags=["voice-profile"])


@app.get("/health")
async def health():
    return {"status": "ok", "service": "project"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=settings.project_service_port)
```

- [ ] **Step 2: 创建项目CRUD路由 routers/project.py**

```python
# services/project/routers/project.py
"""项目 CRUD 路由"""
import uuid
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from shared.database import get_db
from shared.models import Project, Character
from shared.schemas import ProjectCreate, ProjectUpdate, ProjectResponse, CharacterCreate, CharacterResponse

router = APIRouter()


@router.post("", response_model=ProjectResponse)
async def create_project(data: ProjectCreate, db: AsyncSession = Depends(get_db)):
    project = Project(name=data.name, description=data.description, mode=data.mode, config=data.config)
    db.add(project)
    await db.flush()
    await db.refresh(project)
    return project


@router.get("/{project_id}", response_model=ProjectResponse)
async def get_project(project_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Project).where(Project.id == project_id))
    project = result.scalar_one_or_none()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project


@router.put("/{project_id}", response_model=ProjectResponse)
async def update_project(project_id: uuid.UUID, data: ProjectUpdate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Project).where(Project.id == project_id))
    project = result.scalar_one_or_none()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    for key, value in data.model_dump(exclude_unset=True).items():
        setattr(project, key, value)
    await db.flush()
    await db.refresh(project)
    return project


@router.delete("/{project_id}")
async def delete_project(project_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Project).where(Project.id == project_id))
    project = result.scalar_one_or_none()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    await db.delete(project)
    return {"status": "deleted"}


@router.get("/{project_id}/characters", response_model=list[CharacterResponse])
async def get_characters(project_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Character).where(Character.project_id == project_id).order_by(Character.order))
    return list(result.scalars().all())


@router.post("/{project_id}/characters", response_model=list[CharacterResponse])
async def set_characters(project_id: uuid.UUID, characters: list[CharacterCreate], db: AsyncSession = Depends(get_db)):
    # 先删除旧角色
    result = await db.execute(select(Character).where(Character.project_id == project_id))
    for old in result.scalars().all():
        await db.delete(old)
    # 创建新角色
    new_chars = []
    for i, char_data in enumerate(characters):
        char = Character(project_id=project_id, name=char_data.name, personality=char_data.personality, voice_id=char_data.voice_id, order=i)
        db.add(char)
        new_chars.append(char)
    await db.flush()
    for char in new_chars:
        await db.refresh(char)
    return new_chars
```

- [ ] **Step 3: 创建音色档案路由 routers/voice_profile.py**

```python
# services/project/routers/voice_profile.py
"""音色档案管理路由"""
import uuid
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from shared.database import get_db
from shared.models import VoiceProfile
from shared.schemas import VoiceProfileCreate, VoiceProfileResponse

router = APIRouter()


@router.post("", response_model=VoiceProfileResponse)
async def create_voice_profile(data: VoiceProfileCreate, db: AsyncSession = Depends(get_db)):
    profile = VoiceProfile(name=data.name, description=data.description, voice_id=data.voice_id, samples=data.samples, default_effects=data.default_effects)
    db.add(profile)
    await db.flush()
    await db.refresh(profile)
    return profile


@router.get("", response_model=list[VoiceProfileResponse])
async def list_voice_profiles(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(VoiceProfile).order_by(VoiceProfile.created_at.desc()))
    return list(result.scalars().all())


@router.get("/{profile_id}", response_model=VoiceProfileResponse)
async def get_voice_profile(profile_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(VoiceProfile).where(VoiceProfile.id == profile_id))
    profile = result.scalar_one_or_none()
    if not profile:
        raise HTTPException(status_code=404, detail="Voice profile not found")
    return profile


@router.delete("/{profile_id}")
async def delete_voice_profile(profile_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(VoiceProfile).where(VoiceProfile.id == profile_id))
    profile = result.scalar_one_or_none()
    if not profile:
        raise HTTPException(status_code=404, detail="Voice profile not found")
    await db.delete(profile)
    return {"status": "deleted"}
```

- [ ] **Step 4: 编写测试**

```python
# tests/test_project_service.py
"""Project Service 测试"""
import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from shared.database import Base, get_db
from shared.models import Project
from services.project.main import app

TEST_DB_URL = "sqlite+aiosqlite:///./test_project.db"
test_engine = create_async_engine(TEST_DB_URL, echo=True)
test_session = async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)


async def override_get_db():
    async with test_session() as session:
        yield session


app.dependency_overrides[get_db] = override_get_db


@pytest.fixture(autouse=True)
async def setup_db():
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.mark.asyncio
async def test_create_project():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/api/project", json={
            "name": "Test Podcast",
            "description": "A test podcast project",
            "mode": "duo",
            "config": {"style": "tech"}
        })
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Test Podcast"
    assert data["mode"] == "duo"


@pytest.mark.asyncio
async def test_get_project():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # 创建
        create_resp = await client.post("/api/project", json={
            "name": "Test", "description": "Desc", "mode": "solo"
        })
        project_id = create_resp.json()["id"]
        # 获取
        response = await client.get(f"/api/project/{project_id}")
    assert response.status_code == 200
    assert response.json()["name"] == "Test"


@pytest.mark.asyncio
async def test_delete_project():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        create_resp = await client.post("/api/project", json={
            "name": "To Delete", "mode": "group"
        })
        project_id = create_resp.json()["id"]
        response = await client.delete(f"/api/project/{project_id}")
    assert response.status_code == 200
```

- [ ] **Step 5: 运行测试**

```bash
cd /workspace/podcast-app
pip install aiosqlite
pytest tests/test_project_service.py -v
```

Expected: 3 tests pass

- [ ] **Step 6: Commit**

```bash
git add podcast-app/services/project/ podcast-app/tests/
git commit -m "feat: implement Project Service with CRUD and voice profile management"
```

---

## Task 4: Script Service（脚本生成服务）

**Files:**
- Create: `podcast-app/services/script/main.py`
- Create: `podcast-app/services/script/llm/qwen_client.py`
- Create: `podcast-app/services/script/llm/prompt_templates.py`
- Create: `podcast-app/services/script/llm/json_fixer.py`
- Create: `podcast-app/services/script/parser/pdf_parser.py`
- Create: `podcast-app/services/script/parser/docx_parser.py`
- Create: `podcast-app/services/script/parser/md_parser.py`
- Create: `podcast-app/services/script/parser/map_reduce.py`
- Create: `podcast-app/services/script/routers/script.py`
- Test: `podcast-app/tests/test_script_service.py`

- [ ] **Step 1: 创建 Script Service 入口 main.py**

```python
# services/script/main.py
"""Script Service - 脚本生成与文档解析"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from fastapi import FastAPI
from shared.config import settings
from services.script.routers.script import router as script_router

app = FastAPI(title="Podcast Script Service", version="0.1.0")

app.include_router(script_router, prefix="/api/script", tags=["script"])


@app.get("/health")
async def health():
    return {"status": "ok", "service": "script"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=settings.script_service_port)
```

- [ ] **Step 2: 创建 Prompt 模板 llm/prompt_templates.py**

```python
# services/script/llm/prompt_templates.py
"""播客脚本生成 Prompt 模板"""

SOLO_SYSTEM = """你是专业播客编剧，擅长将内容转化为引人入胜的单人旁白脚本。

严格要求：输出且仅输出合法JSON，不要包含任何其他文字、注释或markdown标记。

JSON格式如下：
{
  "title": "播客标题",
  "characters": [{"id": 1, "name": "主播", "personality": "专业、亲和"}],
  "lines": [
    {"character_id": 1, "text": "台词内容", "emotion": "neutral/excited/serious/humorous", "pace": "normal/fast/slow"}
  ],
  "bgm_hints": [{"position": "intro/outro/transition", "style": "tech/casual/dramatic"}]
}

规则：
1. 台词必须口语化，像真实播客一样自然
2. 每段台词控制在50-150字
3. emotion可选：neutral, excited, serious, humorous
4. pace可选：normal, fast, slow
5. 必须包含开头引入和结尾总结
6. 适当插入语气词和连接词增强自然度
"""

DUO_SYSTEM = """你是专业播客编剧，擅长创作双人对话式播客脚本。

严格要求：输出且仅输出合法JSON，不要包含任何其他文字、注释或markdown标记。

JSON格式如下：
{
  "title": "播客标题",
  "characters": [
    {"id": 1, "name": "角色A", "personality": "性格描述"},
    {"id": 2, "name": "角色B", "personality": "性格描述"}
  ],
  "lines": [
    {"character_id": 1, "text": "台词内容", "emotion": "neutral/excited/serious/humorous", "pace": "normal/fast/slow"}
  ],
  "bgm_hints": [{"position": "intro/outro/transition", "style": "tech/casual/dramatic"}]
}

规则：
1. 两人交替发言，形成自然对话节奏
2. 台词口语化，包含互动和回应
3. 每段台词控制在30-100字
4. 两人风格互补（如：一个理性分析，一个感性吐槽）
5. 包含自然语气词："对"、"没错"、"我觉得"、"等等"等
6. 必须包含开头互动引入和结尾总结
7. emotion可选：neutral, excited, serious, humorous
8. pace可选：normal, fast, slow
"""

GROUP_SYSTEM = """你是专业播客编剧，擅长创作多人圆桌讨论式播客脚本。

严格要求：输出且仅输出合法JSON，不要包含任何其他文字、注释或markdown标记。

JSON格式如下：
{
  "title": "播客标题",
  "characters": [
    {"id": 1, "name": "主持人", "personality": "控场、引导话题"},
    {"id": 2, "name": "嘉宾A", "personality": "性格描述"},
    {"id": 3, "name": "嘉宾B", "personality": "性格描述"}
  ],
  "lines": [
    {"character_id": 1, "text": "台词内容", "emotion": "neutral/excited/serious/humorous", "pace": "normal/fast/slow"}
  ],
  "bgm_hints": [{"position": "intro/outro/transition", "style": "tech/casual/dramatic"}]
}

规则：
1. 主持人负责引导话题、总结观点、分配发言
2. 嘉宾各自表达不同角度的观点
3. 台词口语化，允许礼貌插话和回应
4. 每段台词控制在30-100字
5. 包含观点碰撞和共识达成的过程
6. 必须包含开头介绍嘉宾和结尾总结
7. emotion可选：neutral, excited, serious, humorous
8. pace可选：normal, fast, slow
"""

STYLE_GUIDES = {
    "casual": "风格轻松日常，像朋友聊天，多用口语和流行表达",
    "tech": "风格科技专业，术语准确但不晦涩，逻辑清晰",
    "dramatic": "风格戏剧张力强，用悬念和转折，情绪起伏大",
    "educational": "风格知识科普，深入浅出，善用比喻和案例",
}

TEMPLATES = {
    "solo": SOLO_SYSTEM,
    "duo": DUO_SYSTEM,
    "group": GROUP_SYSTEM,
}


def build_prompt(mode: str, style: str, source_text: str, characters: list[dict] | None = None, target_minutes: int = 5) -> str:
    """构建完整的 Prompt"""
    system = TEMPLATES.get(mode, DUO_SYSTEM)
    style_guide = STYLE_GUIDES.get(style, STYLE_GUIDES["casual"])

    char_instruction = ""
    if characters:
        char_names = [f"{c['name']}({c.get('personality', '')})" for c in characters]
        char_instruction = f"\n\n指定角色：{', '.join(char_names)}，请按照这些角色设定生成对话。"

    target_lines = target_minutes * 8  # 约8句/分钟

    user_prompt = f"""{style_guide}

素材内容：
{source_text}

要求：
- 生成约{target_lines}句台词的播客脚本
- 目标时长约{target_minutes}分钟
- 严格输出JSON格式{char_instruction}
"""
    return system, user_prompt
```

- [ ] **Step 3: 创建通义千问客户端 llm/qwen_client.py**

```python
# services/script/llm/qwen_client.py
"""通义千问 API 客户端 - 兼容 OpenAI 接口格式"""
import httpx
from shared.config import settings


class QwenClient:
    """通义千问API客户端，使用OpenAI兼容接口"""

    def __init__(self):
        self.api_key = settings.qwen_api_key
        self.base_url = settings.qwen_base_url
        self.model = settings.qwen_model
        self.client = httpx.AsyncClient(timeout=120.0)

    async def chat(self, system_prompt: str, user_prompt: str, model: str | None = None, temperature: float = 0.7, max_tokens: int = 8000) -> str:
        """调用通义千问 Chat API"""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": model or self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        response = await self.client.post(
            f"{self.base_url}/chat/completions",
            headers=headers,
            json=payload,
        )
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"]

    async def close(self):
        await self.client.aclose()
```

- [ ] **Step 4: 创建 JSON 修复模块 llm/json_fixer.py**

```python
# services/script/llm/json_fixer.py
"""JSON 修复与校验 - 处理 LLM 输出的格式问题"""
import re
import json
import json_repair
from shared.schemas import ScriptContentSchema, ScriptLineSchema


def extract_and_fix_json(raw_text: str) -> dict:
    """从LLM输出中提取并修复JSON"""
    # 尝试直接解析
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        pass

    # 尝试从markdown代码块中提取
    code_block_match = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", raw_text, re.DOTALL)
    if code_block_match:
        try:
            return json.loads(code_block_match.group(1).strip())
        except json.JSONDecodeError:
            pass

    # 使用 json-repair 修复
    try:
        return json_repair.loads(raw_text)
    except Exception:
        pass

    # 最后尝试：找到第一个 { 和最后一个 }
    start = raw_text.find("{")
    end = raw_text.rfind("}")
    if start != -1 and end != -1:
        try:
            return json_repair.loads(raw_text[start:end + 1])
        except Exception:
            pass

    raise ValueError(f"无法从LLM输出中提取有效JSON: {raw_text[:200]}...")


def validate_script_content(data: dict) -> ScriptContentSchema:
    """校验并标准化脚本内容"""
    # 确保必要字段存在
    if "title" not in data:
        data["title"] = "未命名播客"
    if "characters" not in data:
        data["characters"] = [{"id": 1, "name": "主播", "personality": "默认"}]
    if "lines" not in data or not data["lines"]:
        raise ValueError("脚本中没有台词")
    if "bgm_hints" not in data:
        data["bgm_hints"] = []

    # 标准化每行台词
    for line in data["lines"]:
        if "emotion" not in line:
            line["emotion"] = "neutral"
        if "pace" not in line:
            line["pace"] = "normal"
        if line["emotion"] not in ("neutral", "excited", "serious", "humorous"):
            line["emotion"] = "neutral"
        if line["pace"] not in ("normal", "fast", "slow"):
            line["pace"] = "normal"

    return ScriptContentSchema(**data)
```

- [ ] **Step 5: 创建文档解析器**

```python
# services/script/parser/pdf_parser.py
"""PDF 文档解析"""
from PyPDF2 import PdfReader
import io


async def parse_pdf(file_bytes: bytes) -> str:
    """从PDF文件中提取纯文本"""
    reader = PdfReader(io.BytesIO(file_bytes))
    texts = []
    for page in reader.pages:
        text = page.extract_text()
        if text:
            texts.append(text.strip())
    return "\n\n".join(texts)
```

```python
# services/script/parser/docx_parser.py
"""Word 文档解析"""
from docx import Document
import io


async def parse_docx(file_bytes: bytes) -> str:
    """从Word文件中提取纯文本"""
    doc = Document(io.BytesIO(file_bytes))
    texts = [para.text.strip() for para in doc.paragraphs if para.text.strip()]
    return "\n\n".join(texts)
```

```python
# services/script/parser/md_parser.py
"""Markdown 文档解析"""
import markdown
import re


async def parse_markdown(content: str) -> str:
    """从Markdown中提取纯文本（去除标记语法）"""
    # 去除图片、链接等
    text = re.sub(r"!\[.*?\]\(.*?\)", "", content)
    text = re.sub(r"\[(.*?)\]\(.*?\)", r"\1", text)
    text = re.sub(r"#{1,6}\s+", "", text)
    text = re.sub(r"[*_~`]", "", text)
    return text.strip()
```

```python
# services/script/parser/map_reduce.py
"""Map-Reduce 分块摘要处理长文档"""
from shared.config import settings
from services.script.llm.qwen_client import QwenClient

SUMMARY_PROMPT = """请对以下文本片段进行摘要，保留核心信息、关键论点和重要细节。输出简洁的中文摘要。

文本片段：
{chunk}"""

MERGE_PROMPT = """以下是多个文本片段的摘要，请将它们合并为一个连贯的总结，保留所有重要信息。

摘要列表：
{summaries}"""


def split_text(text: str, chunk_size: int = 3000, overlap: int = 300) -> list[str]:
    """将长文本分割为重叠的块"""
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        # 在句号/换行处断开
        if end < len(text):
            for sep in ["。", "\n", "！", "？", ".", " "]:
                last_sep = chunk.rfind(sep)
                if last_sep > chunk_size // 2:
                    chunk = chunk[:last_sep + 1]
                    end = start + last_sep + 1
                    break
        chunks.append(chunk.strip())
        start = end - overlap
    return chunks


async def map_reduce_summarize(text: str, llm: QwenClient) -> str:
    """Map-Reduce：先分块摘要，再合并"""
    chunks = split_text(text, settings.chunk_size, settings.chunk_overlap)

    if len(chunks) <= 1:
        return text

    # Map: 对每个块生成摘要
    summaries = []
    for chunk in chunks:
        prompt = SUMMARY_PROMPT.format(chunk=chunk)
        summary = await llm.chat("你是专业的文本摘要助手。", prompt, temperature=0.3, max_tokens=2000)
        summaries.append(summary)

    # Reduce: 合并所有摘要
    if len(summaries) == 1:
        return summaries[0]

    merged_text = "\n\n---\n\n".join(summaries)
    prompt = MERGE_PROMPT.format(summaries=merged_text)
    result = await llm.chat("你是专业的文本摘要助手。", prompt, temperature=0.3, max_tokens=4000)
    return result
```

```python
# services/script/parser/__init__.py
from services.script.parser.pdf_parser import parse_pdf
from services.script.parser.docx_parser import parse_docx
from services.script.parser.md_parser import parse_markdown
from services.script.parser.map_reduce import map_reduce_summarize, split_text

__all__ = ["parse_pdf", "parse_docx", "parse_markdown", "map_reduce_summarize", "split_text"]
```

- [ ] **Step 6: 创建脚本 API 路由 routers/script.py**

```python
# services/script/routers/script.py
"""脚本生成 API 路由"""
import uuid
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from shared.database import get_db
from shared.models import Project, Script, Character
from shared.schemas import ScriptGenerateRequest, ScriptGenerateFromDocRequest, ScriptResponse, ScriptUpdateRequest, CharacterCreate
from services.script.llm.qwen_client import QwenClient
from services.script.llm.prompt_templates import build_prompt
from services.script.llm.json_fixer import extract_and_fix_json, validate_script_content
from services.script.parser import parse_pdf, parse_docx, parse_markdown, map_reduce_summarize

router = APIRouter()


def get_llm() -> QwenClient:
    return QwenClient()


async def _generate_script_core(
    project_id: uuid.UUID,
    source_text: str,
    mode: str,
    style: str,
    target_minutes: int,
    characters: list[CharacterCreate] | None,
    db: AsyncSession,
    llm: QwenClient,
) -> Script:
    """脚本生成核心逻辑"""
    # 验证项目存在
    result = await db.execute(select(Project).where(Project.id == project_id))
    project = result.scalar_one_or_none()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    # 构建 Prompt
    char_dicts = [c.model_dump() for c in characters] if characters else None
    system_prompt, user_prompt = build_prompt(mode, style, source_text, char_dicts, target_minutes)

    # 调用 LLM
    raw_output = await llm.chat(system_prompt, user_prompt)

    # 修复并校验 JSON
    script_data = extract_and_fix_json(raw_output)
    content = validate_script_content(script_data)

    # 获取当前版本号
    version_result = await db.execute(
        select(Script).where(Script.project_id == project_id).order_by(Script.version.desc())
    )
    latest = version_result.scalars().first()
    version = (latest.version + 1) if latest else 1

    # 保存脚本
    script = Script(
        project_id=project_id,
        version=version,
        content=content.model_dump(),
        model=llm.model,
        prompt_template=mode,
        source_type="text",
        source_text=source_text[:5000],
    )
    db.add(script)
    await db.flush()
    await db.refresh(script)

    # 保存角色到项目
    if content.characters:
        # 清除旧角色
        old_chars = await db.execute(select(Character).where(Character.project_id == project_id))
        for old in old_chars.scalars().all():
            await db.delete(old)
        for i, char_data in enumerate(content.characters):
            char = Character(
                project_id=project_id,
                name=char_data.get("name", f"角色{i+1}"),
                personality=char_data.get("personality", ""),
                voice_id=char_data.get("voice_id"),
                order=i,
            )
            db.add(char)

    return script


@router.post("/generate", response_model=ScriptResponse)
async def generate_script(request: ScriptGenerateRequest, db: AsyncSession = Depends(get_db), llm: QwenClient = Depends(get_llm)):
    """从主题/文本生成播客脚本"""
    if not request.source_text:
        raise HTTPException(status_code=400, detail="source_text is required")

    # 长文本先 Map-Reduce 摘要
    source = request.source_text
    if len(source) >= 4000:
        source = await map_reduce_summarize(source, llm)

    script = await _generate_script_core(
        project_id=request.project_id,
        source_text=source,
        mode=request.mode,
        style=request.style,
        target_minutes=request.target_duration_minutes,
        characters=request.characters,
        db=db,
        llm=llm,
    )
    return script


@router.post("/generate-from-doc", response_model=ScriptResponse)
async def generate_script_from_doc(
    project_id: uuid.UUID = File(...),
    mode: str = File("duo"),
    style: str = File("casual"),
    target_duration_minutes: int = File(5),
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    llm: QwenClient = Depends(get_llm),
):
    """从上传文档生成播客脚本"""
    file_bytes = await file.read()
    filename = file.filename.lower()

    # 按文件类型解析
    if filename.endswith(".pdf"):
        text = await parse_pdf(file_bytes)
    elif filename.endswith((".docx", ".doc")):
        text = await parse_docx(file_bytes)
    elif filename.endswith((".md", ".markdown")):
        text = await parse_markdown(file_bytes.decode("utf-8"))
    elif filename.endswith(".txt"):
        text = file_bytes.decode("utf-8")
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {filename}")

    if not text.strip():
        raise HTTPException(status_code=400, detail="No text content extracted from document")

    # 长文档 Map-Reduce
    if len(text) >= 4000:
        text = await map_reduce_summarize(text, llm)

    script = await _generate_script_core(
        project_id=project_id,
        source_text=text,
        mode=mode,
        style=style,
        target_minutes=target_duration_minutes,
        characters=None,
        db=db,
        llm=llm,
    )
    return script


@router.get("/{project_id}/versions", response_model=list[ScriptResponse])
async def list_script_versions(project_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Script).where(Script.project_id == project_id).order_by(Script.version.desc()))
    return list(result.scalars().all())


@router.get("/detail/{script_id}", response_model=ScriptResponse)
async def get_script(script_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Script).where(Script.id == script_id))
    script = result.scalar_one_or_none()
    if not script:
        raise HTTPException(status_code=404, detail="Script not found")
    return script


@router.put("/detail/{script_id}", response_model=ScriptResponse)
async def update_script(script_id: uuid.UUID, data: ScriptUpdateRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Script).where(Script.id == script_id))
    script = result.scalar_one_or_none()
    if not script:
        raise HTTPException(status_code=404, detail="Script not found")
    script.content = data.content.model_dump()
    await db.flush()
    await db.refresh(script)
    return script
```

- [ ] **Step 7: 编写测试**

```python
# tests/test_script_service.py
"""Script Service 测试"""
import pytest
from services.script.llm.json_fixer import extract_and_fix_json, validate_script_content
from services.script.llm.prompt_templates import build_prompt
from services.script.parser.map_reduce import split_text


def test_extract_and_fix_json_valid():
    raw = '{"title": "Test", "characters": [{"id": 1, "name": "A"}], "lines": [{"character_id": 1, "text": "Hello", "emotion": "neutral", "pace": "normal"}], "bgm_hints": []}'
    result = extract_and_fix_json(raw)
    assert result["title"] == "Test"


def test_extract_and_fix_json_from_code_block():
    raw = '```json\n{"title": "Test", "characters": [], "lines": [{"character_id": 1, "text": "Hi", "emotion": "neutral", "pace": "normal"}], "bgm_hints": []}\n```'
    result = extract_and_fix_json(raw)
    assert result["title"] == "Test"


def test_validate_script_content():
    data = {
        "title": "Test",
        "characters": [{"id": 1, "name": "A", "personality": "test"}],
        "lines": [{"character_id": 1, "text": "Hello", "emotion": "neutral", "pace": "normal"}],
        "bgm_hints": [],
    }
    result = validate_script_content(data)
    assert result.title == "Test"


def test_build_prompt():
    system, user = build_prompt("duo", "tech", "AI发展趋势", target_minutes=5)
    assert "双人" in system
    assert "科技专业" in user
    assert "AI发展趋势" in user


def test_split_text():
    text = "这是一段很长的测试文本。" * 500
    chunks = split_text(text, chunk_size=500, overlap=50)
    assert len(chunks) > 1
    assert all(len(c) <= 600 for c in chunks)  # 允许一定超出
```

- [ ] **Step 8: 运行测试**

```bash
cd /workspace/podcast-app
pytest tests/test_script_service.py -v
```

Expected: 5 tests pass

- [ ] **Step 9: Commit**

```bash
git add podcast-app/services/script/ podcast-app/tests/test_script_service.py
git commit -m "feat: implement Script Service with LLM script generation, document parsing, and Map-Reduce"
```

---

## Task 5: Audio Service（音频服务）

**Files:**
- Create: `podcast-app/services/audio/main.py`
- Create: `podcast-app/services/audio/voicebox/client.py`
- Create: `podcast-app/services/audio/voicebox/multi_node.py`
- Create: `podcast-app/services/audio/pipeline/effects.py`
- Create: `podcast-app/services/audio/pipeline/mixing.py`
- Create: `podcast-app/services/audio/pipeline/loudness.py`
- Create: `podcast-app/services/audio/routers/audio.py`
- Test: `podcast-app/tests/test_audio_service.py`

- [ ] **Step 1: 创建 Audio Service 入口 main.py**

```python
# services/audio/main.py
"""Audio Service - 语音生成与后期处理"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from shared.config import settings
from services.audio.routers.audio import router as audio_router

app = FastAPI(title="Podcast Audio Service", version="0.1.0")

# 挂载静态文件用于音频下载
import os
os.makedirs(settings.storage_path, exist_ok=True)

app.include_router(audio_router, prefix="/api/audio", tags=["audio"])


@app.get("/health")
async def health():
    return {"status": "ok", "service": "audio"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=settings.audio_service_port)
```

- [ ] **Step 2: 创建 Voicebox API 客户端 voicebox/client.py**

```python
# services/audio/voicebox/client.py
"""Voicebox REST API 客户端 - 单节点实现"""
import httpx
from shared.config import settings


class VoiceboxClient:
    """Voicebox REST API 封装

    Voicebox API 端点（FastAPI，端口17493）：
    - POST /generate — TTS生成
    - GET /profiles — 列出音色
    - POST /profiles — 创建音色
    - GET /models — 列出可用模型
    - POST /effects/apply — 应用音频效果
    - GET /health — 健康检查
    """

    def __init__(self, base_url: str | None = None):
        self.base_url = base_url or settings.voicebox_url
        self.client = httpx.AsyncClient(timeout=300.0)  # TTS生成可能需要较长时间

    async def health_check(self) -> bool:
        """检查Voicebox服务是否可用"""
        try:
            resp = await self.client.get(f"{self.base_url}/health")
            return resp.status_code == 200
        except Exception:
            return False

    async def list_models(self) -> list[dict]:
        """列出可用TTS模型"""
        resp = await self.client.get(f"{self.base_url}/models")
        resp.raise_for_status()
        return resp.json()

    async def list_profiles(self) -> list[dict]:
        """列出所有音色档案"""
        resp = await self.client.get(f"{self.base_url}/profiles")
        resp.raise_for_status()
        return resp.json()

    async def get_profile(self, profile_id: str) -> dict:
        """获取音色档案详情"""
        resp = await self.client.get(f"{self.base_url}/profiles/{profile_id}")
        resp.raise_for_status()
        return resp.json()

    async def create_profile(self, name: str, sample_files: list[str] | None = None, language: str = "zh") -> dict:
        """创建音色档案（零样本克隆）"""
        payload = {"name": name, "language": language}
        if sample_files:
            payload["sample_files"] = sample_files
        resp = await self.client.post(f"{self.base_url}/profiles", json=payload)
        resp.raise_for_status()
        return resp.json()

    async def generate_tts(
        self,
        text: str,
        profile_id: str,
        engine: str = "qwen3-tts-0.6b",
        language: str = "zh",
        temperature: float = 1.0,
        speed: float = 1.0,
    ) -> bytes:
        """生成TTS音频

        Args:
            text: 要合成的文本
            profile_id: Voicebox音色档案ID
            engine: TTS引擎 (qwen3-tts-0.6b / qwen3-tts-1.7b / luxtts / chatterbox / chatterbox_turbo / tada / kokoro)
            language: 语言代码
            temperature: 生成温度
            speed: 语速

        Returns:
            音频文件二进制数据 (WAV格式)
        """
        payload = {
            "text": text,
            "profile_id": profile_id,
            "engine": engine,
            "language": language,
            "temperature": temperature,
            "speed": speed,
        }
        resp = await self.client.post(f"{self.base_url}/generate", json=payload)
        resp.raise_for_status()
        return resp.content

    async def generate_tts_with_speaker(
        self,
        text: str,
        speaker: str,
        engine: str = "qwen-custom-voice",
        language: str = "zh",
        instruct: str = "",
    ) -> bytes:
        """使用预设音色生成TTS（Qwen CustomVoice引擎）

        Args:
            text: 要合成的文本
            speaker: 预设音色名称 (如 Vivian, Serena, Ryan)
            engine: 引擎名称
            language: 语言代码
            instruct: 自然语言风格指令 (如 "说得更慢一些")
        """
        payload = {
            "text": text,
            "speaker": speaker,
            "engine": engine,
            "language": language,
        }
        if instruct:
            payload["instruct"] = instruct
        resp = await self.client.post(f"{self.base_url}/generate", json=payload)
        resp.raise_for_status()
        return resp.content

    async def apply_effects(self, audio_data: bytes, effects: list[dict]) -> bytes:
        """应用音频效果

        Args:
            audio_data: 输入音频数据
            effects: 效果链列表，如 [{"type": "reverb", "params": {"room_size": 0.5}}]
        """
        # 先保存为临时文件再上传
        import tempfile
        import os
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
            f.write(audio_data)
            temp_path = f.name

        try:
            with open(temp_path, "rb") as f:
                files = {"audio": ("audio.wav", f, "audio/wav")}
                data = {"effects": str(effects)}
                resp = await self.client.post(f"{self.base_url}/effects/apply", files=files, data=data)
            resp.raise_for_status()
            return resp.content
        finally:
            os.unlink(temp_path)

    async def speak(self, text: str, profile_id: str | None = None, engine: str = "qwen3-tts-0.6b") -> bytes:
        """Agent播报接口（简化版TTS）"""
        payload = {"text": text, "engine": engine}
        if profile_id:
            payload["profile_id"] = profile_id
        resp = await self.client.post(f"{self.base_url}/speak", json=payload)
        resp.raise_for_status()
        return resp.content

    async def close(self):
        await self.client.aclose()
```

- [ ] **Step 3: 创建多节点调度器 voicebox/multi_node.py**

```python
# services/audio/voicebox/multi_node.py
"""多节点 Voicebox 调度器（方案二）"""
import random
from services.audio.voicebox.client import VoiceboxClient


class MultiNodeScheduler:
    """多算力节点负载均衡调度器"""

    def __init__(self, node_urls: list[str]):
        self.nodes: dict[str, VoiceboxClient] = {
            url: VoiceboxClient(base_url=url) for url in node_urls
        }
        self.node_health: dict[str, bool] = {url: True for url in node_urls}

    async def check_health(self):
        """检查所有节点健康状态"""
        for url, client in self.nodes.items():
            self.node_health[url] = await client.health_check()

    def get_available_node(self) -> VoiceboxClient | None:
        """获取一个可用节点（简单随机策略）"""
        available = [url for url, healthy in self.node_health.items() if healthy]
        if not available:
            return None
        return self.nodes[random.choice(available)]

    async def generate_tts(self, **kwargs) -> bytes:
        """在可用节点上生成TTS"""
        node = self.get_available_node()
        if not node:
            raise RuntimeError("No available Voicebox nodes")
        return await node.generate_tts(**kwargs)

    async def close_all(self):
        for client in self.nodes.values():
            await client.close()
```

- [ ] **Step 4: 创建音频效果处理 pipeline/effects.py**

```python
# services/audio/pipeline/effects.py
"""音频效果处理 - 基于 pedalboard"""
import numpy as np
import soundfile as sf
import io
from pedalboard import Pedalboard, Reverb, Compressor, Gain, LowShelfFilter, HighShelfFilter, Chorus, Delay


def apply_pedalboard_effects(audio_data: bytes, effects_config: list[dict]) -> bytes:
    """使用 pedalboard 应用音频效果链

    Args:
        audio_data: WAV格式音频数据
        effects_config: 效果配置列表，如 [{"type": "reverb", "room_size": 0.3}, {"type": "compressor", "threshold_db": -20}]

    Returns:
        处理后的WAV音频数据
    """
    # 读取音频
    audio_buffer = io.BytesIO(audio_data)
    audio, sample_rate = sf.read(audio_buffer)

    # 构建效果链
    plugins = []
    for effect in effects_config:
        effect_type = effect.get("type", "")
        params = {k: v for k, v in effect.items() if k != "type"}

        if effect_type == "reverb":
            plugins.append(Reverb(room_size=params.get("room_size", 0.5), damping=params.get("damping", 0.5), wet_level=params.get("wet_level", 0.3), dry_level=params.get("dry_level", 0.7)))
        elif effect_type == "compressor":
            plugins.append(Compressor(threshold_db=params.get("threshold_db", -20), ratio=params.get("ratio", 4), attack_ms=params.get("attack_ms", 10), release_ms=params.get("release_ms", 100)))
        elif effect_type == "gain":
            plugins.append(Gain(gain_db=params.get("gain_db", 0)))
        elif effect_type == "low_shelf":
            plugins.append(LowShelfFilter(cutoff_frequency_hz=params.get("cutoff_frequency_hz", 300), gain_db=params.get("gain_db", 0)))
        elif effect_type == "high_shelf":
            plugins.append(HighShelfFilter(cutoff_frequency_hz=params.get("cutoff_frequency_hz", 3000), gain_db=params.get("gain_db", 0)))
        elif effect_type == "chorus":
            plugins.append(Chorus(rate_hz=params.get("rate_hz", 1.5), depth=params.get("depth", 0.5), centre_delay_ms=params.get("centre_delay_ms", 7), feedback=params.get("feedback", 0.5), mix=params.get("mix", 0.5)))
        elif effect_type == "delay":
            plugins.append(Delay(delay_seconds=params.get("delay_seconds", 0.3), feedback=params.get("feedback", 0.3), mix=params.get("mix", 0.3)))

    if not plugins:
        return audio_data

    # 应用效果
    board = Pedalboard(plugins)
    # 确保音频是2D数组 (channels, samples)
    if audio.ndim == 1:
        audio = audio.reshape(1, -1)
    processed = board(audio, sample_rate)

    # 写回WAV
    output_buffer = io.BytesIO()
    sf.write(output_buffer, processed.T, sample_rate, format="WAV")
    return output_buffer.getvalue()


# 预设效果链
PRESET_EFFECTS = {
    "podcast_warm": [
        {"type": "compressor", "threshold_db": -18, "ratio": 3},
        {"type": "reverb", "room_size": 0.2, "wet_level": 0.1},
        {"type": "gain", "gain_db": 2},
    ],
    "podcast_clean": [
        {"type": "compressor", "threshold_db": -20, "ratio": 4},
        {"type": "high_shelf", "cutoff_frequency_hz": 3000, "gain_db": -2},
    ],
    "podcast_radio": [
        {"type": "compressor", "threshold_db": -15, "ratio": 5},
        {"type": "gain", "gain_db": 3},
        {"type": "reverb", "room_size": 0.15, "wet_level": 0.08},
    ],
}
```

- [ ] **Step 5: 创建BGM混音与侧链压缩 pipeline/mixing.py**

```python
# services/audio/pipeline/mixing.py
"""BGM混音与侧链压缩"""
import numpy as np
import soundfile as sf
import io


def mix_voice_with_bgm(
    voice_audio: bytes,
    bgm_audio: bytes,
    bgm_volume: float = 0.15,
    sidechain_threshold: float = 0.1,
    sidechain_reduction: float = 0.7,
    fade_in_seconds: float = 1.0,
    fade_out_seconds: float = 2.0,
    sample_rate: int = 44100,
) -> bytes:
    """将人声与BGM混合，带侧链压缩效果

    Args:
        voice_audio: 人声音频数据 (WAV)
        bgm_audio: BGM音频数据 (WAV)
        bgm_volume: BGM基础音量 (0-1)
        sidechain_threshold: 侧链触发阈值
        sidechain_reduction: 侧链压缩量 (0-1, 1=完全静音)
        fade_in_seconds: 片头淡入时长
        fade_out_seconds: 片尾淡出时长
        sample_rate: 采样率

    Returns:
        混音后的WAV音频数据
    """
    # 读取音频
    voice_buf = io.BytesIO(voice_audio)
    voice, voice_sr = sf.read(voice_buf)

    bgm_buf = io.BytesIO(bgm_audio)
    bgm, bgm_sr = sf.read(bgm_buf)

    # 重采样BGM到与人声一致的采样率
    if bgm_sr != voice_sr:
        from scipy import signal
        num_samples = int(len(bgm) * voice_sr / bgm_sr)
        bgm = signal.resample(bgm, num_samples)

    sample_rate = voice_sr

    # 确保单声道
    if voice.ndim > 1:
        voice = voice.mean(axis=1)
    if bgm.ndim > 1:
        bgm = bgm.mean(axis=1)

    # BGM循环或截断到人声长度
    target_len = len(voice)
    if len(bgm) < target_len:
        # 循环BGM
        repeats = (target_len // len(bgm)) + 1
        bgm = np.tile(bgm, repeats)[:target_len]
    else:
        bgm = bgm[:target_len]

    # 侧链压缩：当人声能量超过阈值时，降低BGM音量
    frame_size = int(sample_rate * 0.02)  # 20ms帧
    bgm_gains = np.ones(target_len)

    for i in range(0, target_len - frame_size, frame_size):
        voice_rms = np.sqrt(np.mean(voice[i:i + frame_size] ** 2))
        if voice_rms > sidechain_threshold:
            # 人声活跃时降低BGM
            bgm_gains[i:i + frame_size] = 1.0 - sidechain_reduction

    bgm_processed = bgm * bgm_volume * bgm_gains

    # 片头淡入
    fade_in_samples = int(fade_in_seconds * sample_rate)
    if fade_in_samples > 0 and fade_in_samples < target_len:
        fade_in = np.linspace(0, 1, fade_in_samples)
        bgm_processed[:fade_in_samples] *= fade_in
        voice[:fade_in_samples] *= fade_in

    # 片尾淡出
    fade_out_samples = int(fade_out_seconds * sample_rate)
    if fade_out_samples > 0 and fade_out_samples < target_len:
        fade_out = np.linspace(1, 0, fade_out_samples)
        bgm_processed[-fade_out_samples:] *= fade_out
        voice[-fade_out_samples:] *= fade_out

    # 混合
    mixed = voice + bgm_processed

    # 防止削波
    max_val = np.max(np.abs(mixed))
    if max_val > 0.95:
        mixed = mixed * (0.95 / max_val)

    # 写回WAV
    output_buf = io.BytesIO()
    sf.write(output_buf, mixed, sample_rate, format="WAV")
    return output_buf.getvalue()


def insert_silence(audio_segments: list[bytes], min_pause: float = 0.5, max_pause: float = 1.5, sample_rate: int = 44100) -> bytes:
    """在音频片段之间插入随机时长的静音停顿

    Args:
        audio_segments: 有序的音频片段列表 (WAV格式)
        min_pause: 最小停顿秒数
        max_pause: 最大停顿秒数
        sample_rate: 采样率

    Returns:
        拼接后的WAV音频数据
    """
    import random

    result_samples = np.array([], dtype=np.float64)

    for i, segment_data in enumerate(audio_segments):
        # 读取片段
        buf = io.BytesIO(segment_data)
        audio, sr = sf.read(buf)
        if audio.ndim > 1:
            audio = audio.mean(axis=1)

        # 添加到结果
        result_samples = np.concatenate([result_samples, audio])

        # 非最后一段时插入静音
        if i < len(audio_segments) - 1:
            pause_duration = random.uniform(min_pause, max_pause)
            silence_samples = np.zeros(int(pause_duration * sr))
            result_samples = np.concatenate([result_samples, silence_samples])

    # 写回WAV
    output_buf = io.BytesIO()
    sf.write(output_buf, result_samples, sample_rate, format="WAV")
    return output_buf.getvalue()
```

- [ ] **Step 6: 创建响度标准化 pipeline/loudness.py**

```python
# services/audio/pipeline/loudness.py
"""响度标准化 - 使用 FFmpeg loudnorm 滤镜"""
import subprocess
import io
import tempfile
import os
from shared.config import settings


def normalize_loudness(audio_data: bytes, target_loudness: float = -16.0, input_format: str = "wav", output_format: str = "mp3") -> bytes:
    """使用 FFmpeg loudnorm 进行响度标准化

    Args:
        audio_data: 输入音频数据
        target_loudness: 目标响度 (LUFS), 播客推荐 -16 LUFS
        input_format: 输入格式
        output_format: 输出格式

    Returns:
        标准化后的音频数据
    """
    with tempfile.NamedTemporaryFile(suffix=f".{input_format}", delete=False) as in_file:
        in_file.write(audio_data)
        in_path = in_file.name

    out_path = in_path.replace(f".{input_format}", f"_normalized.{output_format}")

    try:
        # 两遍处理：第一遍分析，第二遍标准化
        # 第一遍：分析响度
        cmd_analyze = [
            "ffmpeg", "-i", in_path,
            "-af", f"loudnorm=I={target_loudness}:TP=-1.5:LRA=11:print_format=json",
            "-f", "null", "-"
        ]
        result = subprocess.run(cmd_analyze, capture_output=True, text=True)

        # 解析分析结果
        stats = _parse_loudnorm_stats(result.stderr)

        if stats:
            # 第二遍：精确标准化
            cmd_normalize = [
                "ffmpeg", "-i", in_path,
                "-af", (
                    f"loudnorm=I={target_loudness}:TP=-1.5:LRA=11"
                    f":measured_I={stats['input_i']}"
                    f":measured_TP={stats['input_tp']}"
                    f":measured_LRA={stats['input_lra']}"
                    f":measured_thresh={stats['input_thresh']}"
                    f":offset={stats['target_offset']}"
                    f":linear=true"
                ),
                "-ar", "44100",
                "-y", out_path,
            ]
        else:
            # 降级为单遍标准化
            cmd_normalize = [
                "ffmpeg", "-i", in_path,
                "-af", f"loudnorm=I={target_loudness}:TP=-1.5:LRA=11",
                "-ar", "44100",
                "-y", out_path,
            ]

        subprocess.run(cmd_normalize, capture_output=True, check=True)

        with open(out_path, "rb") as f:
            return f.read()

    finally:
        for path in [in_path, out_path]:
            if os.path.exists(path):
                os.unlink(path)


def _parse_loudnorm_stats(stderr_output: str) -> dict | None:
    """从 FFmpeg stderr 输出中解析 loudnorm 分析结果"""
    import json
    import re

    match = re.search(r"\{[^}]*\"input_i\"[^}]*\}", stderr_output, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return None


def convert_format(audio_data: bytes, output_format: str = "mp3", bitrate: str = "192k") -> bytes:
    """音频格式转换"""
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as in_file:
        in_file.write(audio_data)
        in_path = in_file.name

    out_path = in_path.replace(".wav", f".{output_format}")

    try:
        cmd = [
            "ffmpeg", "-i", in_path,
            "-b:a", bitrate,
            "-y", out_path,
        ]
        subprocess.run(cmd, capture_output=True, check=True)

        with open(out_path, "rb") as f:
            return f.read()
    finally:
        for path in [in_path, out_path]:
            if os.path.exists(path):
                os.unlink(path)
```

- [ ] **Step 7: 创建音频 API 路由 routers/audio.py**

```python
# services/audio/routers/audio.py
"""音频生成与处理 API 路由"""
import uuid
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from shared.database import get_db
from shared.models import AudioTask, AudioFile, Script, Character, BgmLibrary
from shared.schemas import AudioGenerateRequest, AudioTaskResponse, BgmCreateRequest, BgmResponse, GpuStatusResponse
from shared.storage import get_storage
from services.audio.voicebox.client import VoiceboxClient
from services.audio.pipeline.effects import apply_pedalboard_effects, PRESET_EFFECTS
from services.audio.pipeline.mixing import mix_voice_with_bgm, insert_silence
from services.audio.pipeline.loudness import normalize_loudness, convert_format
from shared.config import settings

router = APIRouter()


def get_voicebox() -> VoiceboxClient:
    return VoiceboxClient()


@router.post("/generate", response_model=AudioTaskResponse)
async def generate_audio(request: AudioGenerateRequest, db: AsyncSession = Depends(get_db), voicebox: VoiceboxClient = Depends(get_voicebox)):
    """提交音频生成任务（同步执行，后续由Task Service异步调度）"""
    # 验证脚本
    result = await db.execute(select(Script).where(Script.id == request.script_id))
    script = result.scalar_one_or_none()
    if not script:
        raise HTTPException(status_code=404, detail="Script not found")

    # 创建音频任务
    task = AudioTask(
        project_id=request.project_id,
        script_id=request.script_id,
        status="processing",
        config={"model": request.model, "apply_effects": request.apply_effects, "bgm_style": request.bgm_style, "output_format": request.output_format},
    )
    db.add(task)
    await db.flush()
    await db.refresh(task)

    try:
        content = script.content
        lines = content.get("lines", [])

        # 获取角色信息
        char_result = await db.execute(select(Character).where(Character.project_id == request.project_id))
        characters = {str(c.id): c for c in char_result.scalars().all()}
        # 也用角色名匹配
        char_by_name = {c.name: c for c in char_result.scalars().all()}

        # 按角色分组生成干音
        voice_segments = []
        for line in lines:
            char_id = line.get("character_id")
            text = line.get("text", "")
            if not text.strip():
                continue

            # 查找对应音色
            profile_id = None
            for char in characters.values():
                if str(char.order + 1) == str(char_id) or char.name == content.get("characters", [{}])[char_id - 1].get("name", "") if char_id <= len(content.get("characters", [])) else False:
                    profile_id = char.voice_id
                    break

            # 生成TTS
            if profile_id:
                audio_bytes = await voicebox.generate_tts(text, profile_id=profile_id, engine=request.model)
            else:
                # 使用默认预设音色
                audio_bytes = await voicebox.generate_tts(text, profile_id="default", engine=request.model)

            # 应用效果
            if request.apply_effects:
                effects = PRESET_EFFECTS.get("podcast_warm", [])
                audio_bytes = apply_pedalboard_effects(audio_bytes, effects)

            voice_segments.append(audio_bytes)

            # 保存干音文件
            storage = get_storage()
            file_path = f"audio/{task.id}/dry_{len(voice_segments)}.wav"
            await storage.save(file_path, audio_bytes)

            audio_file = AudioFile(
                audio_task_id=task.id,
                character_id=None,
                file_path=file_path,
                file_type="dry_voice",
                format="wav",
            )
            db.add(audio_file)

        # 拼接干音（插入停顿）
        if voice_segments:
            merged = insert_silence(voice_segments)

            # BGM混音
            if request.bgm_style:
                bgm_result = await db.execute(select(BgmLibrary).where(BgmLibrary.style == request.bgm_style).limit(1))
                bgm = bgm_result.scalar_one_or_none()
                if bgm:
                    storage = get_storage()
                    bgm_data = await storage.read(bgm.file_path)
                    merged = mix_voice_with_bgm(merged, bgm_data)

            # 响度标准化
            merged = normalize_loudness(merged, target_loudness=settings.target_loudness, output_format=request.output_format)

            # 保存最终文件
            storage = get_storage()
            final_path = f"audio/{task.id}/final_mix.{request.output_format}"
            await storage.save(final_path, merged)

            final_file = AudioFile(
                audio_task_id=task.id,
                file_path=final_path,
                file_type="final_mix",
                format=request.output_format,
            )
            db.add(final_file)

        task.status = "completed"
        task.completed_at = datetime.utcnow()

    except Exception as e:
        task.status = "failed"
        task.error = str(e)

    await db.flush()
    await db.refresh(task)
    return task


@router.get("/task/{task_id}", response_model=AudioTaskResponse)
async def get_audio_task(task_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(AudioTask).where(AudioTask.id == task_id))
    task = result.scalar_one_or_none()
    if not task:
        raise HTTPException(status_code=404, detail="Audio task not found")
    return task


@router.get("/download/{file_id}")
async def download_audio(file_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    from fastapi.responses import FileResponse
    result = await db.execute(select(AudioFile).where(AudioFile.id == file_id))
    audio_file = result.scalar_one_or_none()
    if not audio_file:
        raise HTTPException(status_code=404, detail="Audio file not found")
    storage = get_storage()
    full_path = str(storage.base_path / audio_file.file_path) if hasattr(storage, "base_path") else audio_file.file_path
    import os
    if not os.path.exists(full_path):
        raise HTTPException(status_code=404, detail="File not found on disk")
    return FileResponse(full_path, media_type=f"audio/{audio_file.format}", filename=os.path.basename(full_path))


@router.get("/bgm/library", response_model=list[BgmResponse])
async def list_bgm(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(BgmLibrary))
    return list(result.scalars().all())


@router.post("/bgm/library", response_model=BgmResponse)
async def add_bgm(data: BgmCreateRequest, db: AsyncSession = Depends(get_db)):
    bgm = BgmLibrary(name=data.name, style=data.style, file_path=f"bgm/{data.style}/{data.name}.mp3")
    db.add(bgm)
    await db.flush()
    await db.refresh(bgm)
    return bgm


@router.get("/gpu-status", response_model=GpuStatusResponse)
async def gpu_status(voicebox: VoiceboxClient = Depends(get_voicebox)):
    """通过Voicebox健康检查间接获取GPU状态"""
    available = await voicebox.health_check()
    return GpuStatusResponse(available=available, gpu_name=None, vram_total_mb=None, vram_used_mb=None, vram_free_mb=None, gpu_utilization=None)
```

- [ ] **Step 8: Commit**

```bash
git add podcast-app/services/audio/ podcast-app/tests/test_audio_service.py
git commit -m "feat: implement Audio Service with Voicebox client, effects pipeline, BGM mixing, and loudness normalization"
```

---

## Task 6: Task Service（任务调度服务）

**Files:**
- Create: `podcast-app/services/task/main.py`
- Create: `podcast-app/services/task/celery_app.py`
- Create: `podcast-app/services/task/tasks/podcast_task.py`
- Create: `podcast-app/services/task/gpu_monitor.py`
- Create: `podcast-app/services/task/routers/task.py`

- [ ] **Step 1: 创建 Celery 配置 celery_app.py**

```python
# services/task/celery_app.py
"""Celery 异步任务配置"""
from celery import Celery
from shared.config import settings

celery_app = Celery(
    "podcast_tasks",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_soft_time_limit=1800,  # 30分钟软超时
    task_time_limit=2100,  # 35分钟硬超时
    task_routes={
        "services.task.tasks.podcast_task.generate_podcast": {"queue": "podcast"},
    },
)

# 自动发现任务
celery_app.autodiscover_tasks(["services.task.tasks"])
```

- [ ] **Step 2: 创建播客生成全流程任务 tasks/podcast_task.py**

```python
# services/task/tasks/podcast_task.py
"""播客生成全流程 Celery 任务"""
import asyncio
import httpx
import json
import redis
from celery import shared_task
from shared.config import settings
from shared.events import TaskEvent, STEP_WEIGHTS

redis_client = redis.from_url(settings.redis_url)


def publish_event(event: TaskEvent):
    """发布SSE事件到Redis频道"""
    redis_client.publish(f"task:{event.task_id}", event.to_sse())


def update_progress(task_id: str, step: str, step_progress: float, status: str = "progress", message: str = ""):
    """计算并发布总进度"""
    # 获取已完成步骤的权重
    steps_order = ["script_generation", "dry_audio", "post_processing", "finalize"]
    completed_weight = sum(STEP_WEIGHTS[s] for s in steps_order[:steps_order.index(step)])
    current_weight = STEP_WEIGHTS[step] * step_progress
    total_progress = completed_weight + current_weight

    event = TaskEvent(
        task_id=task_id,
        step=step,
        status=status,
        progress=round(total_progress, 3),
        message=message,
    )
    publish_event(event)


async def _call_script_service(endpoint: str, data: dict) -> dict:
    """调用 Script Service"""
    async with httpx.AsyncClient(timeout=120.0) as client:
        url = f"{settings.script_service_url}{endpoint}"
        response = await client.post(url, json=data)
        response.raise_for_status()
        return response.json()


async def _call_audio_service(endpoint: str, data: dict) -> dict:
    """调用 Audio Service"""
    async with httpx.AsyncClient(timeout=300.0) as client:
        url = f"{settings.audio_service_url}{endpoint}"
        response = await client.post(url, json=data)
        response.raise_for_status()
        return response.json()


async def _run_podcast_generation(task_id: str, config: dict):
    """播客生成全流程"""
    try:
        # Step 1: 生成脚本
        update_progress(task_id, "script_generation", 0.0, "started", "开始生成脚本")
        script_data = await _call_script_service("/api/script/generate", {
            "project_id": config["project_id"],
            "source_type": config.get("source_type", "topic"),
            "source_text": config.get("source_text", ""),
            "mode": config.get("mode", "duo"),
            "characters": config.get("characters"),
            "style": config.get("style", "casual"),
            "target_duration_minutes": config.get("target_duration_minutes", 5),
        })
        update_progress(task_id, "script_generation", 1.0, "completed", "脚本生成完成")

        script_id = script_data["id"]

        # Step 2: 生成音频
        update_progress(task_id, "dry_audio", 0.0, "started", "开始生成音频")
        audio_data = await _call_audio_service("/api/audio/generate", {
            "project_id": config["project_id"],
            "script_id": script_id,
            "model": config.get("tts_model", "qwen3-tts-0.6b"),
            "apply_effects": True,
            "bgm_style": config.get("bgm_style"),
            "output_format": config.get("output_format", "mp3"),
        })
        update_progress(task_id, "dry_audio", 1.0, "completed", "音频生成完成")

        # Step 3: 后期处理（已在Audio Service中完成）
        update_progress(task_id, "post_processing", 1.0, "completed", "后期处理完成")

        # Step 4: 完成
        update_progress(task_id, "finalize", 1.0, "completed", "播客生成完成")

    except Exception as e:
        event = TaskEvent(
            task_id=task_id,
            step="unknown",
            status="failed",
            message=str(e),
        )
        publish_event(event)
        raise


@shared_task(bind=True, max_retries=3, default_retry_delay=30)
def generate_podcast(self, task_id: str, config: dict):
    """播客生成全流程任务（Celery Task）"""
    asyncio.run(_run_podcast_generation(task_id, config))
    return {"task_id": task_id, "status": "completed"}
```

- [ ] **Step 3: 创建GPU监控 gpu_monitor.py**

```python
# services/task/gpu_monitor.py
"""GPU状态监控 - 通过Voicebox健康检查间接获取"""
import httpx
from shared.config import settings


async def get_gpu_status() -> dict:
    """获取GPU状态信息"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # 尝试从Voicebox获取详细硬件信息
            response = await client.get(f"{settings.voicebox_url}/health")
            if response.status_code == 200:
                data = response.json()
                return {
                    "available": True,
                    "gpu_name": data.get("gpu_name"),
                    "vram_total_mb": data.get("vram_total_mb"),
                    "vram_used_mb": data.get("vram_used_mb"),
                    "vram_free_mb": data.get("vram_free_mb"),
                    "gpu_utilization": data.get("gpu_utilization"),
                }
    except Exception:
        pass

    return {"available": False, "gpu_name": None, "vram_total_mb": None, "vram_used_mb": None, "vram_free_mb": None, "gpu_utilization": None}
```

- [ ] **Step 4: 创建任务 API 路由 routers/task.py**

```python
# services/task/routers/task.py
"""任务调度 API 路由"""
import uuid
import json
import redis
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from shared.config import settings
from shared.schemas import PodcastTaskRequest, TaskStatusResponse, GpuStatusResponse
from services.task.tasks.podcast_task import generate_podcast
from services.task.gpu_monitor import get_gpu_status

router = APIRouter()
redis_client = redis.from_url(settings.redis_url)


@router.post("/podcast")
async def submit_podcast_task(request: PodcastTaskRequest):
    """提交播客生成全流程任务"""
    task_id = str(uuid.uuid4())
    config = {
        "project_id": str(request.project_id),
        "source_type": request.source_type,
        "source_text": request.source_text,
        "mode": request.mode,
        "characters": [c.model_dump() for c in request.characters] if request.characters else None,
        "style": request.style,
        "target_duration_minutes": request.target_duration_minutes,
        "tts_model": request.tts_model,
        "bgm_style": request.bgm_style,
        "output_format": request.output_format,
    }

    # 提交 Celery 任务
    celery_task = generate_podcast.delay(task_id, config)

    return {"task_id": task_id, "celery_task_id": celery_task.id, "status": "submitted"}


@router.get("/{task_id}/status")
async def get_task_status(task_id: str):
    """查询任务状态"""
    # 从Redis频道读取最新事件
    event_data = redis_client.get(f"task_status:{task_id}")
    if event_data:
        return json.loads(event_data)
    return {"task_id": task_id, "status": "unknown", "progress": 0.0}


@router.get("/{task_id}/events")
async def task_events(task_id: str):
    """SSE 进度事件流"""
    pubsub = redis_client.pubsub()
    pubsub.subscribe(f"task:{task_id}")

    def event_generator():
        for message in pubsub.listen():
            if message["type"] == "message":
                data = message["data"]
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                yield data
                # 如果是完成或失败事件，关闭流
                if '"completed"' in data or '"failed"' in data:
                    break

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.post("/{task_id}/cancel")
async def cancel_task(task_id: str):
    """取消任务"""
    # 通过Celery撤销任务
    from services.task.celery_app import celery_app
    celery_app.control.revoke(task_id, terminate=True)
    return {"task_id": task_id, "status": "cancelled"}


@router.get("/gpu-status", response_model=GpuStatusResponse)
async def gpu_status():
    """查询GPU状态"""
    status = await get_gpu_status()
    return GpuStatusResponse(**status)
```

- [ ] **Step 5: 创建 Task Service 入口 main.py**

```python
# services/task/main.py
"""Task Service - 任务调度服务"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from fastapi import FastAPI
from shared.config import settings
from services.task.routers.task import router as task_router

app = FastAPI(title="Podcast Task Service", version="0.1.0")

app.include_router(task_router, prefix="/api/task", tags=["task"])


@app.get("/health")
async def health():
    return {"status": "ok", "service": "task"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=settings.task_service_port)
```

- [ ] **Step 6: Commit**

```bash
git add podcast-app/services/task/
git commit -m "feat: implement Task Service with Celery async task orchestration and SSE progress"
```

---

## Task 7: API Gateway

**Files:**
- Create: `podcast-app/services/gateway/main.py`
- Create: `podcast-app/services/gateway/auth/api_key.py`
- Create: `podcast-app/services/gateway/middleware/rate_limit.py`
- Create: `podcast-app/services/gateway/middleware/cors.py`
- Create: `podcast-app/services/gateway/router.py`

- [ ] **Step 1: 创建认证模块 auth/api_key.py**

```python
# services/gateway/auth/api_key.py
"""API Key 认证（方案一）"""
from fastapi import Request, HTTPException
from shared.config import settings


async def verify_api_key(request: Request) -> bool:
    """验证API Key"""
    # 从Header或Query参数获取API Key
    api_key = request.headers.get("X-API-Key") or request.query_params.get("api_key")

    if settings.deploy_mode == "local":
        # 方案一：简单API Key校验
        if api_key != settings.api_key:
            raise HTTPException(status_code=401, detail="Invalid API Key")
    return True
```

- [ ] **Step 2: 创建限流中间件 middleware/rate_limit.py**

```python
# services/gateway/middleware/rate_limit.py
"""简单限流中间件"""
import time
from collections import defaultdict
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, max_requests: int = 60, window_seconds: int = 60):
        super().__init__(app)
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests = defaultdict(list)

    async def dispatch(self, request: Request, call_next):
        client_ip = request.client.host if request.client else "unknown"
        now = time.time()

        # 清理过期记录
        self.requests[client_ip] = [
            t for t in self.requests[client_ip] if now - t < self.window_seconds
        ]

        # 检查限制
        if len(self.requests[client_ip]) >= self.max_requests:
            raise HTTPException(status_code=429, detail="Too many requests")

        self.requests[client_ip].append(now)
        response = await call_next(request)
        return response
```

- [ ] **Step 3: 创建CORS配置 middleware/cors.py**

```python
# services/gateway/middleware/cors.py
"""CORS 配置"""
from fastapi.middleware.cors import CORSMiddleware
from shared.config import settings


def add_cors(app):
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"] if settings.deploy_mode == "local" else [],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
```

- [ ] **Step 4: 创建路由转发 router.py**

```python
# services/gateway/router.py
"""API Gateway 路由转发"""
import httpx
from fastapi import APIRouter, Request, Depends, HTTPException
from shared.config import settings
from services.gateway.auth.api_key import verify_api_key

router = APIRouter()

# 服务地址映射
SERVICE_MAP = {
    "/api/script": settings.script_service_url,
    "/api/audio": settings.audio_service_url,
    "/api/project": settings.project_service_url,
    "/api/voice-profile": settings.project_service_url,
    "/api/task": settings.task_service_url,
}


@router.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy_request(path: str, request: Request, authed: bool = Depends(verify_api_key)):
    """将请求转发到对应的微服务"""
    # 确定目标服务
    full_path = f"/{path}"
    target_url = None
    route_prefix = ""

    for prefix, service_url in SERVICE_MAP.items():
        if full_path.startswith(prefix):
            target_url = service_url
            route_prefix = prefix
            break

    if not target_url:
        raise HTTPException(status_code=404, detail="Service not found")

    # 构建转发URL
    target_path = full_path[len(route_prefix):]
    url = f"{target_url}{route_prefix}{target_path}"

    # 转发请求
    async with httpx.AsyncClient(timeout=300.0) as client:
        try:
            # 准备请求参数
            headers = dict(request.headers)
            headers.pop("host", None)
            headers.pop("x-api-key", None)

            body = await request.body()

            response = await client.request(
                method=request.method,
                url=url,
                headers=headers,
                content=body if body else None,
                params=dict(request.query_params),
            )
            return response.json() if response.headers.get("content-type", "").startswith("application/json") else response.content
        except httpx.ConnectError:
            raise HTTPException(status_code=502, detail=f"Service unavailable: {target_url}")
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="Gateway timeout")
```

- [ ] **Step 5: 创建 Gateway 入口 main.py**

```python
# services/gateway/main.py
"""API Gateway - 统一入口"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from fastapi import FastAPI
from shared.config import settings
from services.gateway.router import router as proxy_router
from services.gateway.middleware.cors import add_cors
from services.gateway.middleware.rate_limit import RateLimitMiddleware
from shared.schemas import HealthResponse

app = FastAPI(title="Podcast API Gateway", version="0.1.0")

# 中间件
add_cors(app)
app.add_middleware(RateLimitMiddleware, max_requests=120, window_seconds=60)

# 路由
app.include_router(proxy_router, prefix="/api")


@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(status="ok", service="gateway")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=settings.gateway_port)
```

- [ ] **Step 6: Commit**

```bash
git add podcast-app/services/gateway/
git commit -m "feat: implement API Gateway with auth, rate limiting, CORS, and request proxying"
```

---

## Task 8: Docker Compose 部署配置

**Files:**
- Create: `podcast-app/docker-compose.yml`
- Create: `podcast-app/services/gateway/Dockerfile`
- Create: `podcast-app/services/script/Dockerfile`
- Create: `podcast-app/services/audio/Dockerfile`
- Create: `podcast-app/services/project/Dockerfile`
- Create: `podcast-app/services/task/Dockerfile`

- [ ] **Step 1: 创建通用 Dockerfile 模板（各服务共用）**

```dockerfile
# services/gateway/Dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml ./
COPY shared/ ./shared/
COPY services/gateway/ ./services/gateway/
RUN pip install --no-cache-dir -e .
CMD ["python", "-m", "uvicorn", "services.gateway.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

```dockerfile
# services/script/Dockerfile
FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml ./
COPY shared/ ./shared/
COPY services/script/ ./services/script/
RUN pip install --no-cache-dir -e .
CMD ["python", "-m", "uvicorn", "services.script.main:app", "--host", "0.0.0.0", "--port", "8001"]
```

```dockerfile
# services/audio/Dockerfile
FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml ./
COPY shared/ ./shared/
COPY services/audio/ ./services/audio/
RUN pip install --no-cache-dir -e .
CMD ["python", "-m", "uvicorn", "services.audio.main:app", "--host", "0.0.0.0", "--port", "8002"]
```

```dockerfile
# services/project/Dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml ./
COPY shared/ ./shared/
COPY services/project/ ./services/project/
RUN pip install --no-cache-dir -e .
CMD ["python", "-m", "uvicorn", "services.project.main:app", "--host", "0.0.0.0", "--port", "8003"]
```

```dockerfile
# services/task/Dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml ./
COPY shared/ ./shared/
COPY services/task/ ./services/task/
RUN pip install --no-cache-dir -e .
# Task Service有两个进程：FastAPI + Celery Worker
CMD ["python", "-m", "uvicorn", "services.task.main:app", "--host", "0.0.0.0", "--port", "8004"]
```

- [ ] **Step 2: 创建 docker-compose.yml**

```yaml
version: "3.8"

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: podcast
      POSTGRES_USER: podcast
      POSTGRES_PASSWORD: ${DB_PASSWORD:-podcast123}
    volumes:
      - pgdata:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U podcast"]
      interval: 5s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5

  gateway:
    build:
      context: .
      dockerfile: services/gateway/Dockerfile
    ports:
      - "8000:8000"
    environment:
      - DEPLOY_MODE=local
      - API_KEY=${API_KEY:-podcast-local-dev-key}
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
    build:
      context: .
      dockerfile: services/script/Dockerfile
    ports:
      - "8001:8001"
    environment:
      - DATABASE_URL=postgresql+asyncpg://podcast:${DB_PASSWORD:-podcast123}@postgres:5432/podcast
      - QWEN_API_KEY=${QWEN_API_KEY}
      - QWEN_BASE_URL=${QWEN_BASE_URL:-https://dashscope.aliyuncs.com/compatible-mode/v1}
      - QWEN_MODEL=${QWEN_MODEL:-qwen-plus}
    depends_on:
      postgres:
        condition: service_healthy

  audio:
    build:
      context: .
      dockerfile: services/audio/Dockerfile
    ports:
      - "8002:8002"
    environment:
      - DATABASE_URL=postgresql+asyncpg://podcast:${DB_PASSWORD:-podcast123}@postgres:5432/podcast
      - VOICEBOX_URL=${VOICEBOX_URL:-http://host.docker.internal:17493}
      - STORAGE_PATH=/app/storage
    volumes:
      - ./storage:/app/storage
    depends_on:
      postgres:
        condition: service_healthy

  project:
    build:
      context: .
      dockerfile: services/project/Dockerfile
    ports:
      - "8003:8003"
    environment:
      - DATABASE_URL=postgresql+asyncpg://podcast:${DB_PASSWORD:-podcast123}@postgres:5432/podcast
    depends_on:
      postgres:
        condition: service_healthy

  task:
    build:
      context: .
      dockerfile: services/task/Dockerfile
    ports:
      - "8004:8004"
    environment:
      - DATABASE_URL=postgresql+asyncpg://podcast:${DB_PASSWORD:-podcast123}@postgres:5432/podcast
      - REDIS_URL=redis://redis:6379/0
      - CELERY_BROKER_URL=redis://redis:6379/1
      - CELERY_RESULT_BACKEND=redis://redis:6379/2
      - SCRIPT_SERVICE_URL=http://script:8001
      - AUDIO_SERVICE_URL=http://audio:8002
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy

  celery-worker:
    build:
      context: .
      dockerfile: services/task/Dockerfile
    command: ["celery", "-A", "services.task.celery_app:celery_app", "worker", "-l", "info", "-Q", "podcast"]
    environment:
      - DATABASE_URL=postgresql+asyncpg://podcast:${DB_PASSWORD:-podcast123}@postgres:5432/podcast
      - REDIS_URL=redis://redis:6379/0
      - CELERY_BROKER_URL=redis://redis:6379/1
      - CELERY_RESULT_BACKEND=redis://redis:6379/2
      - SCRIPT_SERVICE_URL=http://script:8001
      - AUDIO_SERVICE_URL=http://audio:8002
      - VOICEBOX_URL=${VOICEBOX_URL:-http://host.docker.internal:17493}
      - QWEN_API_KEY=${QWEN_API_KEY}
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy

volumes:
  pgdata:
```

- [ ] **Step 3: Commit**

```bash
git add podcast-app/docker-compose.yml podcast-app/services/*/Dockerfile
git commit -m "feat: add Docker Compose deployment with all microservices"
```

---

## Task 9: 集成测试与启动脚本

**Files:**
- Create: `podcast-app/tests/conftest.py`
- Create: `podcast-app/tests/test_integration.py`
- Modify: `podcast-app/tests/test_project_service.py`

- [ ] **Step 1: 创建测试 conftest.py**

```python
# tests/conftest.py
"""测试公共 fixtures"""
import pytest
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from shared.database import Base, get_db


TEST_DB_URL = "sqlite+aiosqlite:///./test_podcast.db"


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
async def setup_test_db():
    """每个测试前创建表、测试后清理"""
    test_engine = create_async_engine(TEST_DB_URL, echo=False)
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await test_engine.dispose()
    import os
    if os.path.exists("./test_podcast.db"):
        os.unlink("./test_podcast.db")
```

- [ ] **Step 2: 创建集成测试 test_integration.py**

```python
# tests/test_integration.py
"""集成测试 - 全流程链路验证"""
import pytest
from services.script.llm.json_fixer import extract_and_fix_json, validate_script_content
from services.script.llm.prompt_templates import build_prompt
from services.script.parser.map_reduce import split_text
from services.audio.voicebox.client import VoiceboxClient
from shared.config import settings


def test_json_extraction_code_block():
    """测试从Markdown代码块提取JSON"""
    raw = '下面是生成的脚本：\n```json\n{"title": "科技播客", "characters": [{"id": 1, "name": "小明"}], "lines": [{"character_id": 1, "text": "大家好", "emotion": "neutral", "pace": "normal"}], "bgm_hints": []}\n```'
    result = extract_and_fix_json(raw)
    assert result["title"] == "科技播客"
    assert len(result["lines"]) == 1


def test_json_extraction_with_extra_text():
    """测试从带额外文字的输出提取JSON"""
    raw = '好的，这是您的播客脚本：\n{"title": "测试", "characters": [{"id": 1, "name": "A"}], "lines": [{"character_id": 1, "text": "你好", "emotion": "neutral", "pace": "normal"}], "bgm_hints": []}\n希望对您有帮助！'
    result = extract_and_fix_json(raw)
    assert result["title"] == "测试"


def test_validate_script_normalization():
    """测试脚本校验和标准化"""
    data = {
        "title": "测试",
        "characters": [{"id": 1, "name": "A", "personality": "test"}],
        "lines": [
            {"character_id": 1, "text": "Hello"},
            {"character_id": 1, "text": "World", "emotion": "invalid", "pace": "wrong"},
        ],
    }
    result = validate_script_content(data)
    # 无效值应该被标准化
    assert result.lines[0].emotion == "neutral"
    assert result.lines[1].emotion == "neutral"
    assert result.lines[1].pace == "normal"


def test_prompt_building_all_modes():
    """测试所有模式的Prompt构建"""
    for mode in ["solo", "duo", "group"]:
        system, user = build_prompt(mode, "tech", "AI发展趋势", target_minutes=5)
        assert len(system) > 100
        assert "AI发展趋势" in user


@pytest.mark.asyncio
async def test_voicebox_client_health():
    """测试Voicebox客户端健康检查（可能失败如果服务未启动）"""
    client = VoiceboxClient()
    # 在测试环境中Voicebox可能不可用，只验证方法可调用
    result = await client.health_check()
    assert isinstance(result, bool)
    await client.close()


def test_split_text_long():
    """测试长文本分块"""
    text = "这是测试句子。" * 1000
    chunks = split_text(text, chunk_size=500, overlap=50)
    assert len(chunks) > 1
```

- [ ] **Step 3: 运行所有测试**

```bash
cd /workspace/podcast-app
pip install aiosqlite pytest pytest-asyncio
pytest tests/ -v --tb=short
```

Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
git add podcast-app/tests/
git commit -m "feat: add integration tests for full pipeline validation"
```

---

## Task 10: 启动脚本与文档

**Files:**
- Create: `podcast-app/start.sh`
- Create: `podcast-app/.env.example` (update)

- [ ] **Step 1: 创建一键启动脚本 start.sh**

```bash
#!/bin/bash
# 播客Web应用后端一键启动脚本
# 使用方式：./start.sh [local|docker]

set -e

MODE=${1:-local}

echo "=== 播客Web应用后端启动 ==="
echo "模式: $MODE"

if [ "$MODE" = "docker" ]; then
    echo "使用 Docker Compose 启动..."
    docker compose up -d
    echo ""
    echo "服务已启动："
    echo "  API Gateway: http://localhost:8000"
    echo "  Script Service: http://localhost:8001"
    echo "  Audio Service: http://localhost:8002"
    echo "  Project Service: http://localhost:8003"
    echo "  Task Service: http://localhost:8004"
    echo ""
    echo "API文档: http://localhost:8000/docs"
else
    echo "本地开发模式启动..."

    # 检查依赖
    if ! command -v python3 &> /dev/null; then
        echo "错误：未找到 Python3"
        exit 1
    fi

    # 创建虚拟环境
    if [ ! -d "venv" ]; then
        python3 -m venv venv
    fi
    source venv/bin/activate

    # 安装依赖
    pip install -e ".[dev]" -q

    # 创建存储目录
    mkdir -p storage/{audio,documents,bgm}

    # 启动各服务（后台进程）
    echo "启动 Project Service (:8003)..."
    python -m uvicorn services.project.main:app --host 0.0.0.0 --port 8003 &
    PID_PROJECT=$!

    echo "启动 Script Service (:8001)..."
    python -m uvicorn services.script.main:app --host 0.0.0.0 --port 8001 &
    PID_SCRIPT=$!

    echo "启动 Audio Service (:8002)..."
    python -m uvicorn services.audio.main:app --host 0.0.0.0 --port 8002 &
    PID_AUDIO=$!

    echo "启动 Task Service (:8004)..."
    python -m uvicorn services.task.main:app --host 0.0.0.0 --port 8004 &
    PID_TASK=$!

    echo "启动 API Gateway (:8000)..."
    python -m uvicorn services.gateway.main:app --host 0.0.0.0 --port 8000 &
    PID_GATEWAY=$!

    echo ""
    echo "所有服务已启动 (PID: Project=$PID_PROJECT, Script=$PID_SCRIPT, Audio=$PID_AUDIO, Task=$PID_TASK, Gateway=$PID_GATEWAY)"
    echo "API Gateway: http://localhost:8000"
    echo "API文档: http://localhost:8000/docs"
    echo ""
    echo "按 Ctrl+C 停止所有服务"

    # 等待
    wait
fi
```

- [ ] **Step 2: 添加执行权限并验证**

```bash
chmod +x podcast-app/start.sh
bash -n podcast-app/start.sh && echo "Script syntax OK"
```

Expected: `Script syntax OK`

- [ ] **Step 3: 最终 Commit**

```bash
git add podcast-app/start.sh
git commit -m "feat: add one-click start script for local and Docker deployment"
```

---

## Spec Coverage Check

| 设计文档要求 | 对应Task |
|-------------|---------|
| Script Service (LLM脚本、文档解析、Map-Reduce) | Task 4 |
| Audio Service (Voicebox对接、后期处理、BGM) | Task 5 |
| Project Service (CRUD、音色管理) | Task 3 |
| Task Service (Celery、SSE、GPU监控) | Task 6 |
| API Gateway (认证、限流、路由转发) | Task 7 |
| PostgreSQL 数据模型 | Task 1 (models) + Task 2 (migration) |
| 方案一/方案二切换策略 | Task 1 (config.py DEPLOY_MODE) + storage抽象 |
| Docker Compose 部署 | Task 8 |
| 测试 | Task 3 + Task 4 + Task 9 |
| 启动脚本 | Task 10 |

All design requirements are covered by at least one task.
