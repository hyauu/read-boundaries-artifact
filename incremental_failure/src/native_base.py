"""Native local Cognee runner. Fixtures replace AI calls, not storage/retrieval.

The observational wrappers forward the original call and original result unchanged.
Never use the recorded mappings to select or hydrate native search results.
"""
from __future__ import annotations

import argparse
import asyncio
import contextvars
import dataclasses
import datetime
import functools
import hashlib
import importlib
import inspect
import json
import os
from pathlib import Path
import sys
import time
import traceback
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
# A single module instance keeps ContextVar identity shared by the runner and hooks.
if __name__ == "__main__":
    sys.modules["cognee_adapter.runtime"] = sys.modules[__name__]
OP = contextvars.ContextVar("replication_operation", default="setup")
V1 = "Alice lives in Seattle. Alice works at Company X."
V2 = "Alice lives in San Francisco. Alice works at Company Y."
V3 = "Alice lives in Boston. Alice works at Company Z."


def plain(value):
    if hasattr(value, "model_dump"):
        return plain(value.model_dump())
    if dataclasses.is_dataclass(value):
        return plain(dataclasses.asdict(value))
    if isinstance(value, dict):
        return {str(k): plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [plain(x) for x in value]
    if isinstance(value, (UUID, Path, datetime.datetime)):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return {"python_type": type(value).__name__, "repr": repr(value)}


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(plain(value), ensure_ascii=False, indent=2), encoding="utf-8")


class Observer:
    def __init__(self, out):
        self.out = out
        self.seq = 0
        self.events = []
        self.gate = None

    def emit(self, event, **data):
        self.seq += 1
        value = dict(seq=self.seq, utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                     monotonic_ns=time.perf_counter_ns(), operation=OP.get(), event=event, **plain(data))
        self.events.append(value)
        with (self.out / "events.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(value, ensure_ascii=False) + "\n")
        return value

    def install(self):
        for backend, modname, clsname in [
            ("graph", "cognee.infrastructure.databases.graph.get_graph_engine", "_GraphEngineHandle"),
            ("vector", "cognee.infrastructure.databases.vector.get_vector_engine", "_VectorEngineHandle"),
        ]:
            cls = getattr(importlib.import_module(modname), clsname)
            original = cls.__getattr__

            def traced_getattr(handle, name, _original=original, _backend=backend):
                target = _original(handle, name)
                wanted = {"add_nodes", "add_edges", "index_data_points", "create_data_points",
                          "search", "retrieve", "get_neighborhood", "get_nodes", "get_node",
                          "get_connections", "is_empty", "get_graph_data", "delete_nodes",
                          "delete_data_points", "remove_source_ref_keys", "get_provenance"}
                if name not in wanted or not callable(target):
                    return target

                @functools.wraps(target)
                async def forward(*args, **kwargs):
                    request = self.emit("backend.request", backend=_backend, method=name,
                                        args=args, kwargs=kwargs)
                    if self.gate:
                        await self.gate.before(_backend, name, args, kwargs)
                    try:
                        result = target(*args, **kwargs)
                        if inspect.isawaitable(result):
                            result = await result
                    except BaseException as exc:
                        self.emit("backend.error", request_seq=request["seq"], error=repr(exc))
                        raise
                    response = self.emit("backend.response", request_seq=request["seq"],
                                         backend=_backend, method=name, result=result)
                    if self.gate:
                        await self.gate.after(_backend, name, args, kwargs, result)
                    return result
                return forward
            cls.__getattr__ = traced_getattr

        stage_module = importlib.import_module("cognee.api.v1.update.incremental")
        original_stage = stage_module._stage_new_content

        @functools.wraps(original_stage)
        async def staged(*args, **kwargs):
            result = await original_stage(*args, **kwargs)
            self.emit("native.staged_content", input=args[0], staged=result)
            return result
        stage_module._stage_new_content = staged
        update_module = importlib.import_module("cognee.api.v1.update.update")
        original_add = update_module.add

        @functools.wraps(original_add)
        async def reingest(*args, **kwargs):
            result = await original_add(*args, **kwargs)
            await record_source(self, kwargs["dataset_id"])
            self.emit("native.reingest_return", result=result)
            return result
        update_module.add = reingest
        retriever_module = importlib.import_module("cognee.modules.retrieval.hybrid_retriever")
        original_retrieve = retriever_module.HybridRetriever.get_retrieved_objects

        @functools.wraps(original_retrieve)
        async def hybrid(retriever, *args, **kwargs):
            self.emit("retriever.begin", class_name="HybridRetriever")
            result = await original_retrieve(retriever, *args, **kwargs)
            self.emit("retriever.return", class_name="HybridRetriever", result=result)
            return result
        retriever_module.HybridRetriever.get_retrieved_objects = hybrid
        original_passages = retriever_module.retrieve_hybrid_chunks

        @functools.wraps(original_passages)
        async def passages(*args, **kwargs):
            result = await original_passages(*args, **kwargs)
            self.emit("native.passage_lane_return", result=result)
            if self.gate:
                await self.gate.lane_return(OP.get(), result)
            return result
        retriever_module.retrieve_hybrid_chunks = passages


def configure(out):
    # Ladybug's Windows native file API cannot open a non-ASCII path.
    # NTFS's existing 8.3 alias names the very same directory, without relocating it.
    import ctypes
    alias = ctypes.create_unicode_buffer(4096)
    length = ctypes.windll.kernel32.GetShortPathNameW(str(out), alias, len(alias))
    if not length or any(ord(c) > 127 for c in alias.value):
        raise RuntimeError("No ASCII alias for the isolated runtime directory")
    native_out = Path(alias.value)
    config = {
        "DB_PROVIDER": "sqlite", "VECTOR_DB_PROVIDER": "lancedb",
        "GRAPH_DATABASE_PROVIDER": "kuzu", "CACHE_BACKEND": "sqlite",
        "ENABLE_BACKEND_ACCESS_CONTROL": "false", "MOCK_EMBEDDING": "true",
        "EMBEDDING_PROVIDER": "openai", "EMBEDDING_MODEL": "openai/text-embedding-3-small",
        "EMBEDDING_DIMENSIONS": "8", "LLM_PROVIDER": "openai", "LLM_MODEL": "gpt-4o-mini",
        "LLM_API_KEY": "offline-fixture-unused", "EMBEDDING_API_KEY": "offline-fixture-unused",
        "TELEMETRY_DISABLED": "1", "AUTO_FEEDBACK": "false",
        "DATA_ROOT_DIRECTORY": str(native_out / "runtime" / "data"),
        "SYSTEM_ROOT_DIRECTORY": str(native_out / "runtime" / "system"),
        "GRAPH_FILENAME": str(native_out / "runtime" / "system" / "databases" / "cognee_graph_kuzu"),
        "CACHE_ROOT_DIRECTORY": str(native_out / "runtime" / "cache"),
        "COGNEE_LOGS_DIR": str(out / "runtime" / "logs"),
        "COGNEE_REPOS_DIR": str(out / "runtime" / "repos"),
        "COGNEE_TRACING_ENABLED": "false",
        "TIKTOKEN_CACHE_DIR": str(ROOT / ".cache" / "tiktoken"),
        "LITELLM_LOCAL_MODEL_COST_MAP": "True", "PYTHONDONTWRITEBYTECODE": "1",
    }
    # Do not inherit credentials or remote backend endpoints.
    for key in list(os.environ):
        if key.endswith("API_KEY") or key in {"DATABASE_URL", "VECTOR_DB_URL", "GRAPH_DATABASE_URL"}:
            os.environ.pop(key, None)
    os.environ.update(config)
    import cognee
    os.environ.update(config)  # Import uses dotenv(override=True), as upstream tests note.
    for module, factory in [
        ("cognee.base_config", "get_base_config"),
        ("cognee.infrastructure.databases.relational.config", "get_relational_config"),
        ("cognee.infrastructure.databases.relational.get_relational_engine", "get_relational_engine"),
        ("cognee.infrastructure.databases.graph.config", "get_graph_config"),
        ("cognee.infrastructure.databases.vector.config", "get_vectordb_config"),
        ("cognee.infrastructure.databases.cache.config", "get_cache_config"),
        ("cognee.infrastructure.databases.cache.get_cache_engine", "create_cache_engine"),
        ("cognee.infrastructure.databases.vector.embeddings.config", "get_embedding_config"),
        ("cognee.infrastructure.databases.vector.embeddings.get_embedding_engine", "create_embedding_engine"),
        ("cognee.infrastructure.llm.config", "get_llm_config"),
    ]:
        target = getattr(importlib.import_module(module), factory)
        if hasattr(target, "cache_clear"):
            target.cache_clear()
    save(out / "configuration.json", config)
    return cognee


def install_ai_fixture(observer):
    from cognee.infrastructure.llm.LLMGateway import LLMGateway
    from cognee.shared.data_models import KnowledgeGraph, Node, Edge, SummarizedContent
    from cognee.infrastructure.databases.vector.embeddings.LiteLLMEmbeddingEngine import LiteLLMEmbeddingEngine

    @staticmethod
    async def structured(text_input, system_prompt, response_model, **kwargs):
        text = str(text_input)
        observer.emit("fixture.ai_input", response_model=response_model.__name__, text_input=text_input)
        if isinstance(response_model, type) and issubclass(response_model, KnowledgeGraph):
            versions = [(V1, "Seattle", "Company X"), (V2, "San Francisco", "Company Y"),
                        (V3, "Boston", "Company Z")]
            matches = [(city, company) for source, city, company in versions if source in text]
            if len(matches) != 1:
                raise RuntimeError("Fixture does not recognize exactly one source; no guessed lineage")
            city, company = matches[0]
            nodes = [Node(id="Alice", name="Alice", type="Person", description="Alice"),
                     Node(id=city, name=city, type="City", description=city),
                     Node(id=company, name=company, type="Company", description=company)]
            edges = [Edge(source_node_id="Alice", target_node_id=city, relationship_name="lives_in",
                          description=f"Alice lives in {city}."),
                     Edge(source_node_id="Alice", target_node_id=company, relationship_name="works_at",
                          description=f"Alice works at {company}.")]
            return KnowledgeGraph(nodes=nodes, edges=edges)
        if isinstance(response_model, type) and issubclass(response_model, SummarizedContent):
            matches = [source for source in (V1, V2, V3) if source in text]
            if len(matches) != 1:
                raise RuntimeError("Summary fixture cannot assign source")
            return SummarizedContent(summary=matches[0], description="")
        if response_model is str and text == "test":
            return "offline connectivity fixture"
        raise RuntimeError(f"Unexpected AI call: {response_model}")

    async def embed(self, texts):
        observer.emit("fixture.embedding", texts=texts)
        # Nonzero vectors ensure the tiny corpus is retrieved; ranking is outside scope.
        return [[1.0] + [0.0] * 7 for _ in texts]

    LLMGateway.acreate_structured_output = structured
    LiteLLMEmbeddingEngine.embed_text = embed


async def native(observer, name, coroutine):
    token = OP.set(name)
    started = observer.emit("api.begin", boundary=name)
    try:
        result = await coroutine
        observer.emit("api.return", boundary=name, result=result,
                      elapsed_ms=(time.perf_counter_ns() - started["monotonic_ns"]) / 1e6)
        return result
    except BaseException as exc:
        observer.emit("api.error", boundary=name, error=repr(exc), exception_type=type(exc).__name__)
        raise
    finally:
        OP.reset(token)


async def recall(cognee, observer, dataset_id, name):
    from cognee.api.v1.search import SearchType
    from cognee.modules.retrieval.exceptions.exceptions import NoDataError
    try:
        result = await native(observer, name, cognee.search(
            query_text="Where does Alice live and where does Alice work?",
            query_type=SearchType.HYBRID_COMPLETION, dataset_ids=[dataset_id],
            only_context=True, verbose=True, include_references=True, top_k=20,
            retriever_specific_config={"chunks_top_k": 10, "entities_top_k": 10,
                                       "max_edges_per_entity": 20, "facts_top_k": 30},
        ))
    except NoDataError as exc:
        # Preserve the native error, rather than manufacture a successful empty response.
        result = {"native_exception": "NoDataError", "message": str(exc)}
    save(observer.out / f"{name}.native.json", result)
    return result


async def probe(observer, dataset_id, data_id, name):
    from cognee.modules.data.methods.get_dataset_data import get_dataset_data
    from cognee.infrastructure.databases.graph import get_graph_engine
    from cognee.infrastructure.databases.vector import get_vector_engine_async
    token = OP.set("probe:" + name)
    try:
        graph = await get_graph_engine()
        vector = await get_vector_engine_async()
        rows = await get_dataset_data(dataset_id)
        nodes, edges = await graph.get_graph_data()
        collections = await (await vector.get_connection()).table_names()
        vectors = {}
        for collection in collections:
            table = await vector.get_collection(collection)
            vectors[collection] = await table.query().to_list()
        result = {"canonical": [{c.name: getattr(row, c.name) for c in row.__table__.columns} for row in rows],
                  "graph_nodes": nodes, "graph_edges": edges, "vectors": vectors,
                  "dataset_id": dataset_id, "data_id": data_id}
        save(observer.out / f"{name}.probe.json", result)
        return result
    finally:
        OP.reset(token)


async def record_source(observer, dataset_id):
    from cognee.modules.data.methods.get_dataset_data import get_dataset_data
    from cognee.infrastructure.files.utils.open_data_file import open_data_file
    rows = await get_dataset_data(dataset_id)
    assert len(rows) == 1
    row = rows[0]
    async with open_data_file(row.raw_data_location) as stream:
        data = stream.read()
    text = data.decode("utf-8") if isinstance(data, bytes) else data
    observer.emit("source.publish", data_id=row.id, dataset_id=dataset_id,
                  raw_data_location=row.raw_data_location, content_hash=row.content_hash,
                  text=text, sha256=hashlib.sha256(text.encode()).hexdigest())

