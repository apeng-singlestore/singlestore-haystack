# SPDX-FileCopyrightText: 2023-present John Doe <jd@example.com>
#
# SPDX-License-Identifier: Apache-2.0
import json
import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from haystack import Document
from haystack.document_stores.errors import DuplicateDocumentError, MissingDocumentError
from haystack.document_stores.types import DuplicatePolicy
from sqlalchemy.pool import QueuePool

logger = logging.getLogger(__name__)

class SingleStoreDocumentStore:
    def __init__(
        self,
        uri: Optional[str] = None,
        table_name: str = "documents",
        content_field: str = "content",
        metadata_field: str = "metadata",
        id_field: str = "id",
        pool_size: int = 5,
        max_overflow: int = 10,
        timeout: float = 30,
        **kwargs: Any
    ):
        self.table_name = table_name
        self.content_field = content_field
        self.metadata_field = metadata_field
        self.id_field = id_field
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.timeout = timeout

        self.connection_kwargs = kwargs
        if uri:
            self._parse_uri(uri)

        self.connection_pool = QueuePool(
            self._get_connection,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            timeout=self.timeout,
        )

        self._create_table()

    def _parse_uri(self, uri: str):
        parsed = urlparse(uri)
        self.connection_kwargs["host"] = parsed.hostname
        self.connection_kwargs["port"] = parsed.port or 3306
        self.connection_kwargs["user"] = parsed.username
        self.connection_kwargs["password"] = parsed.password
        if parsed.path and parsed.path != "/":
            self.connection_kwargs["database"] = parsed.path.strip("/")

    def _get_connection(self):
        try:
            import singlestoredb as s2
        except ImportError:
            raise ImportError(
                "Could not import singlestoredb python package. "
                "Please install it with `pip install singlestoredb`."
            )
        return s2.connect(**self.connection_kwargs)

    def _create_table(self):
        conn = self.connection_pool.connect()
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    f"""CREATE TABLE IF NOT EXISTS {self.table_name}
                    ({self.id_field} VARCHAR(100) PRIMARY KEY,
                    {self.content_field} TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
                    {self.metadata_field} JSON);"""
                )
            finally:
                cur.close()
        finally:
            conn.close()

    def count_documents(self) -> int:
        conn = self.connection_pool.connect()
        try:
            cur = conn.cursor()
            try:
                cur.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                return cur.fetchone()[0]
            finally:
                cur.close()
        finally:
            conn.close()

    def filter_documents(self, filters: Optional[Dict[str, Any]] = None) -> List[Document]:
        conn = self.connection_pool.connect()
        try:
            cur = conn.cursor()
            try:
                query = f"SELECT {self.id_field}, {self.content_field}, {self.metadata_field} FROM {self.table_name}"
                params = []
                if filters:
                    where_clauses = []
                    for key, value in filters.items():
                        where_clauses.append(f"JSON_EXTRACT({self.metadata_field}, '$.{key}') = %s")
                        params.append(json.dumps(value))
                    if where_clauses:
                        query += " WHERE " + " AND ".join(where_clauses)

                cur.execute(query, tuple(params))
                results = cur.fetchall()

                documents = []
                for row in results:
                    doc_id, content, metadata = row
                    metadata = json.loads(metadata)
                    documents.append(Document(content=content, id=doc_id, metadata=metadata))

                return documents
            finally:
                cur.close()
        finally:
            conn.close()

    def write_documents(self, documents: List[Document], policy: DuplicatePolicy = DuplicatePolicy.FAIL) -> int:
        conn = self.connection_pool.connect()
        try:
            cur = conn.cursor()
            try:
                count = 0
                for doc in documents:
                    try:
                        cur.execute(
                            f"INSERT INTO {self.table_name} ({self.id_field}, {self.content_field}, {self.metadata_field}) "
                            f"VALUES (%s, %s, %s)",
                            (doc.id, doc.content, json.dumps(doc.metadata))
                        )
                        count += 1
                    except Exception as e:
                        if "Duplicate entry" in str(e):
                            if policy == DuplicatePolicy.FAIL:
                                raise DuplicateDocumentError(f"Document with id {doc.id} already exists.")
                            elif policy == DuplicatePolicy.SKIP:
                                continue
                            elif policy == DuplicatePolicy.OVERWRITE:
                                cur.execute(
                                    f"REPLACE INTO {self.table_name} ({self.id_field}, {self.content_field}, {self.metadata_field}) "
                                    f"VALUES (%s, %s, %s)",
                                    (doc.id, doc.content, json.dumps(doc.metadata))
                                )
                                count += 1
                        else:
                            raise e
                return count
            finally:
                cur.close()
        finally:
            conn.close()

    def delete_documents(self, document_ids: List[str]) -> None:
        conn = self.connection_pool.connect()
        try:
            cur = conn.cursor()
            try:
                for doc_id in document_ids:
                    cur.execute(f"DELETE FROM {self.table_name} WHERE {self.id_field} = %s", (doc_id,))
                    if cur.rowcount == 0:
                        raise MissingDocumentError(f"Document with id '{doc_id}' not found.")
            finally:
                cur.close()
        finally:
            conn.close()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "SingleStoreDocumentStore",
            "table_name": self.table_name,
            "content_field": self.content_field,
            "metadata_field": self.metadata_field,
            "id_field": self.id_field,
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "timeout": self.timeout,
            **self.connection_kwargs
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SingleStoreDocumentStore":
        store_type = data.pop("type", None)
        if store_type != "SingleStoreDocumentStore":
            raise ValueError(f"Incompatible document store type: {store_type}")

        return cls(**data)
