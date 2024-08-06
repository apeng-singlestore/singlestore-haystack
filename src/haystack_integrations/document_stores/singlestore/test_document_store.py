from unittest.mock import Mock, patch

import pytest
from haystack import Document
from haystack.document_stores.errors import DuplicateDocumentError, MissingDocumentError
from haystack.testing.document_store import DocumentStoreBaseTests
from haystack_integrations.document_stores.singlestore.document_store import SingleStoreDocumentStore


class TestSingleStoreDocumentStore(DocumentStoreBaseTests):
    @pytest.fixture
    def mock_connection(self):
        with patch("singlestoredb.connect") as mock_connect:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_conn.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_conn
            yield mock_conn

    @pytest.fixture
    def docstore(self, mock_connection: Mock):
        store = SingleStoreDocumentStore(
            uri="mock://localhost:3306/test",
            table_name="test_documents"
        )
        return store

    def test_count_documents(self, docstore: SingleStoreDocumentStore, mock_connection: Mock):
        mock_cursor = mock_connection.cursor.return_value
        mock_cursor.fetchone.return_value = (2,)

        count = docstore.count_documents()
        assert count == 2

        mock_cursor.execute.assert_called_once_with(
            "SELECT COUNT(*) FROM test_documents"
        )
s
    def test_filter_documents(self, docstore: SingleStoreDocumentStore, mock_connection: Mock):
        mock_cursor = mock_connection.cursor.return_value
        mock_cursor.fetchall.return_value = [
            ("1", "Test document 1", '{"key": "value1"}'),
            ("3", "Test document 3", '{"key": "value1"}')
        ]

        filtered_docs = docstore.filter_documents({"key": "value1"})

        assert len(filtered_docs) == 2
        assert all(doc.metadata["key"] == "value1" for doc in filtered_docs)
        assert filtered_docs[0].id == "1"
        assert filtered_docs[1].id == "3"

        mock_cursor.execute.assert_called_once_with(
            "SELECT id, content, metadata FROM test_documents WHERE JSON_EXTRACT(metadata, '$.key') = %s",
            ('"value1"',)
        )

    def test_write_documents(self, docstore: SingleStoreDocumentStore, mock_connection: Mock):
        mock_cursor = mock_connection.cursor.return_value
        mock_cursor.rowcount = 1

        docs = [Document(id="1", content="Test document 1", metadata={"key": "value1"})]
        count = docstore.write_documents(docs)

        assert count == 1
        mock_cursor.execute.assert_called_once_with(
            "INSERT INTO test_documents (id, content, metadata) VALUES (%s, %s, %s)",
            ("1", "Test document 1", '{"key": "value1"}')
        )

    def test_write_documents_duplicate(self, docstore: SingleStoreDocumentStore, mock_connection: Mock):
        mock_cursor = mock_connection.cursor.return_value
        mock_cursor.execute.side_effect = Exception("Duplicate entry")

        docs = [Document(id="1", content="Test document 1", metadata={"key": "value1"})]

        with pytest.raises(DuplicateDocumentError):
            docstore.write_documents(docs)

    def test_delete_documents(self, docstore: SingleStoreDocumentStore, mock_connection: Mock):
        mock_cursor = mock_connection.cursor.return_value
        mock_cursor.rowcount = 1

        docstore.delete_documents(["1"])

        mock_cursor.execute.assert_called_once_with(
            "DELETE FROM test_documents WHERE id = %s",
            ("1",)
        )

    def test_delete_documents_missing(self, docstore: SingleStoreDocumentStore, mock_connection: Mock):
        mock_cursor = mock_connection.cursor.return_value
        mock_cursor.rowcount = 0

        with pytest.raises(MissingDocumentError):
            docstore.delete_documents(["1"])

