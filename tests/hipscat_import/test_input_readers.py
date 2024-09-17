from hipscat_import.catalog.file_readers import IndexedCsvReader, IndexedParquetReader


def test_indexed_parquet_reader(storage_options, local_data_dir):
    # Chunksize covers all the inputs.
    total_chunks = 0
    total_len = 0
    for frame in IndexedParquetReader(chunksize=10_000, upath_kwargs=storage_options).read(
        local_data_dir / "indexed_files" / "parquet_list_single.txt"
    ):
        total_chunks += 1
        assert len(frame) == 131
        total_len += len(frame)

    assert total_chunks == 1
    assert total_len == 131

    # Requesting a very small chunksize. This will split up reads on the parquet.
    total_chunks = 0
    total_len = 0
    for frame in IndexedParquetReader(chunksize=5, upath_kwargs=storage_options).read(
        local_data_dir / "indexed_files" / "parquet_list_single.txt"
    ):
        total_chunks += 1
        assert len(frame) <= 5
        total_len += len(frame)

    assert total_chunks == 28
    assert total_len == 131


def test_indexed_csv_reader(storage_options, local_data_dir):
    # Chunksize covers all the inputs.
    total_chunks = 0
    total_len = 0
    for frame in IndexedCsvReader(chunksize=10_000, upath_kwargs=storage_options).read(
        local_data_dir / "indexed_files" / "csv_list_single.txt"
    ):
        total_chunks += 1
        assert len(frame) == 131
        total_len += len(frame)

    assert total_chunks == 1
    assert total_len == 131

    # Requesting a very small chunksize. This will split up reads on the parquet.
    total_chunks = 0
    total_len = 0
    for frame in IndexedCsvReader(chunksize=5, upath_kwargs=storage_options).read(
        local_data_dir / "indexed_files" / "csv_list_single.txt"
    ):
        total_chunks += 1
        assert len(frame) <= 5
        total_len += len(frame)

    assert total_chunks == 29
    assert total_len == 131
