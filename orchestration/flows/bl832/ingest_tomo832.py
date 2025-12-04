def test_ingest_raw_tomo() -> bool:
    from orchestration.flows.scicat.ingest import ingest_dataset
    INGESTER_SPEC = "als_832_dx_4"
    file_path = "examples/tomo_scan_no_email.h5"
    print(f"Ingesting {file_path} with {INGESTER_SPEC}")
    try:
        ingest_dataset(file_path, INGESTER_SPEC)
        return True
    except Exception as e:
        print(f"SciCat ingest failed with {e}")
        return False


if __name__ == "__main__":

    test_ingest_raw_tomo()
