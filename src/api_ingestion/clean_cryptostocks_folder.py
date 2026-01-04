from pathlib import Path


def clean_cryptostock_jsons(folder: str = "/opt/raw_datasets/cryptostocks") -> int:
    """
    Delete all JSON files inside the given folder.
    Returns the number of deleted files.
    """
    folder_path = Path(folder)
    if not folder_path.exists():
        print(f"Folder does not exist: {folder_path}")
        return 0

    deleted_count = 0
    for json_file in folder_path.glob("*.json"):
        try:
            json_file.unlink()
            print(f"Deleted: {json_file}")
            deleted_count += 1
        except Exception as e:
            print(f"Failed to delete {json_file}: {e}")

    print(f"\nTotal JSON files deleted: {deleted_count}")
    return deleted_count


def main():
    clean_cryptostock_jsons()


if __name__ == "__main__":
    main()
