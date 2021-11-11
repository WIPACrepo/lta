"""Small workbench to discover bundles handled externally."""

import sys

def main():
    """Discover bundles handled externally."""
    bundles_list = sys.argv[1]
    bundle_uuids = []
    with open(bundles_list) as bl:
        bundle_lines = [line.rstrip() for line in bl]
        for line in bundle_lines:
            if line:
                bundle_uuid = line.split(" ")[1]
                bundle_uuids.append(bundle_uuid)

    files_list = sys.argv[2]
    file_uuids = []
    with open(files_list) as fl:
        file_lines = [line.rstrip() for line in fl]
        for line in file_lines:
            if line:
                file_uuid = line.split(".")[0]
                file_uuids.append(file_uuid)

    count = 0
    for buuid in bundle_uuids:
        if buuid not in file_uuids:
            print(f"ltacmd bundle update-status --uuid {buuid} --new-status external")
            count = count + 1
    print(f"count: {count}")


if __name__ == '__main__':
    main()
