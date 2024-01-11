import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--client-id", required=True)
parser.add_argument("--client-secret", required=True)
parser.add_argument("--tenant-id", required=True)
parser.add_argument("--databricks-url", required=True)
parser.add_argument("--environment", required=True)
args = parser.parse_args()

print(f"The value of client-id is {args.client-id}")
print(f"The value of client-secret is {args.client-secret}")
print(f"The value of tenant-id is {args.tenant-id}")
print(f"The value of databricks-url is {args.databricks-url}")
print(f"The value of environment is {args.environment}")
