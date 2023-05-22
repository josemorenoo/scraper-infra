import boto3
from datetime import datetime, timedelta
import json


def download_combine_monthly_json_files(
    end_date,
    bucket_name="coincommit",
):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)

    start_date = end_date - timedelta(days=29)

    monthly_data = {}

    for i in range((end_date - start_date).days + 1):
        datestr = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        s3_object = bucket.Object(f"reports/{datestr}/{datestr}.json")
        try:
            file_content = s3_object.get()["Body"].read().decode("utf-8")
            daily_data = json.loads(file_content)
        except:
            print(f"Error downloading or parsing data for {datestr}")
            continue
        for token, metadata in daily_data.items():
            if token in monthly_data:
                existing_token_data = monthly_data[token]
                existing_token_data["commit_count"] += metadata["commit_count"]
                existing_token_data["lines_of_code"] += metadata["lines_of_code"]
                existing_token_data["commit_messages"].extend(
                    metadata["commit_messages"]
                )
                existing_token_data["distinct_authors"].extend(
                    metadata["distinct_authors"]
                )
                if "active_repos" in metadata:
                    if "active_repos" in existing_token_data:
                        existing_token_data["active_repos"].extend(
                            metadata["active_repos"]
                        )
                    else:
                        existing_token_data["active_repos"] = metadata["active_repos"]
                else:
                    existing_token_data["active_repos"] = []

                existing_token_data["commit_urls"].extend(metadata["commit_urls"])
                existing_token_data["changed_methods"].extend(
                    metadata["changed_methods"]
                )
                # these don't need to be appended, just assigned
                existing_token_data["description"] = metadata["description"]
                existing_token_data["project_created_cmc"] = metadata[
                    "project_created_cmc"
                ]

                merged_dict = existing_token_data["file_extensions"].copy()
                for filetype, filecount in metadata["file_extensions"].items():
                    if filetype in merged_dict:
                        merged_dict[filetype] += filecount
                    else:
                        merged_dict[filetype] = filecount
                existing_token_data["file_extensions"] = merged_dict

                merged_dict = existing_token_data["loc_changes_by_filetype"].copy()
                for action in ["insertions", "deletions", "net"]:
                    actiondict = merged_dict[action]
                    actiondict2 = metadata["loc_changes_by_filetype"][action]
                    for filetype, filecount in actiondict2.items():
                        if filetype in actiondict:
                            actiondict[filetype] += filecount
                        else:
                            actiondict[filetype] = filecount
                    merged_dict[action] = actiondict
                monthly_data[token]["loc_changes_by_filetype"] = merged_dict
            else:
                monthly_data[token] = metadata

        # de-dupe lists
        for token, metadata in monthly_data.items():
            monthly_data[token]["commit_messages"] = list(
                set(metadata["commit_messages"])
            )
            monthly_data[token]["distinct_authors"] = list(
                set(metadata["distinct_authors"])
            )
            monthly_data[token]["commit_urls"] = list(set(metadata["commit_urls"]))
            monthly_data[token]["changed_methods"] = list(
                set(metadata["changed_methods"])
            )

    monthly_file_content = json.dumps(monthly_data, indent=2)
    monthly_aggregation_path = f"/tmp/monthly.json"
    with open(monthly_aggregation_path, "w") as f:
        f.write(monthly_file_content)

    # s3_location = f'reports/{end_date.strftime("%Y-%m-%d")}/monthly.json'
    # s3.Object(bucket_name, s3_location).put(Body=monthly_file_content)

    return monthly_aggregation_path


if __name__ == "__main__":
    end_date = datetime.today()
    monthly_file_name = download_combine_monthly_json_files(end_date)

    print(f"monthly data has been saved to {monthly_file_name}")
